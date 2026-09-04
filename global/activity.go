package global

import (
	"fmt"
	"sort"
	"strings"

	"github.com/tjbdwanghaibo/roost-core/errcode"
)

// This file is the cross-server activity coordination half of the package:
// aggregating a phase advance (close, settle, ...) across the game servers
// that take part in one activity, the per-participant progress those servers
// report, and the delivery of the aggregated result back to them.
//
// The business boundary document of the implementation this replaces stated
// the rules below as conventions. They were not kept, and three of the gaps
// were confirmed by reading that implementation:
//
//   - Every store exposed both an unconditional SetXxx and a read-modify-write
//     UpdateXxx. The Redis implementation's Update used compare-and-set; the
//     DAO-backed one's Update read and then wrote unconditionally. Both
//     satisfied the same interface, so which one was configured decided
//     whether the documented CAS invariant held. Here every mutation goes
//     through versionstore, whose contract has no unconditional write, so a
//     non-CAS implementation of activity state cannot be written at all.
//   - The notify path had no idempotency, so a redelivered notification
//     double-counted a participant's progress. Here a progress apply must
//     first claim an insert-only reservation in the request ledger, and the
//     apply that follows records the request id in the same compare-and-set
//     that moves the score, so no interleaving of a redelivery can count
//     twice.
//   - Notify audit was documented but written on only some refusal paths.
//     Here a refusal is not something a branch can return: the only
//     expression in this package that produces a notify refusal is
//     refuseNotify, which writes the audit first and fails the call if it
//     cannot. A refusal without an audit is not reachable.
//
// One further rule from that document shapes the API more than anything else:
// "活动开关和阶段推进由各 game 根据本服业务时间线通知，global 不按自身 CloseAt
// 主动驱动活动时间线" — global never advances an activity from its own clock.
// So an activity opens in ActivityPending and stays there indefinitely; only a
// game's notification starts the grace window, and the sweep that back-stops
// the aggregation can only finish a window a game already started.

// Activity error codes. The block in types.go ends at 570111 and this one
// continues from there, written out rather than derived with iota so a code is
// a fact about the wire and not a fact about the order of const specs.
const (
	CodeActivityInvalid    int32 = 570112
	CodeActivityMissing    int32 = 570113
	CodeActivityExists     int32 = 570114
	CodeActivityStatus     int32 = 570115
	CodeNotifyUnexpected   int32 = 570116
	CodeNotifyLate         int32 = 570117
	CodeActivityBacklog    int32 = 570118
	CodeParticipantInvalid int32 = 570119
	CodeRequestInvalid     int32 = 570120
	CodeDispatchMissing    int32 = 570121
	CodeDispatchToken      int32 = 570122
	CodeDispatchExhausted  int32 = 570123
	CodeDispatchNotDue     int32 = 570124
)

var (
	ErrActivityInvalid = errcode.Define(CodeActivityInvalid, "global: activity request is invalid", "")
	ErrActivityMissing = errcode.Define(CodeActivityMissing, "global: activity not found", "")
	ErrActivityExists  = errcode.Define(CodeActivityExists, "global: activity already open", "")
	// ErrActivityStatus reports that the activity is not in a status the
	// operation applies to — a notify or a progress apply against an
	// aggregation that already completed. It is distinct from ErrNotifyLate so
	// an operator can tell "the window closed while you were away" from "the
	// result was already dispatched".
	ErrActivityStatus = errcode.Define(CodeActivityStatus, "global: activity is not in a status that allows this", "")
	// ErrNotifyUnexpected reports a notification from a game server that is
	// not in the activity's expected set. The expected set is fixed when the
	// activity opens and is never taken from a notify, because an aggregation
	// whose membership can be widened by the notification itself can always be
	// completed by whoever notifies last.
	ErrNotifyUnexpected = errcode.Define(CodeNotifyUnexpected, "global: notifying game is not expected by this activity", "")
	// ErrNotifyLate reports a notification that arrived after the grace
	// deadline. Accepting it would make the aggregated result depend on
	// whether the sweep happened to have run yet, so the same interleaving
	// would produce different results on different days.
	ErrNotifyLate = errcode.Define(CodeNotifyLate, "global: notification arrived after the grace window closed", "")
	// ErrActivityBacklog reports that the group's pending window is full.
	// Refusing to open is deliberate: the window is what the sweep scans, so
	// an activity that is not in it would never be back-stopped, and silently
	// opening one outside the window would trade a loud refusal for a
	// permanently stranded aggregation.
	ErrActivityBacklog = errcode.Define(CodeActivityBacklog, "global: pending activity window is full", "")

	ErrParticipantInvalid = errcode.Define(CodeParticipantInvalid, "global: participant is invalid", "")
	// ErrRequestInvalid reports a malformed idempotency key, or one that was
	// reserved for a different delta. Reusing a request id for different
	// arguments cannot be resolved by applying either of them, and applying
	// both is the double count this ledger exists to prevent.
	ErrRequestInvalid = errcode.Define(CodeRequestInvalid, "global: progress request is invalid", "")

	ErrDispatchMissing = errcode.Define(CodeDispatchMissing, "global: dispatch not found", "")
	// ErrDispatchToken reports an ACK that did not present the token the
	// dispatch was delivered with. It never echoes the expected token: the
	// token is the whole authorization, so a wrong guess must not teach the
	// caller the right one.
	ErrDispatchToken = errcode.Define(CodeDispatchToken, "global: dispatch ack token does not match", "")
	// ErrDispatchExhausted reports a dispatch whose attempt budget is spent.
	// It is terminal on purpose. Quietly accepting an ACK after the budget was
	// spent would erase the evidence that delivery to that game never worked,
	// which is precisely the class of silent path the design constraints call
	// out as able to survive for years.
	ErrDispatchExhausted = errcode.Define(CodeDispatchExhausted, "global: dispatch attempts are exhausted", "")
	ErrDispatchNotDue    = errcode.Define(CodeDispatchNotDue, "global: dispatch is not due for another attempt", "")
)

// ActivityCode maps an activity error to its code.
//
// It delegates to global.Code, which covers this package's route, lease and
// activity sentinels alike — they share one code segment and one error
// vocabulary, so two mapping functions would be two places for a new sentinel
// to be forgotten in. It stays for the callers that named it.
func ActivityCode(err error) int32 { return Code(err) }

// Bounds. Every one of these is a bound a zero value cannot bypass: a limit of
// zero is an error rather than "unlimited", and a size a caller controls is
// checked before it is stored, not after it has grown.
const (
	// MaxExpectedGames bounds an activity's expected set. It also bounds the
	// dispatch fan-out and the per-activity dispatch scan, so those need no
	// separate limit.
	MaxExpectedGames = 64

	// MaxPendingActivities bounds one group's pending window. The window is
	// the only enumerable index of activities that are not finished, so it is
	// also the bound on how much a single sweep can read.
	MaxPendingActivities = 256

	// MaxNotifyAudits bounds the audit log kept per activity. An unbounded
	// audit log is a denial of service written by a misconfigured game server
	// that retries a refused notify forever, so the log keeps the oldest
	// entries — the ones that say when the misconfiguration started — and
	// counts what it dropped in NotifyAuditLog.Overflowed. Dropping without
	// counting is how a silent path stays silent.
	MaxNotifyAudits = 64

	// MaxProgressWindow bounds the ring of applied request ids kept inside a
	// participant record. It only has to cover requests that are in flight
	// concurrently for one participant, not the whole retry window: a replay
	// that arrives later is already answered by the ledger, which is bounded
	// by time rather than by count. See Participant.AppliedRequestIDs.
	MaxProgressWindow = 32

	// MaxActivityIDLen and MaxParticipantIDLen bound the identifiers that
	// become store keys, so a caller cannot mint an unbounded key.
	MaxActivityIDLen    = 128
	MaxParticipantIDLen = 128
	// MaxRequestIDLen bounds the client-chosen idempotency key.
	MaxRequestIDLen = 128
)

// ActivityPhase is which cross-server advance an aggregation is for. "close"
// and "settle" are the ones the boundary document names; the type is a string
// because which phases exist is a game concept and baking an enum in here
// would make every new phase a change to this package.
type ActivityPhase string

const (
	PhaseClose  ActivityPhase = "close"
	PhaseSettle ActivityPhase = "settle"
)

// ActivityKey is the aggregation key: one activity instance, one phase, within
// one coordination group. The group is part of the key because the pending
// window a sweep scans is per group, and a sweep must not be able to reach
// activities outside the group it was asked about.
type ActivityKey struct {
	GroupID    string        `json:"group_id"`
	ActivityID string        `json:"activity_id"`
	Phase      ActivityPhase `json:"phase"`
}

func (k ActivityKey) Validate() error {
	if strings.TrimSpace(k.GroupID) == "" {
		return fmt.Errorf("%w: group id is empty", ErrActivityInvalid)
	}
	if strings.TrimSpace(k.ActivityID) == "" {
		return fmt.Errorf("%w: activity id is empty", ErrActivityInvalid)
	}
	if len(k.ActivityID) > MaxActivityIDLen {
		return fmt.Errorf("%w: activity id is %d bytes, limit %d", ErrActivityInvalid, len(k.ActivityID), MaxActivityIDLen)
	}
	if strings.TrimSpace(string(k.Phase)) == "" {
		return fmt.Errorf("%w: phase is empty", ErrActivityInvalid)
	}
	if len(k.Phase) > MaxActivityIDLen {
		return fmt.Errorf("%w: phase is %d bytes, limit %d", ErrActivityInvalid, len(k.Phase), MaxActivityIDLen)
	}
	// A separator that can appear inside a component would let two different
	// keys render to one store key, which is a cross-activity write.
	for _, part := range []string{k.GroupID, k.ActivityID, string(k.Phase)} {
		if strings.Contains(part, "/") {
			return fmt.Errorf("%w: key components must not contain '/'", ErrActivityInvalid)
		}
	}
	return nil
}

// String renders the key for messages and for a backend KeyFunc.
func (k ActivityKey) String() string {
	return k.GroupID + "/" + k.ActivityID + "/" + string(k.Phase)
}

// ActivityStatus is where an aggregation is.
//
//	pending ──(first notify)──> collecting ──> complete
//
// pending is not a formality. It is the state that says an activity exists and
// which games are expected, while no game has reached the phase yet. Nothing
// in this package moves an activity out of pending on its own — the grace
// deadline does not even exist until a game notifies — because global driving
// the timeline from its own clock is exactly what the boundary document
// forbade.
type ActivityStatus string

const (
	ActivityPending    ActivityStatus = "pending"
	ActivityCollecting ActivityStatus = "collecting"
	ActivityComplete   ActivityStatus = "complete"
)

// CompletionReason says which of the two completion paths finished an
// aggregation. It is recorded rather than inferred because "everyone reported"
// and "we gave up waiting" are different operational facts, and a result whose
// MissingGameSIDs is empty for the second reason is a bug worth being able to
// see.
type CompletionReason string

const (
	// CompletedCollected: every expected game notified, so the aggregation
	// completed immediately on the last notify.
	CompletedCollected CompletionReason = "collected"
	// CompletedGraceExpired: the grace window closed with games missing and
	// the sweep finished the aggregation.
	CompletedGraceExpired CompletionReason = "grace_expired"
)

// Activity is one cross-server aggregation.
type Activity struct {
	Key ActivityKey `json:"key"`
	// ExpectedGameSIDs is fixed when the activity opens. See
	// ErrNotifyUnexpected for why it is never taken from a notify.
	ExpectedGameSIDs []int32 `json:"expected_game_sids"`
	// NotifiedGameSIDs is the collecting snapshot: which expected games have
	// reported the phase. It carries no duplicates, so a redelivered
	// notification cannot make the set look collected.
	NotifiedGameSIDs []int32        `json:"notified_game_sids,omitempty"`
	Status           ActivityStatus `json:"status"`
	// GraceDeadlineUnix is zero until the first notify. Zero means "no game
	// has reached this phase", and the sweep skips it.
	GraceDeadlineUnix int64            `json:"grace_deadline_unix,omitempty"`
	CompletionReason  CompletionReason `json:"completion_reason,omitempty"`

	OpenedAtUnix      int64 `json:"opened_at_unix"`
	FirstNotifyAtUnix int64 `json:"first_notify_at_unix,omitempty"`
	CompletedAtUnix   int64 `json:"completed_at_unix,omitempty"`
	UpdatedAtUnix     int64 `json:"updated_at_unix"`
}

// Expects reports whether gameSID is in the expected set.
func (a Activity) Expects(gameSID int32) bool { return containsSID(a.ExpectedGameSIDs, gameSID) }

// Notified reports whether gameSID has already reported this phase.
func (a Activity) Notified(gameSID int32) bool { return containsSID(a.NotifiedGameSIDs, gameSID) }

// Collected reports whether every expected game has notified. Since
// NotifiedGameSIDs only ever gains expected sids and never duplicates, the
// count comparison is sound and does not need a set difference.
func (a Activity) Collected() bool {
	return len(a.ExpectedGameSIDs) > 0 && len(a.NotifiedGameSIDs) >= len(a.ExpectedGameSIDs)
}

// GraceExpired reports whether a started grace window has closed. A zero
// deadline is never expired: an activity no game has reached must not be
// completed by the passage of time.
func (a Activity) GraceExpired(nowUnix int64) bool {
	if a.GraceDeadlineUnix == 0 {
		return false
	}
	return nowUnix >= a.GraceDeadlineUnix
}

// MissingGameSIDs are the expected games that never notified.
func (a Activity) MissingGameSIDs() []int32 {
	var missing []int32
	for _, gameSID := range a.ExpectedGameSIDs {
		if !a.Notified(gameSID) {
			missing = append(missing, gameSID)
		}
	}
	return missing
}

// clone deep-copies the slices.
//
// This is not defensive habit: versionstore's memory implementation stores the
// value as given rather than round-tripping it through a codec, so a returned
// slice would share a backing array with stored state and a caller appending
// to it would mutate the store behind every compare-and-set. The Redis
// implementation would not show this, which is the worst kind of difference to
// leave to chance.
func (a Activity) clone() Activity {
	out := a
	out.ExpectedGameSIDs = cloneSIDs(a.ExpectedGameSIDs)
	out.NotifiedGameSIDs = cloneSIDs(a.NotifiedGameSIDs)
	return out
}

// ActivityResult is what gets dispatched to each game when an aggregation
// finishes: the coordination outcome, not the game's own business payload.
// MissingGameSIDs is part of it because a game settling on a grace-expired
// aggregation needs to know it is settling on partial input.
type ActivityResult struct {
	Key              ActivityKey      `json:"key"`
	Reason           CompletionReason `json:"reason"`
	NotifiedGameSIDs []int32          `json:"notified_game_sids,omitempty"`
	MissingGameSIDs  []int32          `json:"missing_game_sids,omitempty"`
	CompletedAtUnix  int64            `json:"completed_at_unix"`
}

func (r ActivityResult) clone() ActivityResult {
	out := r
	out.NotifiedGameSIDs = cloneSIDs(r.NotifiedGameSIDs)
	out.MissingGameSIDs = cloneSIDs(r.MissingGameSIDs)
	return out
}

// result builds the dispatch payload from a completed activity.
func (a Activity) result() ActivityResult {
	return ActivityResult{
		Key:              a.Key,
		Reason:           a.CompletionReason,
		NotifiedGameSIDs: cloneSIDs(a.NotifiedGameSIDs),
		MissingGameSIDs:  a.MissingGameSIDs(),
		CompletedAtUnix:  a.CompletedAtUnix,
	}
}

// ParticipantKey addresses one participant's progress within one aggregation.
type ParticipantKey struct {
	Activity      ActivityKey `json:"activity"`
	ParticipantID string      `json:"participant_id"`
}

func (k ParticipantKey) Validate() error {
	if err := k.Activity.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(k.ParticipantID) == "" {
		return fmt.Errorf("%w: participant id is empty", ErrParticipantInvalid)
	}
	if len(k.ParticipantID) > MaxParticipantIDLen {
		return fmt.Errorf("%w: participant id is %d bytes, limit %d",
			ErrParticipantInvalid, len(k.ParticipantID), MaxParticipantIDLen)
	}
	return nil
}

func (k ParticipantKey) String() string { return k.Activity.String() + "/" + k.ParticipantID }

// ProgressDelta is an increment to a participant's standing in an activity.
//
// Both fields must be non-negative and at least one positive. A negative delta
// looks like a convenience — "the game corrects a bad apply" — and is really
// how a double count gets papered over instead of reported: after a
// compensating write the score is right and the ledger says two requests
// applied, so nothing is left to notice. Progress in an activity aggregation
// only moves forward.
type ProgressDelta struct {
	Score    int64 `json:"score"`
	Progress int64 `json:"progress"`
}

func (d ProgressDelta) Validate() error {
	if d.Score < 0 || d.Progress < 0 {
		return fmt.Errorf("%w: delta must not be negative (score %d, progress %d)",
			ErrRequestInvalid, d.Score, d.Progress)
	}
	if d.Score == 0 && d.Progress == 0 {
		return fmt.Errorf("%w: delta must move something", ErrRequestInvalid)
	}
	return nil
}

// Participant is one participant's aggregated standing in one activity.
type Participant struct {
	Key           ActivityKey `json:"key"`
	ParticipantID string      `json:"participant_id"`
	Score         int64       `json:"score"`
	Progress      int64       `json:"progress"`

	// AppliedRequestIDs is a bounded FIFO ring of the request ids most
	// recently applied to this participant. It is written in the same
	// compare-and-set that moves Score, and that is the point: the ledger
	// reservation and the participant record are two keys and cannot be
	// written atomically, so "reserved" alone can never tell whether the apply
	// landed. This ring can, because it landed with the score or not at all.
	//
	// It is therefore not the general idempotency answer — that is the ledger,
	// which is bounded by time. This ring only has to cover the window between
	// a reservation and its being marked applied, so MaxProgressWindow bounds
	// concurrent in-flight requests per participant, not client retry
	// horizons.
	AppliedRequestIDs []string `json:"applied_request_ids,omitempty"`
	// Applies counts accepted applies. A replay does not increment it, which
	// is what makes "the replay was a no-op" observable rather than inferred.
	Applies       uint64 `json:"applies"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

// Applied reports whether requestID is still inside the participant's ring.
func (p Participant) Applied(requestID string) bool {
	return containsString(p.AppliedRequestIDs, requestID)
}

func (p Participant) clone() Participant {
	out := p
	out.AppliedRequestIDs = cloneStrings(p.AppliedRequestIDs)
	return out
}

// RequestKey addresses one reservation in the request ledger. The request id
// is scoped by activity and participant, so two games choosing the same
// request id for different participants do not collide — a global request id
// namespace would make one game's retry cancel another's write.
type RequestKey struct {
	Activity      ActivityKey `json:"activity"`
	ParticipantID string      `json:"participant_id"`
	RequestID     string      `json:"request_id"`
}

func (k RequestKey) Validate() error {
	if err := (ParticipantKey{Activity: k.Activity, ParticipantID: k.ParticipantID}).Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(k.RequestID) == "" {
		return fmt.Errorf("%w: request id is empty", ErrRequestInvalid)
	}
	if len(k.RequestID) > MaxRequestIDLen {
		return fmt.Errorf("%w: request id is %d bytes, limit %d",
			ErrRequestInvalid, len(k.RequestID), MaxRequestIDLen)
	}
	if strings.Contains(k.RequestID, "/") {
		return fmt.Errorf("%w: request id must not contain '/'", ErrRequestInvalid)
	}
	return nil
}

func (k RequestKey) String() string {
	return k.Activity.String() + "/" + k.ParticipantID + "/" + k.RequestID
}

// ReservationState is where a ledger entry is.
type ReservationState string

const (
	// ReservationReserved: the right to apply was claimed, the apply may or
	// may not have landed yet.
	ReservationReserved ReservationState = "reserved"
	// ReservationApplied: the apply landed, so any further request with this
	// id is a replay and must not reach the participant record.
	ReservationApplied ReservationState = "applied"
)

// ProgressReservation is one entry in the request ledger: the claim that gives
// a caller the right to apply a delta once.
//
// The ledger is bounded by TIME, not by count, and this is a deliberate
// choice with a stated cost. Its keys are request ids, so an entry cannot be
// evicted by a count bound without silently un-answering the oldest requests —
// the very ones a slow retry would come back for. Instead each entry carries
// ExpiresAtUnix and the store is wired with a matching TTL
// (versionstore.RedisConfig.TTL), so the ledger's size is bounded by the
// request rate times the TTL. The cost: a replay that arrives after the TTL is
// indistinguishable from a new request and will apply again, so the TTL must
// exceed the longest client retry horizon. Making that a configured duration
// rather than a comment is why ActivityConfig.ReservationTTL must be positive.
//
// Note that expiry is never a logic gate in this package. Nothing refuses a
// replay because its reservation "looks expired", because that would turn a
// redelivery into a double count on a clock skew.
type ProgressReservation struct {
	Key   RequestKey       `json:"key"`
	Delta ProgressDelta    `json:"delta"`
	State ReservationState `json:"state"`

	CreatedAtUnix int64 `json:"created_at_unix"`
	AppliedAtUnix int64 `json:"applied_at_unix,omitempty"`
	ExpiresAtUnix int64 `json:"expires_at_unix"`
}

// NotifyRefusal is why a notification was refused. Every value here is a case
// the boundary document requires to be audited.
type NotifyRefusal string

const (
	// RefusalUnknownActivity: no activity is open under that key. Audited
	// because a game notifying an activity global never opened is a
	// configuration split, and it is invisible without a record.
	RefusalUnknownActivity NotifyRefusal = "unknown_activity"
	// RefusalUnexpectedGame: the notifying game is not in the expected set.
	RefusalUnexpectedGame NotifyRefusal = "unexpected_game"
	// RefusalStaleStatus: the aggregation already completed and its result was
	// dispatched, so this notify cannot change it.
	RefusalStaleStatus NotifyRefusal = "stale_status"
	// RefusalLate: the grace window closed before this notify arrived.
	RefusalLate NotifyRefusal = "late"
)

// NotifyAudit is one refused notification.
type NotifyAudit struct {
	// Seq is assigned by the log, not by the caller: a caller-chosen sequence
	// can collide, and an audit that overwrote another audit would be worse
	// than no audit at all.
	Seq     uint64        `json:"seq"`
	Key     ActivityKey   `json:"key"`
	GameSID int32         `json:"game_sid"`
	Refusal NotifyRefusal `json:"refusal"`
	// Status is the activity's status at the moment of refusal, so the record
	// answers "late relative to what" without a second lookup.
	Status ActivityStatus `json:"status"`
	AtUnix int64          `json:"at_unix"`
	Detail string         `json:"detail,omitempty"`
}

// NotifyAuditLog is the append-only refusal log for one activity.
//
// Append-only by construction: entries are only ever appended, never rewritten
// or removed, and nothing in this package exposes a path that edits one. When
// the log is full it stops accepting entries and counts them instead, so the
// oldest evidence — when the misbehaviour started — survives a flood.
type NotifyAuditLog struct {
	Key     ActivityKey   `json:"key"`
	Entries []NotifyAudit `json:"entries,omitempty"`
	NextSeq uint64        `json:"next_seq"`
	// Overflowed counts refusals that were audited only as this number,
	// because the log was full. It is a metric, and it exists because a drop
	// nobody counts is how a silent path survives a rewrite.
	Overflowed uint64 `json:"overflowed"`
}

func (l NotifyAuditLog) clone() NotifyAuditLog {
	out := l
	out.Entries = cloneAudits(l.Entries)
	return out
}

// DispatchKey addresses the delivery of one aggregation result to one game.
// It is derived from the activity key and the game sid rather than being a
// minted id, so a retry never has to enumerate anything to find its dispatch.
type DispatchKey struct {
	Activity ActivityKey `json:"activity"`
	GameSID  int32       `json:"game_sid"`
}

func (k DispatchKey) String() string { return fmt.Sprintf("%s/%d", k.Activity, k.GameSID) }

// DispatchState is where a delivery is.
//
//	pending ──(ack)──> acked
//	pending ──(attempt budget spent)──> exhausted
type DispatchState string

const (
	DispatchPending DispatchState = "pending"
	DispatchAcked   DispatchState = "acked"
	// DispatchExhausted is terminal: no further attempt is handed out and an
	// ACK is refused. See ErrDispatchExhausted for why a late ACK is not
	// quietly accepted.
	DispatchExhausted DispatchState = "exhausted"
)

// Dispatch is a retryable delivery of one aggregation result to one game, with
// an ACK.
//
// It is a record rather than a fire-and-forget call because the boundary
// document says the delivery is a retryable task the game ACKs. A push with no
// record has no way to answer "did game 7 ever process the settlement", and
// that question is asked exactly when it can no longer be reconstructed.
type Dispatch struct {
	Key     ActivityKey `json:"key"`
	GameSID int32       `json:"game_sid"`
	// Token authorizes the ACK. It is server-minted and unguessable, and it
	// travels with the payload, which is what makes an ACK evidence that the
	// delivery arrived rather than a claim anyone could make about a
	// well-known key.
	Token  string         `json:"token"`
	Result ActivityResult `json:"result"`
	State  DispatchState  `json:"state"`

	Attempts    int `json:"attempts"`
	MaxAttempts int `json:"max_attempts"`
	// NextAttemptAtUnix is when the next attempt may be handed out. A dispatch
	// is created due immediately.
	NextAttemptAtUnix int64 `json:"next_attempt_at_unix"`

	CreatedAtUnix     int64 `json:"created_at_unix"`
	LastAttemptAtUnix int64 `json:"last_attempt_at_unix,omitempty"`
	AckedAtUnix       int64 `json:"acked_at_unix,omitempty"`
	ExhaustedAtUnix   int64 `json:"exhausted_at_unix,omitempty"`
}

// Due reports whether a pending dispatch may be attempted now.
func (d Dispatch) Due(nowUnix int64) bool {
	return d.State == DispatchPending && nowUnix >= d.NextAttemptAtUnix
}

// AttemptsLeft reports how many attempts the budget still allows.
func (d Dispatch) AttemptsLeft() int {
	if d.Attempts >= d.MaxAttempts {
		return 0
	}
	return d.MaxAttempts - d.Attempts
}

func (d Dispatch) clone() Dispatch {
	out := d
	out.Result = d.Result.clone()
	return out
}

// ActivityWindow is one group's index of activities that are not finished.
//
// versionstore is a keyed store with no listing — deliberately, since an
// unbounded scan is not a primitive worth offering — so the back-stop sweep
// needs an index of its own. This one is a single versioned record per group,
// which makes it the only enumerable thing in this file and therefore the only
// thing that has to be bounded by count.
//
// A key is added when the activity opens and removed when it is complete. The
// ordering matters and is asymmetric: the window entry is written BEFORE the
// activity record, because a window entry whose activity does not exist is
// pruned by the next sweep, whereas an activity with no window entry would
// never be swept and would sit at its grace deadline forever.
type ActivityWindow struct {
	GroupID string        `json:"group_id"`
	Keys    []ActivityKey `json:"keys,omitempty"`
	// RefusedOpens counts opens rejected because the window was full — the
	// backlog signal an operator needs to see before it turns into a stall.
	RefusedOpens uint64 `json:"refused_opens"`
}

func (w ActivityWindow) contains(key ActivityKey) bool {
	for _, existing := range w.Keys {
		if existing == key {
			return true
		}
	}
	return false
}

func (w ActivityWindow) clone() ActivityWindow {
	out := w
	if len(w.Keys) > 0 {
		out.Keys = make([]ActivityKey, len(w.Keys))
		copy(out.Keys, w.Keys)
	}
	return out
}

// validateExpectedGames checks the expected set at open time. Duplicates are
// refused rather than de-duplicated: Activity.Collected compares counts, so a
// duplicate in the expected set would mean the aggregation could never
// collect, and quietly rewriting the caller's set hides a configuration bug
// that will show up again elsewhere.
func validateExpectedGames(expected []int32) error {
	if len(expected) == 0 {
		return fmt.Errorf("%w: expected game set is empty", ErrActivityInvalid)
	}
	if len(expected) > MaxExpectedGames {
		return fmt.Errorf("%w: expected game set has %d entries, limit %d",
			ErrActivityInvalid, len(expected), MaxExpectedGames)
	}
	seen := make(map[int32]struct{}, len(expected))
	for _, gameSID := range expected {
		if gameSID <= 0 {
			return fmt.Errorf("%w: expected game sid must be positive, got %d", ErrActivityInvalid, gameSID)
		}
		if _, duplicate := seen[gameSID]; duplicate {
			return fmt.Errorf("%w: game %d appears twice in the expected set", ErrActivityInvalid, gameSID)
		}
		seen[gameSID] = struct{}{}
	}
	return nil
}

// validateActivityLimit is the shared bound for every listing and sweep here.
// A non-positive limit is an error and never means unlimited, which is design
// constraint 7 and the reason it cannot be bypassed by leaving the field at
// its zero value.
func validateActivityLimit(limit int) error {
	if limit <= 0 {
		return fmt.Errorf("%w: limit must be positive, got %d", ErrRangeInvalid, limit)
	}
	if limit > MaxPageSize {
		return fmt.Errorf("%w: limit %d exceeds %d", ErrRangeInvalid, limit, MaxPageSize)
	}
	return nil
}

func containsSID(haystack []int32, needle int32) bool {
	for _, value := range haystack {
		if value == needle {
			return true
		}
	}
	return false
}

func containsString(haystack []string, needle string) bool {
	for _, value := range haystack {
		if value == needle {
			return true
		}
	}
	return false
}

func cloneSIDs(in []int32) []int32 {
	if len(in) == 0 {
		return nil
	}
	out := make([]int32, len(in))
	copy(out, in)
	return out
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func cloneAudits(in []NotifyAudit) []NotifyAudit {
	if len(in) == 0 {
		return nil
	}
	out := make([]NotifyAudit, len(in))
	copy(out, in)
	return out
}

// appendBounded appends value to a FIFO ring of at most max entries, dropping
// the oldest. It always allocates: appending in place would write into the
// backing array the store still holds.
func appendBounded(ring []string, value string, max int) []string {
	next := make([]string, 0, min(len(ring)+1, max))
	start := 0
	if len(ring)+1 > max {
		start = len(ring) + 1 - max
	}
	next = append(next, ring[start:]...)
	return append(next, value)
}

// sortActivityKeys gives the sweep a deterministic order, so which activities
// a bounded sweep picks does not depend on map iteration or store internals.
func sortActivityKeys(keys []ActivityKey) {
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
}

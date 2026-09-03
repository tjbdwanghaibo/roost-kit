// Package match is matchmaking: subjects queue for a mode, the service groups
// them, and a group becomes a match exactly once.
//
// This is a redesign, not a port. The implementation it replaces had ten
// severe defects and — decisively — no production caller for its queue half,
// so there was no migration pressure to preserve its shape. Each decision
// here answers one of those defects:
//
//   - Ticket ids came off the wire and were used verbatim as identity, with no
//     per-subject uniqueness check. One client could queue the same player
//     twice and have it committed into two matches, or into one match twice.
//     Here the service mints ids, and a subject may hold one live ticket.
//   - Committing a match popped the queue, then wrote the match, then flipped
//     each ticket in a loop. A failure anywhere after the pop lost those
//     players: gone from the queue, still "waiting", with no reaper. Here the
//     commit is a single atomic step and the queue is only mutated by it.
//   - The version used for optimistic writes was a process-local counter
//     starting at one, so versions from different replicas — and from the same
//     replica after a restart — were incomparable, and a stale write was
//     dropped while the caller was told it succeeded. Here versions come from
//     the store and a refused write is an error.
//   - Server-generated ids were "ticket-1", "match-1" process sequences, so
//     two replicas minted identical ids and an idempotent enqueue could return
//     another player's ticket.
//   - Cancel, read and close carried no caller identity, over guessable ids.
//     Here every operation naming a ticket also names its subject.
//   - ExpiresAt was written and never read: nothing enforced a timeout. Here
//     expiry is enforced on read and by an explicit sweep.
package match

import (
	"errors"
	"fmt"
	"time"
)

// Error codes.
const (
	CodeOK int32 = 0

	CodeQueueInvalid   int32 = 550101
	CodeSubjectInvalid int32 = 550102
	CodeTicketInvalid  int32 = 550103
	CodeTicketMissing  int32 = 550104
	CodeAlreadyQueued  int32 = 550105
	CodeNotPermitted   int32 = 550106
	CodeTicketMatched  int32 = 550107
	CodeConflict       int32 = 550108
	CodeStoreFailed    int32 = 550109
)

var (
	ErrQueueInvalid   = errors.New("match: queue is invalid")
	ErrSubjectInvalid = errors.New("match: subject is invalid")
	ErrTicketInvalid  = errors.New("match: ticket is invalid")
	ErrTicketMissing  = errors.New("match: ticket not found")
	// ErrAlreadyQueued reports that the subject already holds a live ticket.
	// A subject with two live tickets is what allowed one player to be
	// committed into two matches.
	ErrAlreadyQueued = errors.New("match: subject is already queued")
	// ErrNotPermitted reports that the caller does not own the ticket. Every
	// operation naming a ticket also names its subject, so a guessable id is
	// not enough to act on someone else's ticket.
	ErrNotPermitted  = errors.New("match: caller does not own this ticket")
	ErrTicketMatched = errors.New("match: ticket is already matched")
	ErrConflict      = errors.New("match: conflict")
)

// Limits bound what a queue may hold. Every one of these is a bound the
// implementation this replaces did not have: its queue length was unbounded
// and each enqueue read every queued ticket.
const (
	// MaxQueueLength caps one queue. Reaching it rejects new tickets rather
	// than degrading every enqueue.
	MaxQueueLength = 10000
	// MaxGroupSize caps how many subjects one match may hold.
	MaxGroupSize = 64
	// MaxPageSize caps a listing.
	MaxPageSize = 200
	// DefaultTicketTTL is how long a ticket waits before it expires. Unlike
	// the implementation this replaces, this is enforced.
	DefaultTicketTTL = 5 * time.Minute
)

// SubjectKind says what kind of entity is queueing. It is opaque to this
// service: a subject is whatever the caller says it is.
type SubjectKind string

// Subject is who is queueing.
type Subject struct {
	Kind SubjectKind `json:"kind"`
	ID   int64       `json:"id"`
	// Score is what the grouping policy ranks on; its meaning is the caller's.
	Score int64 `json:"score"`
	// Payload is opaque data carried through to the match, bounded by
	// MaxPayloadBytes.
	Payload []byte `json:"payload,omitempty"`
}

// MaxPayloadBytes bounds a subject's opaque payload.
const MaxPayloadBytes = 1024

func (s Subject) Validate() error {
	if s.Kind == "" {
		return fmt.Errorf("%w: kind is empty", ErrSubjectInvalid)
	}
	if s.ID == 0 {
		return fmt.Errorf("%w: id is zero", ErrSubjectInvalid)
	}
	if len(s.Payload) > MaxPayloadBytes {
		return fmt.Errorf("%w: payload is %d bytes, limit %d", ErrSubjectInvalid, len(s.Payload), MaxPayloadBytes)
	}
	return nil
}

// Key identifies one subject uniquely, and is what enforces "one live ticket
// per subject".
func (s Subject) Key() string { return fmt.Sprintf("%s:%d", s.Kind, s.ID) }

// Queue identifies one matchmaking pool.
type Queue struct {
	// Mode is the gameplay mode, opaque here.
	Mode string `json:"mode"`
	// GroupSize is how many subjects a match holds. A queue is defined by it:
	// a 2-player and a 5-player pool of the same mode are different queues.
	GroupSize int `json:"group_size"`
	// Partition scopes a queue, e.g. a region or a server group. Tickets in
	// different partitions never match together, and — this is the part the
	// implementation this replaces lacked — it is part of the routing key, so
	// operations on one queue land on one instance.
	Partition string `json:"partition"`
}

func (q Queue) Validate() error {
	if q.Mode == "" {
		return fmt.Errorf("%w: mode is empty", ErrQueueInvalid)
	}
	if q.GroupSize < 2 {
		return fmt.Errorf("%w: group size must be at least 2, got %d", ErrQueueInvalid, q.GroupSize)
	}
	if q.GroupSize > MaxGroupSize {
		return fmt.Errorf("%w: group size %d exceeds %d", ErrQueueInvalid, q.GroupSize, MaxGroupSize)
	}
	return nil
}

// Key renders the queue identity. It is also the affinity key: passing it to
// servicerpc.WithAffinityKey routes every operation on one queue to one
// instance, so the compare-and-set below contends with itself rather than
// with a different replica reading the same head.
func (q Queue) Key() string {
	return fmt.Sprintf("%s:%d:%s", q.Mode, q.GroupSize, q.Partition)
}

// TicketState is where a ticket is in its lifecycle.
//
//	waiting ─┬─> matched    (committed into a match)
//	         ├─> cancelled  (withdrawn by its subject)
//	         └─> expired    (waited past its deadline)
//
// matched, cancelled and expired are terminal.
type TicketState string

const (
	TicketWaiting   TicketState = "waiting"
	TicketMatched   TicketState = "matched"
	TicketCancelled TicketState = "cancelled"
	TicketExpired   TicketState = "expired"
)

func (s TicketState) terminal() bool {
	return s == TicketMatched || s == TicketCancelled || s == TicketExpired
}

// Ticket is one subject's place in a queue.
type Ticket struct {
	// ID is minted by the service, never accepted from a caller. A
	// caller-supplied id was what let one client queue a subject twice and
	// what let an "idempotent" enqueue return another player's ticket.
	ID      string      `json:"id"`
	Queue   Queue       `json:"queue"`
	Subject Subject     `json:"subject"`
	State   TicketState `json:"state"`
	// MatchID is set when State is matched.
	MatchID string `json:"match_id,omitempty"`
	// RequestID is the caller's idempotency key for the enqueue that created
	// this ticket. A repeat enqueue with the same key returns this ticket
	// instead of creating a second one.
	RequestID      string `json:"request_id,omitempty"`
	CreatedAtUnix  int64  `json:"created_at_unix"`
	ExpiresAtUnix  int64  `json:"expires_at_unix"`
	ResolvedAtUnix int64  `json:"resolved_at_unix,omitempty"`
}

// Expired reports whether a waiting ticket has passed its deadline. A resolved
// ticket never expires — its state is already terminal.
func (t Ticket) Expired(now time.Time) bool {
	if t.State != TicketWaiting || t.ExpiresAtUnix == 0 {
		return false
	}
	return now.Unix() >= t.ExpiresAtUnix
}

// Match is a committed group.
type Match struct {
	ID    string `json:"id"`
	Queue Queue  `json:"queue"`
	// Members are the subjects, in the order the grouping policy placed them.
	Members []Subject `json:"members"`
	// TicketIDs are the tickets this match consumed, in member order. They
	// are recorded so a match can be reconciled against its tickets.
	TicketIDs     []string `json:"ticket_ids"`
	CreatedAtUnix int64    `json:"created_at_unix"`
}

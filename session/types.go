// Package session is the bounded-run primitive extracted from a dungeon
// instance service: one owner enters a run, the run has a deadline, it reaches
// a terminal state, and the resources it allocated are released exactly once.
//
// Only the primitive was extracted. The service it came from was not a
// candidate for extraction at all: its "port" types were the game process's
// own manager structs, so the interfaces did not invert any dependency, and it
// reached directly into the live scene graph. What IS general is the lifecycle
// it got wrong, and it got it wrong in four confirmed ways:
//
//   - Idempotency failed open. The dedupe key was built from
//     (owner, definition, requestID) and returned "" when the requestID was
//     empty — and an empty key made the lookup return "not found" rather than
//     an error. So a caller that omitted the request id, or sent a fresh one
//     each time, could Enter repeatedly and allocate a new run and a new scene
//     every time, with no capacity accounting anywhere.
//
//     Worse, the older runs became unreachable: Leave needs a run id, the
//     caller only holds the last one it was given, and nothing indexed a run
//     by its owner. Those runs could never be left and their scenes were never
//     destroyed.
//
//     Here the request id is required, and there is at most one live run per
//     owner — enforced by insert-only versioned state, so a caller racing
//     itself loses rather than getting a second run.
//
//   - Compensation was asymmetric. The first allocation step's failure path
//     released what it had taken; the second step's failure path just
//     returned, leaking both. The asymmetry is the evidence: the author knew
//     compensation was needed and simply missed one branch. Here attachment is
//     one CAS on the run, and a run that fails to attach is released by the
//     same path that releases any other run — there is no second branch to
//     forget.
//
//   - The deadline was computed, stored, and sent to clients, and nothing in
//     the entire repository ever compared it to a clock. Here Get resolves an
//     elapsed deadline before returning, so a reader is correct before
//     anything sweeps, and Sweep is what makes it durable.
//
//   - The version field was decoration. Every save incremented it and then
//     wrote unconditionally, and when the store was absent the save returned
//     nil — reporting success for a write that never happened. Here state
//     lives in versionstore, whose contract has no unconditional write.
package session

import (
	"errors"
	"fmt"
	"strings"
)

// Error codes.
const (
	CodeOK int32 = 0

	CodeRunInvalid      int32 = 610101
	CodeRunMissing      int32 = 610102
	CodeNotOwner        int32 = 610103
	CodeAlreadyRunning  int32 = 610104
	CodeRunTerminal     int32 = 610105
	CodeRunExpired      int32 = 610106
	CodeNotAttached     int32 = 610107
	CodeAlreadyAttached int32 = 610108
	CodeRequestInvalid  int32 = 610109
	CodeRangeInvalid    int32 = 610110
	CodeConflict        int32 = 610111
	CodeStoreFailed     int32 = 610112
)

var (
	ErrRunInvalid = errors.New("session: run is invalid")
	ErrRunMissing = errors.New("session: run not found")
	// ErrNotOwner reports that the caller does not own this run. The service
	// this replaces checked ownership on one of its four entry points.
	ErrNotOwner = errors.New("session: caller does not own this run")
	// ErrAlreadyRunning reports that the owner already holds a live run. Its
	// absence is what let repeated Enter calls allocate unbounded scenes.
	ErrAlreadyRunning = errors.New("session: owner already holds a live run")
	ErrRunTerminal    = errors.New("session: run has already finished")
	// ErrRunExpired reports that the run's deadline has passed. The service
	// this replaces computed a deadline that nothing ever read.
	ErrRunExpired      = errors.New("session: run deadline has passed")
	ErrNotAttached     = errors.New("session: run has no attachment")
	ErrAlreadyAttached = errors.New("session: run is already attached")
	ErrRequestInvalid  = errors.New("session: request is invalid")
	ErrRangeInvalid    = errors.New("session: range is invalid")
	ErrConflict        = errors.New("session: conflict")
)

// Bounds.
const (
	// MaxPageSize bounds a listing, and cannot be bypassed with a zero limit.
	MaxPageSize = 200
	// MaxContextEntries bounds the opaque evaluation context a caller may
	// attach to a run, so one enter cannot grow without limit.
	MaxContextEntries = 64
	// MaxResourceEntries bounds how many external resources one run records.
	MaxResourceEntries = 8
)

// State is where a run stands.
//
//	open ──┬──> succeeded
//	       ├──> failed
//	       ├──> abandoned   (the owner left)
//	       └──> expired     (the deadline passed with no outcome)
//
// Expired is a distinct terminal state from abandoned on purpose. "The player
// gave up" and "the run ran out of time" call for different rewards, different
// telemetry and different support answers, and a service that collapses them
// cannot tell an operator which happened.
type State string

const (
	StateOpen      State = "open"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
	StateAbandoned State = "abandoned"
	StateExpired   State = "expired"
)

// Terminal reports whether the run needs no further lifecycle work.
//
// It enumerates the terminal states rather than testing "not open", because
// "not open" makes the ZERO value terminal — and a zero Run returned alongside
// an error then reports itself as a finished run. That is not a hypothetical:
// it made a test assert successfully against a zero value.
func (s State) Terminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateAbandoned, StateExpired:
		return true
	default:
		return false
	}
}

// Resource is one external thing a run allocated, recorded so it can be
// released exactly once.
//
// The service this replaces allocated a scene and a replica through two
// separate calls with two separate compensation paths, one of which was
// missing. Recording allocations on the run means release walks a list rather
// than being a hand-written unwind per failure branch.
type Resource struct {
	// Kind names what it is — a scene, a replica, a reserved shard. Opaque
	// here: this package releases resources by handing them back, it does not
	// know what they are.
	Kind string `json:"kind"`
	// ID identifies it within its kind.
	ID string `json:"id"`
	// ReleasedAtUnix is set when the resource has been handed back, so a
	// retried release is a no-op rather than a second free.
	ReleasedAtUnix int64 `json:"released_at_unix,omitempty"`
}

// Released reports whether this resource has already been handed back.
func (r Resource) Released() bool { return r.ReleasedAtUnix > 0 }

func (r Resource) Validate() error {
	if strings.TrimSpace(r.Kind) == "" {
		return fmt.Errorf("%w: resource kind is empty", ErrRunInvalid)
	}
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("%w: resource id is empty", ErrRunInvalid)
	}
	return nil
}

// Run is one bounded execution owned by one owner.
type Run struct {
	// ID identifies the run. Server-minted and unguessable: the service this
	// replaces used a per-process counter, so ids collided across replicas
	// and were trivially enumerable.
	ID string `json:"id"`
	// OwnerID is who entered. Ownership is checked on every operation, and
	// the owner is a parameter of every call rather than a request field.
	OwnerID int64 `json:"owner_id"`
	// Kind is what was entered — a dungeon definition id, a trial id.
	Kind string `json:"kind"`
	// RequestID is the enter idempotency key. Required; see Config.
	RequestID string `json:"request_id"`

	State State `json:"state"`
	// Resources are the external allocations this run holds.
	Resources []Resource `json:"resources,omitempty"`
	// Context is opaque per-run data the caller evaluates outcomes against.
	Context map[string]string `json:"context,omitempty"`

	// Outcome is set when the run reaches a terminal state, opaque to this
	// package.
	Outcome string `json:"outcome,omitempty"`

	StartedAtUnix int64 `json:"started_at_unix"`
	// DeadlineUnix is when the run expires. It must be set, and this package
	// actually reads it — which is the whole difference from the service it
	// replaces.
	DeadlineUnix   int64 `json:"deadline_unix"`
	FinishedAtUnix int64 `json:"finished_at_unix,omitempty"`
	UpdatedAtUnix  int64 `json:"updated_at_unix"`
}

// Expired reports whether an open run's deadline has passed.
func (r Run) Expired(nowUnix int64) bool {
	if r.State != StateOpen || r.DeadlineUnix == 0 {
		return false
	}
	return nowUnix >= r.DeadlineUnix
}

// Live reports whether the run is open and within its deadline.
func (r Run) Live(nowUnix int64) bool { return r.State == StateOpen && !r.Expired(nowUnix) }

// Attached returns the first unreleased resource of a kind.
func (r Run) Attached(kind string) (Resource, bool) {
	for _, resource := range r.Resources {
		if resource.Kind == kind && !resource.Released() {
			return resource, true
		}
	}
	return Resource{}, false
}

// Pending returns the resources that have not been released.
func (r Run) Pending() []Resource {
	out := make([]Resource, 0, len(r.Resources))
	for _, resource := range r.Resources {
		if !resource.Released() {
			out = append(out, resource)
		}
	}
	return out
}

func (r Run) Validate() error {
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("%w: id is empty", ErrRunInvalid)
	}
	if r.OwnerID <= 0 {
		return fmt.Errorf("%w: owner id must be positive", ErrRunInvalid)
	}
	if strings.TrimSpace(r.Kind) == "" {
		return fmt.Errorf("%w: kind is empty", ErrRunInvalid)
	}
	if strings.TrimSpace(r.RequestID) == "" {
		return fmt.Errorf("%w: an idempotency key is required", ErrRequestInvalid)
	}
	if r.DeadlineUnix <= 0 {
		// A run with no deadline is a run nothing ever cleans up, and the
		// resources it holds are held forever.
		return fmt.Errorf("%w: a run must have a deadline", ErrRunInvalid)
	}
	if r.StartedAtUnix > 0 && r.DeadlineUnix <= r.StartedAtUnix {
		return fmt.Errorf("%w: deadline %d is not after the start %d",
			ErrRunInvalid, r.DeadlineUnix, r.StartedAtUnix)
	}
	if len(r.Context) > MaxContextEntries {
		return fmt.Errorf("%w: context has %d entries, limit %d",
			ErrRunInvalid, len(r.Context), MaxContextEntries)
	}
	if len(r.Resources) > MaxResourceEntries {
		return fmt.Errorf("%w: run holds %d resources, limit %d",
			ErrRunInvalid, len(r.Resources), MaxResourceEntries)
	}
	for _, resource := range r.Resources {
		if err := resource.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (r Run) clone() Run {
	out := r
	out.Resources = append([]Resource(nil), r.Resources...)
	if r.Context != nil {
		out.Context = make(map[string]string, len(r.Context))
		for key, value := range r.Context {
			out.Context[key] = value
		}
	}
	return out
}

// Claim is the per-owner exclusive record that makes "one live run per owner"
// an insert rather than a count.
//
// Deliberately NOT a directory reservation: a directory is idempotent for the
// same owner, so an owner racing itself would share one claim and every racer
// would proceed. That is the exact defect this replaces — repeated Enter calls
// allocating unbounded runs — so the claim is insert-only versioned state,
// where a second entrant loses regardless of who it is.
type Claim struct {
	OwnerID       int64  `json:"owner_id"`
	RunID         string `json:"run_id"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

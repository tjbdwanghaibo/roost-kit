// Package directory is the reserve/commit/cancel primitive behind globally
// unique names and exclusive membership.
//
// Two independent implementations of this idea in one business repository had
// the same three defects, all of which this contract makes unrepresentable:
//
//   - The reservation and the record it protects were written to two stores
//     with no transaction, and the compensating release dropped its error. A
//     crash between the writes burned the name permanently: reserved to an
//     owner that does not exist, and unreleasable by any code path.
//   - A second write path (an "upsert" API) bypassed reservation entirely, so
//     the same name could be taken twice and a rename orphaned the old
//     reservation.
//   - Release compared the owner and then deleted in a separate round trip, so
//     a release racing a re-reservation deleted the new owner's claim.
//
// The shape here is a two-phase claim over versioned state: Reserve takes the
// key for one owner and returns a claim, Commit makes it permanent, Cancel
// releases it. A reservation carries the owner and an expiry, so an abandoned
// claim frees itself instead of burning the key forever — the failure mode
// that has no recovery path.
//
// What it deliberately does not offer: a way to write an entry without going
// through Reserve. There is no Set.
package directory

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrKeyTaken reports that the key is reserved or committed to a
	// different owner.
	ErrKeyTaken = errors.New("directory: key is taken")

	// ErrClaimNotFound reports that no reservation matches the claim — it was
	// cancelled, it expired, or it was never made.
	ErrClaimNotFound = errors.New("directory: claim not found")

	// ErrClaimStale reports that a reservation exists for the key but not for
	// this claim: the caller's token no longer matches. Returned rather than
	// silently succeeding, because "commit something someone else reserved"
	// must never look like success.
	ErrClaimStale = errors.New("directory: claim is stale")

	// ErrOwnerMismatch reports that the entry belongs to a different owner.
	ErrOwnerMismatch = errors.New("directory: owner mismatch")

	ErrKeyEmpty   = errors.New("directory: key is empty")
	ErrOwnerEmpty = errors.New("directory: owner is empty")
)

// State is where a directory key is in its lifecycle.
type State string

const (
	// StateReserved means the key is held by a claim that has not been
	// committed. It becomes free again when the claim expires.
	StateReserved State = "reserved"
	// StateCommitted means the key belongs to its owner permanently.
	StateCommitted State = "committed"
)

// Owner identifies who holds a key. It is a string so a directory can key on
// whatever identity the caller has — a player id, an alliance id, a tenant —
// without this package knowing which.
type Owner string

// Entry is a directory record.
type Entry struct {
	// Key is the normalized unique key, as returned by the Normalizer.
	Key string `json:"key"`
	// Raw is the key as the caller supplied it, kept for display. Uniqueness
	// is decided on Key alone, so two Raw values differing only by case or
	// surrounding space collide.
	Raw string `json:"raw"`
	// Owner holds the key.
	Owner Owner `json:"owner"`
	// State is Reserved or Committed.
	State State `json:"state"`
	// Token identifies the reservation. Commit and Cancel require it, so a
	// caller cannot act on a reservation it does not hold.
	Token string `json:"token"`
	// ExpiresAtUnix is when a reservation lapses; zero for a committed entry.
	// An expired reservation is treated as absent, which is what keeps an
	// abandoned claim from burning the key.
	ExpiresAtUnix int64 `json:"expires_at_unix"`
	// ReservedAtUnix and CommittedAtUnix are for operators, not for logic.
	ReservedAtUnix  int64 `json:"reserved_at_unix"`
	CommittedAtUnix int64 `json:"committed_at_unix"`
}

// Expired reports whether a reservation has lapsed at now. A committed entry
// never expires.
func (e Entry) Expired(now time.Time) bool {
	if e.State == StateCommitted || e.ExpiresAtUnix == 0 {
		return false
	}
	return now.Unix() >= e.ExpiresAtUnix
}

// Claim is what Reserve hands back. Commit and Cancel take it, so holding a
// claim is what authorizes acting on the reservation.
type Claim struct {
	Key   string
	Owner Owner
	Token string
	// ExpiresAt is when the reservation lapses if not committed.
	ExpiresAt time.Time
}

// Normalizer maps a caller-supplied key to the form uniqueness is decided on.
// It is required: leaving it out is how "Alice" and "alice" both get reserved.
type Normalizer func(raw string) (string, error)

// Directory reserves unique keys and records exclusive ownership.
type Directory interface {
	// Reserve takes the key for owner. It fails with ErrKeyTaken when the key
	// is held by anyone else, and is idempotent for the same owner: a repeat
	// Reserve returns the existing claim rather than a second one, so a
	// retried request does not consume two reservations.
	Reserve(ctx context.Context, raw string, owner Owner, ttl time.Duration) (Claim, error)

	// Commit makes a reservation permanent. It requires the claim's token, so
	// committing someone else's reservation is not expressible.
	Commit(ctx context.Context, claim Claim) (Entry, error)

	// Cancel releases a reservation. It requires the token for the same
	// reason, and is idempotent: cancelling an already-released claim
	// succeeds, because a caller retrying its own rollback must not fail.
	Cancel(ctx context.Context, claim Claim) error

	// Lookup returns the entry for a key. Expired reservations report absent.
	Lookup(ctx context.Context, raw string) (Entry, bool, error)

	// Release removes a committed entry, but only for its owner. This is the
	// path a rename or a deletion takes; going around it is what orphaned
	// reservations in the implementation this replaces.
	Release(ctx context.Context, raw string, owner Owner) error
}

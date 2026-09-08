package mail

import (
	"fmt"
	"sort"
)

// Mailbox is one player's state for every mail delivered to them: read
// status, claim record, and the unread count.
//
// It is ONE versioned entry on purpose. The implementation this replaces kept
// per-mail state documents and derived the unread count with an aggregation
// over every active envelope, which meant:
//
//   - the count could disagree with the statuses it summarized, because they
//     were written separately; and
//   - one player opening their mailbox ran an unbounded `$lookup` over a
//     collection that holds every server-wide mail for every player.
//
// Here a status change and the count move in the same compare-and-set, so they
// cannot disagree, and reading the whole mailbox is one read.
type Mailbox struct {
	PlayerID int64 `json:"player_id"`
	// Entries is the per-mail state, keyed by envelope id. Bounded by
	// MaxMailboxEntries; see evict.
	Entries map[string]Entry `json:"entries,omitempty"`
	// Unread is maintained here rather than counted on read. It is the
	// authoritative count precisely because it moves with the statuses.
	Unread int32 `json:"unread"`
	// Evicted counts entries retention dropped, so a mailbox that is losing
	// history says so instead of just being shorter than the client expects.
	Evicted uint64 `json:"evicted,omitempty"`

	UpdatedAtUnix int64 `json:"updated_at_unix"`
}

// Entry is one mail's state for one player.
type Entry struct {
	MailID string `json:"mail_id"`
	Status Status `json:"status"`

	// ClaimToken is the idempotency key for this mail's attachment delivery.
	//
	// It is minted by the service and is CONSTANT for the life of this entry:
	// a retried claim gets the same token back, never a new one. That is the
	// whole fix. The implementation this replaces let the CLIENT supply the
	// key and allowed a lapsed reservation to be taken over by a different
	// key, so a caller that retried after a lost response was handed the
	// attachment again under a key the delivery side had never seen.
	ClaimToken string `json:"claim_token,omitempty"`
	// ClaimDeadlineUnix is when an in-flight claim may be retried. It bounds
	// how long a crashed deliverer blocks the mail; it does NOT allow a
	// different token to take over, because the token does not change.
	ClaimDeadlineUnix int64 `json:"claim_deadline_unix,omitempty"`
	// ClaimAttempts counts reservations. It is not an idempotency key and is
	// not part of one — it exists so an operator can see a mail whose
	// delivery keeps failing.
	ClaimAttempts int32 `json:"claim_attempts,omitempty"`

	DeliveredAtUnix int64 `json:"delivered_at_unix"`
	UpdatedAtUnix   int64 `json:"updated_at_unix"`
}

// claimable reports whether a reservation may be taken now, and why not when
// it may not.
func (e Entry) claimable(nowUnix int64) error {
	switch {
	case e.Status == StatusClaimed:
		return fmt.Errorf("%w: mail %s", ErrAlreadyClaimed, e.MailID)
	case e.Status == StatusDeleted:
		// Deliberate: deleting a mail does not release its attachment. A
		// player who could delete-then-claim would claim twice.
		return fmt.Errorf("%w: mail %s was deleted", ErrMailMissing, e.MailID)
	case e.ClaimToken != "" && e.ClaimDeadlineUnix > nowUnix:
		return fmt.Errorf("%w: mail %s until %d", ErrClaimHeld, e.MailID, e.ClaimDeadlineUnix)
	default:
		return nil
	}
}

func (m *Mailbox) init(playerID int64) {
	m.PlayerID = playerID
	if m.Entries == nil {
		m.Entries = make(map[string]Entry)
	}
}

// entry returns the state for one mail, and whether it was delivered.
func (m Mailbox) entry(mailID string) (Entry, bool) {
	entry, ok := m.Entries[mailID]
	return entry, ok
}

// setStatus moves one entry and keeps Unread in step.
//
// It is the ONLY expression in this package that writes a status, which is
// what makes the count trustworthy: there is no path that changes a status
// without passing through the counter.
func (m *Mailbox) setStatus(mailID string, next Status, nowUnix int64) (Entry, bool) {
	entry, ok := m.Entries[mailID]
	if !ok {
		return Entry{}, false
	}
	if entry.Status == next {
		return entry, false
	}
	if entry.Status == StatusUnread {
		m.Unread--
	}
	if next == StatusUnread {
		m.Unread++
	}
	entry.Status = next
	entry.UpdatedAtUnix = nowUnix
	m.Entries[mailID] = entry
	m.UpdatedAtUnix = nowUnix
	return entry, true
}

// deliver records a mail as delivered to this mailbox, idempotently.
func (m *Mailbox) deliver(mailID string, nowUnix int64) (Entry, bool) {
	if existing, ok := m.Entries[mailID]; ok {
		// Idempotent: a redelivered mail does not become unread again and
		// does not increment the count a second time. The transport is
		// at-least-once, so this path is the normal one, not the exception.
		return existing, false
	}
	entry := Entry{
		MailID: mailID, Status: StatusUnread,
		DeliveredAtUnix: nowUnix, UpdatedAtUnix: nowUnix,
	}
	m.Entries[mailID] = entry
	m.Unread++
	m.UpdatedAtUnix = nowUnix
	return entry, true
}

// evict brings the mailbox back within MaxMailboxEntries by dropping the
// oldest entries that are safe to drop, and counts what it dropped.
//
// Safe means terminal: deleted, or claimed. An unread mail is never evicted to
// make room for a newer one — silently discarding mail a player has not seen
// is the failure this bound exists to make visible, not to cause. A mailbox
// that cannot be brought under the limit refuses the delivery instead, which
// is an error the caller can act on rather than a loss nobody observes.
func (m *Mailbox) evict(nowUnix int64) {
	if len(m.Entries) <= MaxMailboxEntries {
		return
	}
	evictable := make([]Entry, 0, len(m.Entries))
	for _, entry := range m.Entries {
		if entry.Status == StatusDeleted || entry.Status == StatusClaimed {
			evictable = append(evictable, entry)
		}
	}
	sort.Slice(evictable, func(i, j int) bool {
		if evictable[i].UpdatedAtUnix == evictable[j].UpdatedAtUnix {
			return evictable[i].MailID < evictable[j].MailID
		}
		return evictable[i].UpdatedAtUnix < evictable[j].UpdatedAtUnix
	})
	for _, entry := range evictable {
		if len(m.Entries) <= MaxMailboxEntries {
			break
		}
		delete(m.Entries, entry.MailID)
		m.Evicted++
		m.UpdatedAtUnix = nowUnix
	}
}

// full reports whether the mailbox is at its bound with nothing evictable, so
// a delivery must be refused rather than silently dropped.
func (m Mailbox) full() bool {
	if len(m.Entries) < MaxMailboxEntries {
		return false
	}
	for _, entry := range m.Entries {
		if entry.Status == StatusDeleted || entry.Status == StatusClaimed {
			return false
		}
	}
	return true
}

func (m Mailbox) clone() Mailbox {
	out := m
	if m.Entries != nil {
		out.Entries = make(map[string]Entry, len(m.Entries))
		for id, entry := range m.Entries {
			out.Entries[id] = entry
		}
	}
	return out
}

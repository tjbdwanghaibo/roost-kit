// Package mail is the mailbox service: envelopes with optional attachments,
// per-player read/claim state, and the three-phase claim that hands an
// attachment to a caller exactly once.
//
// The implementation this replaces had four defects that shaped this design:
//
//   - The claim idempotency key came from the client. `ClaimRequest.RequestID`
//     was whatever the caller sent, and the reservation could be taken over
//     once its lease elapsed. So a caller whose commit response was lost could
//     retry with a NEW request id, win the reservation, and be handed the
//     attachment a second time — the goods were already in the player's
//     inventory. The only thing standing between that and a duplicate grant
//     was a dedupe table three hops away in the game process.
//
//     Here the claim token is minted by the service and is CONSTANT for one
//     (player, mail) pair. A retry cannot produce a different key, so a
//     delivery side that dedupes on it converges no matter how many times the
//     caller retries or how long it waits.
//
//   - Listing was O(n) round trips with no bound on the round trips. `List`
//     clamped how many items it RETURNED, then looped fetching pages of
//     `limit*4` envelopes and issued one state read per envelope, continuing
//     until it had filled the page. A player with many deleted mails made one
//     protocol packet into an unbounded number of database calls. Here the
//     bound is on the work, not just the output: one envelope page plus one
//     mailbox read, and the mailbox is a single versioned entry.
//
//   - The unread count was an unbounded aggregation. Every summary ran a
//     `$lookup`/`$unwind`/`$group` over every active envelope for that player,
//     and server-wide mail is in that collection for everybody. Here the count
//     lives in the mailbox and moves in the SAME compare-and-set as the status
//     change, so it cannot disagree with the statuses it summarizes.
//
//   - `PlayerID` came from the request body, so any caller could claim another
//     player's attachments. Here the caller identity is a parameter of the
//     service call and is never read out of a request struct.
package mail

import (
	"errors"
	"fmt"
	"strings"
)

// Error codes.
const (
	CodeOK int32 = 0

	CodeMailInvalid     int32 = 590101
	CodeMailMissing     int32 = 590102
	CodeBodyInvalid     int32 = 590103
	CodeAudienceInvalid int32 = 590104
	CodeNotRecipient    int32 = 590105
	CodeExpired         int32 = 590106
	CodeNoAttachment    int32 = 590107
	CodeAlreadyClaimed  int32 = 590108
	CodeClaimHeld       int32 = 590109
	CodeClaimTokenWrong int32 = 590110
	CodeMailboxFull     int32 = 590111
	CodeRangeInvalid    int32 = 590112
	CodeRequestInvalid  int32 = 590113
	CodeConflict        int32 = 590114
	CodeStoreFailed     int32 = 590115
	CodeDeliveryRefused int32 = 590116
)

var (
	ErrMailInvalid     = errors.New("mail: mail is invalid")
	ErrMailMissing     = errors.New("mail: mail not found")
	ErrBodyInvalid     = errors.New("mail: body is invalid")
	ErrAudienceInvalid = errors.New("mail: audience is invalid")
	// ErrNotRecipient reports that the caller is not among this envelope's
	// recipients. The implementation this replaces took the player id from the
	// request body, so there was no caller to compare against and this error
	// had no reason to exist.
	ErrNotRecipient   = errors.New("mail: caller is not a recipient")
	ErrExpired        = errors.New("mail: mail has expired")
	ErrNoAttachment   = errors.New("mail: mail has no attachment")
	ErrAlreadyClaimed = errors.New("mail: attachment is already claimed")
	// ErrClaimHeld reports that a claim is in flight and its deadline has not
	// passed. It is distinct from ErrAlreadyClaimed: "someone is delivering
	// this right now" and "this was delivered" call for different client
	// behaviour — wait against give up.
	ErrClaimHeld = errors.New("mail: a claim is in flight")
	// ErrClaimTokenWrong reports that a commit or cancel presented a token
	// that is not the one this mailbox holds.
	ErrClaimTokenWrong = errors.New("mail: claim token does not match")
	ErrMailboxFull     = errors.New("mail: mailbox is full")
	ErrRangeInvalid    = errors.New("mail: range is invalid")
	ErrRequestInvalid  = errors.New("mail: request is invalid")
	ErrConflict        = errors.New("mail: conflict")
)

// Bounds. None of them can be bypassed with a zero value.
const (
	// MaxPageSize bounds one list page.
	MaxPageSize = 100
	// DefaultPageSize is used when a caller passes zero. A zero limit means
	// "the default", never "no limit" — that translation is what turned one
	// rank protocol packet into a full-board read.
	DefaultPageSize = 20
	// MaxMailboxEntries bounds how many per-mail states one mailbox holds.
	// Reaching it evicts the oldest terminal entries and counts them, rather
	// than growing without limit.
	MaxMailboxEntries = 200
	// MaxSubjectBytes and MaxBodyBytes bound one envelope.
	MaxSubjectBytes = 256
	MaxBodyBytes    = 4096
	// MaxAttachmentBytes bounds the opaque attachment payload. Its contents
	// are the business repository's business; its size is not.
	MaxAttachmentBytes = 4096
	// MaxRecipients bounds an explicit recipient list, so one send cannot
	// address an unbounded set.
	MaxRecipients = 200
)

// Status is where one mail stands for one player.
//
//	unread ──> read ──┬──> claimed
//	                  └──> deleted
//
// Deleted is terminal and does not erase the claim record: a player who
// deletes a mail after claiming it must not be able to claim it again.
type Status string

const (
	StatusUnread  Status = "unread"
	StatusRead    Status = "read"
	StatusClaimed Status = "claimed"
	StatusDeleted Status = "deleted"
)

func (s Status) terminal() bool { return s == StatusDeleted }

// Audience says who an envelope addresses.
type Audience string

const (
	// AudienceDirect addresses an explicit, bounded recipient list.
	AudienceDirect Audience = "direct"
	// AudienceBroadcast addresses a scope — a server, a guild, everyone.
	// Delivery into individual mailboxes is the caller's decision, expressed
	// through Deliverer: "how does one mail reach a hundred thousand players"
	// is a deployment question, not a library one.
	AudienceBroadcast Audience = "broadcast"
)

// Envelope is one mail's content and addressing. It is written once and never
// rewritten: an edit is a new envelope.
type Envelope struct {
	ID string `json:"id"`
	// Audience and Scope address it. Scope is opaque to this package — a
	// server id, a guild id, whatever the caller's world is keyed by.
	Audience Audience `json:"audience"`
	Scope    string   `json:"scope,omitempty"`
	// Recipients is the explicit list, for AudienceDirect.
	Recipients []int64 `json:"recipients,omitempty"`

	Subject string `json:"subject"`
	Body    string `json:"body,omitempty"`
	// Attachment is opaque. This service decides who may claim it and that it
	// is handed out once; what is inside it is the caller's business.
	Attachment []byte `json:"attachment,omitempty"`

	// SendRequestID is the idempotency key for the send itself, so a retried
	// send does not produce a second envelope.
	SendRequestID string `json:"send_request_id,omitempty"`

	CreatedAtUnix int64 `json:"created_at_unix"`
	// ExpiresAtUnix is when the mail stops being readable and claimable. It
	// must be set: a mail with no expiry is a row that is never removed, and
	// the implementation this replaces had no TTL index and could not add one
	// because its timestamps were int64 milliseconds rather than dates.
	ExpiresAtUnix int64 `json:"expires_at_unix"`
}

// HasAttachment reports whether there is anything to claim.
func (e Envelope) HasAttachment() bool { return len(e.Attachment) > 0 }

// Expired reports whether the envelope has lapsed.
func (e Envelope) Expired(nowUnix int64) bool {
	return e.ExpiresAtUnix > 0 && nowUnix >= e.ExpiresAtUnix
}

// Addresses reports whether this envelope addresses one player in one scope.
//
// This is the authority check, and it takes the caller's identity as an
// argument rather than reading it out of a request struct.
func (e Envelope) Addresses(playerID int64, scope string) bool {
	if e.Audience == AudienceBroadcast {
		return e.Scope == "" || e.Scope == scope
	}
	for _, recipient := range e.Recipients {
		if recipient == playerID {
			return true
		}
	}
	return false
}

func (e Envelope) Validate() error {
	if strings.TrimSpace(e.ID) == "" {
		return fmt.Errorf("%w: id is empty", ErrMailInvalid)
	}
	switch e.Audience {
	case AudienceDirect:
		if len(e.Recipients) == 0 {
			return fmt.Errorf("%w: a direct mail has no recipients", ErrAudienceInvalid)
		}
		if len(e.Recipients) > MaxRecipients {
			return fmt.Errorf("%w: %d recipients, limit %d", ErrAudienceInvalid, len(e.Recipients), MaxRecipients)
		}
		for _, recipient := range e.Recipients {
			if recipient <= 0 {
				return fmt.Errorf("%w: recipient %d is not a player", ErrAudienceInvalid, recipient)
			}
		}
	case AudienceBroadcast:
		if len(e.Recipients) > 0 {
			// A broadcast with a recipient list is two addressing schemes at
			// once, and whichever the reader assumes will be wrong somewhere.
			return fmt.Errorf("%w: a broadcast mail must not carry a recipient list", ErrAudienceInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown audience %q", ErrAudienceInvalid, e.Audience)
	}
	if strings.TrimSpace(e.Subject) == "" {
		return fmt.Errorf("%w: subject is empty", ErrBodyInvalid)
	}
	if len(e.Subject) > MaxSubjectBytes {
		return fmt.Errorf("%w: subject is %d bytes, limit %d", ErrBodyInvalid, len(e.Subject), MaxSubjectBytes)
	}
	if len(e.Body) > MaxBodyBytes {
		return fmt.Errorf("%w: body is %d bytes, limit %d", ErrBodyInvalid, len(e.Body), MaxBodyBytes)
	}
	if len(e.Attachment) > MaxAttachmentBytes {
		return fmt.Errorf("%w: attachment is %d bytes, limit %d", ErrBodyInvalid, len(e.Attachment), MaxAttachmentBytes)
	}
	if e.ExpiresAtUnix <= 0 {
		return fmt.Errorf("%w: a mail must expire", ErrMailInvalid)
	}
	if e.CreatedAtUnix > 0 && e.ExpiresAtUnix <= e.CreatedAtUnix {
		return fmt.Errorf("%w: expiry %d is not after creation %d", ErrMailInvalid, e.ExpiresAtUnix, e.CreatedAtUnix)
	}
	return nil
}

func (e Envelope) clone() Envelope {
	out := e
	out.Recipients = append([]int64(nil), e.Recipients...)
	out.Attachment = append([]byte(nil), e.Attachment...)
	return out
}

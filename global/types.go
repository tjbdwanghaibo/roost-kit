// Package global is the cross-server coordination service: it owns which
// global group a game server belongs to, and the liveness lease each game
// server holds.
//
// The business boundary document of the implementation this replaces already
// wrote down the invariants this package must hold — "route binding migration
// must use epoch CAS, not an in-process lock", "aggregation and participant
// progress must use a CAS store". They were conventions, and the
// implementation did not keep them:
//
//   - Every store exposed both an unconditional SetXxx and a read-modify-write
//     UpdateXxx. The Redis implementation's Update used compare-and-set; the
//     DAO-backed implementation's Update read and then wrote unconditionally.
//     Both satisfied the same interface, so the type system could not tell
//     them apart and whichever was configured decided whether the documented
//     invariant held. Four stores, one shape.
//   - The game lease store had no compare-and-set path at all. Its heartbeat
//     read the record, computed a next version from it, and wrote
//     unconditionally — the version field was decorative. Concurrent
//     heartbeats overwrote each other, and a late heartbeat from a previous
//     game incarnation could overwrite an active lease with its own start
//     time and load, because nothing checked that the writer was still the
//     holder.
//
// Here the invariants are held by the types. State goes through
// versionstore, whose contract has no unconditional write, so a non-CAS
// implementation cannot exist. And the lease carries an incarnation token that
// a renewal must present, so a writer that is no longer the holder is refused
// rather than silently winning.
package global

import (
	"errors"
	"fmt"
	"strings"
)

// Error codes.
const (
	CodeOK int32 = 0

	CodeRouteInvalid   int32 = 570101
	CodeRouteMissing   int32 = 570102
	CodeRouteStale     int32 = 570103
	CodeRouteMigrating int32 = 570104
	CodeLeaseInvalid   int32 = 570105
	CodeLeaseMissing   int32 = 570106
	CodeLeaseNotHolder int32 = 570107
	CodeLeaseExpired   int32 = 570108
	CodeRangeInvalid   int32 = 570109
	CodeConflict       int32 = 570110
	CodeStoreFailed    int32 = 570111
)

var (
	ErrRouteInvalid = errors.New("global: route is invalid")
	ErrRouteMissing = errors.New("global: route binding not found")
	// ErrRouteStale reports that a rebind presented an epoch that is no
	// longer current. It is the epoch CAS the boundary document required and
	// the implementation enforced with an in-process lock, which is no
	// guarantee across instances.
	ErrRouteStale     = errors.New("global: route epoch is stale")
	ErrRouteMigrating = errors.New("global: route binding is migrating")

	ErrLeaseInvalid = errors.New("global: lease is invalid")
	ErrLeaseMissing = errors.New("global: lease not found")
	// ErrLeaseNotHolder reports that the caller presented an incarnation
	// token that is not the current holder's. This is the check whose absence
	// let a previous incarnation's late heartbeat overwrite a live lease.
	ErrLeaseNotHolder = errors.New("global: caller does not hold this lease")
	ErrLeaseExpired   = errors.New("global: lease has expired")

	ErrRangeInvalid = errors.New("global: range is invalid")
	ErrConflict     = errors.New("global: conflict")
)

// MaxPageSize bounds a listing, and cannot be bypassed with a zero limit.
const MaxPageSize = 200

// MaxLoadEntries bounds the load snapshot a game server may report, so a
// heartbeat cannot grow without limit.
const MaxLoadEntries = 32

// RouteState is where a binding is in its lifecycle.
//
//	active ──> migrating ──> active (at a new global sid)
//
// Migration is two-step on purpose: a binding that is moving is visible as
// moving, so a caller resolving it can wait rather than reach a server that
// is handing over.
type RouteState string

const (
	RouteActive    RouteState = "active"
	RouteMigrating RouteState = "migrating"
)

// RouteBinding says which global group and instance serve one game server.
type RouteBinding struct {
	// GameSID identifies the game server.
	GameSID int32 `json:"game_sid"`
	// GlobalGroupID is the coordination group it belongs to.
	GlobalGroupID string `json:"global_group_id"`
	// GlobalSID is the instance currently serving it.
	GlobalSID int32 `json:"global_sid"`
	// TargetGlobalSID is where it is moving, while State is migrating.
	TargetGlobalSID int32 `json:"target_global_sid,omitempty"`
	// Epoch increments on every accepted change. A rebind must present the
	// epoch it read, which is what makes concurrent migrations safe without a
	// lock — and a lock across instances is exactly what the boundary
	// document forbade.
	Epoch uint64     `json:"epoch"`
	State RouteState `json:"state"`

	UpdatedAtUnix int64 `json:"updated_at_unix"`
}

func (b RouteBinding) Validate() error {
	if b.GameSID <= 0 {
		return fmt.Errorf("%w: game sid must be positive", ErrRouteInvalid)
	}
	if strings.TrimSpace(b.GlobalGroupID) == "" {
		return fmt.Errorf("%w: global group id is empty", ErrRouteInvalid)
	}
	if b.GlobalSID <= 0 {
		return fmt.Errorf("%w: global sid must be positive", ErrRouteInvalid)
	}
	return nil
}

// LeaseState is whether a lease is held.
type LeaseState string

const (
	LeaseActive   LeaseState = "active"
	LeaseReleased LeaseState = "released"
	// LeaseLapsed is a lease whose deadline passed without a heartbeat. It is
	// a distinct state from released on purpose: "the holder gave it up" and
	// "the holder stopped answering" call for different operational
	// responses, and collapsing them loses the only signal that a game server
	// died rather than shut down cleanly.
	//
	// It is never stored — a lapsed lease is stored as active with an elapsed
	// deadline — so a reader computes it. That is what lets a reader be
	// correct before anything sweeps.
	LeaseLapsed LeaseState = "lapsed"
)

// GameLease is one game server's liveness and load snapshot within its global
// group.
//
// It says nothing about player state and does not replace service discovery —
// it is how the coordination group knows a game server is alive and how loaded
// it is.
type GameLease struct {
	GameSID int32 `json:"game_sid"`
	// Incarnation identifies one run of that game server. A renewal must
	// present it. Without it, a late heartbeat from a process that has since
	// been replaced overwrites the live lease — the confirmed defect in the
	// implementation this replaces.
	Incarnation string `json:"incarnation"`
	// GlobalGroupID and GlobalSID are the binding this lease was taken under.
	// A renewal against a different binding is refused, so a lease cannot
	// outlive the routing decision that created it.
	GlobalGroupID string     `json:"global_group_id"`
	GlobalSID     int32      `json:"global_sid"`
	RouteEpoch    uint64     `json:"route_epoch"`
	State         LeaseState `json:"state"`

	// Load is an opaque snapshot the game server reports, bounded by
	// MaxLoadEntries.
	Load map[string]string `json:"load,omitempty"`

	StartedAtUnix       int64 `json:"started_at_unix"`
	LastHeartbeatAtUnix int64 `json:"last_heartbeat_at_unix"`
	ExpiresAtUnix       int64 `json:"expires_at_unix"`
}

// Expired reports whether an active lease has lapsed.
func (l GameLease) Expired(nowUnix int64) bool {
	if l.State != LeaseActive || l.ExpiresAtUnix == 0 {
		return false
	}
	return nowUnix >= l.ExpiresAtUnix
}

// Held reports whether the lease is currently held by the given incarnation.
func (l GameLease) Held(incarnation string, nowUnix int64) bool {
	return l.State == LeaseActive && l.Incarnation == incarnation && !l.Expired(nowUnix)
}

func validateLoad(load map[string]string) error {
	if len(load) > MaxLoadEntries {
		return fmt.Errorf("%w: load has %d entries, limit %d", ErrLeaseInvalid, len(load), MaxLoadEntries)
	}
	return nil
}

func cloneLoad(load map[string]string) map[string]string {
	if len(load) == 0 {
		return nil
	}
	out := make(map[string]string, len(load))
	for key, value := range load {
		out[key] = value
	}
	return out
}

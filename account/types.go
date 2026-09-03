// Package account is the account and role directory: it maps a channel
// identity to an account, owns the roles under it, and issues role session
// tokens.
//
// This is a rewrite. The implementation it replaces had, among others, these
// defects — and each one is why something here looks the way it does:
//
//   - Login verified nothing. It accepted {channel, open_id} and returned the
//     account, and selecting a role then returned a valid session token for
//     any player under it. The only gate was one repo-wide shared secret
//     whose signature covered the body with no timestamp or nonce, so a
//     captured request replayed forever — and in a non-production
//     configuration with no secret set the gate was off entirely. That is
//     full account takeover. Here identity verification is a required
//     interface with **no default**: a service that cannot verify does not
//     start.
//   - The default player-id allocator was a per-process counter starting at
//     one, selected whenever Redis was absent but Mongo was not, and role
//     creation wrote with a replace that matched and overwrote an existing
//     document. Restart the service, create a role, and an existing player's
//     record was gone. Here there is no default allocator, and creation is
//     insert-only.
//   - The per-server role limit was a read-count-write with a deliberately
//     non-unique index behind it, so two concurrent creates both passed.
//     Here it is an exclusive claim in the directory primitive.
//   - A separate upsert entry point bypassed name reservation, account
//     existence and the role limit, and would reparent a role to another
//     account or rename it while orphaning the old reservation. There is no
//     such entry point here; each mutation is a narrow, constrained operation.
//   - Name reservation and the role record were two collection writes with no
//     transaction, and the compensating release dropped its error, so a crash
//     between them burned the name permanently. Here reservation is the
//     two-phase claim in the directory package: an uncommitted claim expires.
//   - Redis role updates compared a re-marshalled value against stored bytes,
//     so adding one field wedged every role write across a rolling deploy.
//     Here state goes through versionstore, which compares versions.
package account

import (
	"errors"
	"fmt"
	"strings"
)

// Error codes.
//
// Written out rather than derived with iota: in the implementation this
// replaces the error code evaluated to 2 because CodeOK occupied the first
// const spec, while every decode failure returned 1 — a value no constant
// named. A client matching on the error code never matched.
const (
	CodeOK int32 = 0

	CodeIdentityInvalid int32 = 560101
	CodeIdentityDenied  int32 = 560102
	CodeAccountMissing  int32 = 560103
	CodeServerInvalid   int32 = 560104
	CodeServerClosed    int32 = 560105
	CodeNameInvalid     int32 = 560106
	CodeNameTaken       int32 = 560107
	CodeRoleLimit       int32 = 560108
	CodeRoleMissing     int32 = 560109
	CodeNotPermitted    int32 = 560110
	CodeSessionInvalid  int32 = 560111
	CodeRangeInvalid    int32 = 560112
	CodeConflict        int32 = 560113
	CodeStoreFailed     int32 = 560114
)

var (
	ErrIdentityInvalid = errors.New("account: identity is invalid")
	// ErrIdentityDenied reports that the channel refused the credential. It
	// is distinct from ErrIdentityInvalid so a malformed request and a
	// rejected one are not the same event to an operator.
	ErrIdentityDenied = errors.New("account: identity was denied by the channel")
	ErrAccountMissing = errors.New("account: account not found")
	ErrServerInvalid  = errors.New("account: server is invalid")
	ErrServerClosed   = errors.New("account: server is not open")
	ErrNameInvalid    = errors.New("account: role name is invalid")
	ErrNameTaken      = errors.New("account: role name is taken")
	ErrRoleLimit      = errors.New("account: role limit reached for this server")
	ErrRoleMissing    = errors.New("account: role not found")
	ErrNotPermitted   = errors.New("account: caller does not own this role")
	ErrSessionInvalid = errors.New("account: session token is invalid")
	ErrRangeInvalid   = errors.New("account: range is invalid")
	ErrConflict       = errors.New("account: conflict")
)

// Bounds. Every listing is bounded, and no bound can be bypassed by leaving a
// limit at zero.
const (
	MaxPageSize = 100
	// MaxProfileBytes bounds the opaque per-role profile. Progression fields
	// are game concepts, so they are carried as an opaque blob rather than
	// baked into this schema the way level, power, alliance and avatar were.
	MaxProfileBytes = 4096
)

// Channel names an identity provider: a store, a platform, a self-hosted
// login. It is opaque here.
type Channel string

// Identity is a credential presented for login.
type Identity struct {
	Channel Channel `json:"channel"`
	// OpenID is the channel's identifier for the user.
	OpenID string `json:"open_id"`
	// Credential is what the channel verifies — a platform token, a receipt,
	// a signature. This service never interprets it; it hands it to the
	// IdentityVerifier. It is never logged and never stored.
	Credential string `json:"credential"`
}

func (i Identity) Validate() error {
	if strings.TrimSpace(string(i.Channel)) == "" {
		return fmt.Errorf("%w: channel is empty", ErrIdentityInvalid)
	}
	if strings.TrimSpace(i.OpenID) == "" {
		return fmt.Errorf("%w: open id is empty", ErrIdentityInvalid)
	}
	return nil
}

// AccountID is the stable account key, derived from the verified identity.
// Derivation is deterministic so the same verified identity always maps to the
// same account without a lookup table.
func (i Identity) AccountID() string {
	return strings.ToLower(strings.TrimSpace(string(i.Channel))) + ":" + strings.TrimSpace(i.OpenID)
}

// Account is one player's account across servers.
type Account struct {
	ID      string  `json:"id"`
	Channel Channel `json:"channel"`
	OpenID  string  `json:"open_id"`
	// Banned blocks login and role creation. It is here rather than in a
	// separate service because login is the only place it can be enforced
	// cheaply.
	Banned          bool  `json:"banned"`
	CreatedAtUnix   int64 `json:"created_at_unix"`
	LastLoginAtUnix int64 `json:"last_login_at_unix"`
}

// ServerStatus says whether a server accepts new roles.
type ServerStatus string

const (
	ServerOpen        ServerStatus = "open"
	ServerMaintenance ServerStatus = "maintenance"
	ServerFull        ServerStatus = "full"
	ServerClosed      ServerStatus = "closed"
)

// acceptsNewRoles reports whether a role may be created on this server.
func (s ServerStatus) acceptsNewRoles() bool { return s == ServerOpen }

// Server is one game server a role can live on.
type Server struct {
	ID     int32        `json:"id"`
	Name   string       `json:"name"`
	Status ServerStatus `json:"status"`
	// Region groups servers for display; opaque here.
	Region        string `json:"region"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
}

// Role is one character under an account on one server.
type Role struct {
	// PlayerID is allocated by the injected PlayerIDAllocator, never by this
	// service. It is the role's identity everywhere else in the system.
	PlayerID  int64  `json:"player_id"`
	AccountID string `json:"account_id"`
	ServerID  int32  `json:"server_id"`
	// Name is the display name; uniqueness is enforced on its normalized form.
	Name string `json:"name"`
	// Profile is opaque game state — level, power, guild, avatar. It is a
	// blob because those are gameplay concepts: baking them into this schema
	// is what made the directory carry one game's progression model.
	Profile          []byte `json:"profile,omitempty"`
	CreatedAtUnix    int64  `json:"created_at_unix"`
	LastLoginAtUnix  int64  `json:"last_login_at_unix"`
	LastLogoutAtUnix int64  `json:"last_logout_at_unix"`
}

// Session is what a verified role selection produces.
type Session struct {
	PlayerID      int64  `json:"player_id"`
	AccountID     string `json:"account_id"`
	ServerID      int32  `json:"server_id"`
	Token         string `json:"token"`
	ExpiresAtUnix int64  `json:"expires_at_unix"`
}

// Slot records that an account occupies its role allowance on one server.
//
// It exists as its own record — rather than being derived by counting roles —
// because the limit has to be enforced by a single atomic claim. Counting is
// what let two concurrent creates both read zero and both insert.
type Slot struct {
	AccountID string `json:"account_id"`
	ServerID  int32  `json:"server_id"`
	// PlayerID is filled once the role exists. It is zero for the window
	// between claiming the slot and inserting the role, which is what makes
	// an abandoned claim identifiable.
	PlayerID int64 `json:"player_id,omitempty"`
}

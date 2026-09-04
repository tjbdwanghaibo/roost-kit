// Package servicemods holds the app.ModName constants for every service in
// this repository, and the small helpers their Mods share.
//
// The names live in one table rather than one per package for the same reason
// roost-kit keeps its own: a capability name is a global key in the registry,
// so a collision is a runtime failure that two separately-correct packages can
// cause together. One table is where a collision is visible by reading.
//
// The names are prefixed "service." so they cannot collide with roost-kit's
// infrastructure capabilities — a service named "chat" and a hypothetical
// kit transport named "chat" would otherwise be the same key.
package servicemods

import "github.com/tjbdwanghaibo/roost-core/app"

// Capability names for the services in this repository.
const (
	ModAccount   app.ModName = "service.account"
	ModChat      app.ModName = "service.chat"
	ModDirectory app.ModName = "service.directory"
	ModGlobal    app.ModName = "service.global"
	// ModGlobalActivity is a separate capability from ModGlobal because the
	// two halves share no state — routing and leases answer "where does this
	// game belong and is it alive", activity coordination answers "has every
	// game reached the phase yet". A deployment may want one and not the
	// other, and joining them would give the activity half a reason to reach
	// into the lease store.
	ModGlobalActivity app.ModName = "service.global.activity"
	ModMail           app.ModName = "service.mail"
	ModMatch          app.ModName = "service.match"
	ModPlatform       app.ModName = "service.platform"
	ModRank           app.ModName = "service.rank"
	ModSession        app.ModName = "service.session"
)

// All is every capability name this repository can register.
//
// It exists so a test can assert that the table has no duplicates — the one
// failure a name table is supposed to prevent, and the one that a table
// nobody checks does not prevent at all.
var All = []app.ModName{
	ModAccount, ModChat, ModDirectory, ModGlobal, ModGlobalActivity,
	ModMail, ModMatch, ModPlatform, ModRank, ModSession,
}

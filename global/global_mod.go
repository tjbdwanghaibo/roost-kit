package global

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/tjbdwanghaibo/roost-core/app"
	"github.com/tjbdwanghaibo/roost-kit/mods"

	"github.com/tjbdwanghaibo/roost-service/servicemetrics"
	"github.com/tjbdwanghaibo/roost-service/servicemods"
)

// Mod wires the two global services into an app and registers them as TWO
// capabilities.
//
// Two rather than one because the halves share no state: routing and leases
// answer "where does this game belong and is it alive", activity coordination
// answers "has every game reached the phase yet". A deployment may want one
// and not the other, and a single capability would give the activity half a
// reason to reach into the lease store — which is how two bounded services
// become one unbounded one.
//
// The stores are built once and shared, because they are namespaced under one
// prefix and building them twice would mean two prefixes to keep in step.
type Mod struct {
	metrics servicemetrics.Reporter

	prefix           string
	leaseTTL         time.Duration
	reservationTTL   time.Duration
	graceWindow      time.Duration
	dispatchAttempts int
	dispatchBackoff  time.Duration

	service  *Service
	activity *ActivityService
}

// NewMod returns a global Mod. It needs no policy collaborator: every decision
// here is a compare-and-set against state, not a judgement about a caller.
func NewMod(reporter servicemetrics.Reporter) *Mod {
	return &Mod{metrics: reporter}
}

// Name implements app.Mod.
func (m *Mod) Name() app.ModName { return servicemods.ModGlobal }

// DependsOn implements app.ModDependencyProvider.
func (m *Mod) DependsOn() []app.ModName { return []app.ModName{mods.ModRedis} }

// Init reads configuration.
//
//	global:
//	  key_prefix: roost:global   # required, no default
//	  lease_ttl: 30s             # optional
//	  reservation_ttl: 30m       # required; see below
//	  grace_window: 60s          # optional
//	  dispatch_attempts: 5       # optional
//	  dispatch_backoff: 5s       # optional
func (m *Mod) Init(cfg *viper.Viper) error {
	prefix, err := servicemods.KeyPrefix(cfg, "global")
	if err != nil {
		return err
	}
	leaseTTL, err := servicemods.Duration(cfg, "global.lease_ttl", DefaultLeaseTTL)
	if err != nil {
		return err
	}
	// Required, with no default. It must exceed the longest client retry
	// horizon: past it a replayed progress request is indistinguishable from a
	// new one and the progress is applied twice. That horizon belongs to the
	// caller's transport.
	reservationTTL, err := servicemods.RequiredDuration(cfg, "global.reservation_ttl")
	if err != nil {
		return err
	}
	graceWindow, err := servicemods.Duration(cfg, "global.grace_window", DefaultGraceWindow)
	if err != nil {
		return err
	}
	dispatchBackoff, err := servicemods.Duration(cfg, "global.dispatch_backoff", DefaultDispatchBackoff)
	if err != nil {
		return err
	}
	dispatchAttempts := DefaultDispatchAttempts
	if cfg.IsSet("global.dispatch_attempts") {
		dispatchAttempts = cfg.GetInt("global.dispatch_attempts")
		if dispatchAttempts <= 0 {
			return fmt.Errorf("global mod: global.dispatch_attempts must be positive, got %d", dispatchAttempts)
		}
	}
	m.prefix, m.leaseTTL, m.reservationTTL = prefix, leaseTTL, reservationTTL
	m.graceWindow, m.dispatchAttempts, m.dispatchBackoff = graceWindow, dispatchAttempts, dispatchBackoff
	return nil
}

// Provide builds both services and registers both capabilities.
func (m *Mod) Provide(r *app.Registry) error {
	client, err := servicemods.Redis(r)
	if err != nil {
		return err
	}
	stores, err := NewRedisStores(client, m.prefix, m.reservationTTL)
	if err != nil {
		return fmt.Errorf("global mod: %w", err)
	}
	service, err := New(Config{
		Routes: stores.Routes, Leases: stores.Leases,
		LeaseTTL: m.leaseTTL, Metrics: m.metrics,
	})
	if err != nil {
		return fmt.Errorf("global mod: routing: %w", err)
	}
	activity, err := NewActivityService(ActivityConfig{
		Activities: stores.Activities, Participants: stores.Participants,
		Ledger: stores.Ledger, Audits: stores.Audits,
		Dispatches: stores.Dispatches, Windows: stores.Windows,
		GraceWindow: m.graceWindow, ReservationTTL: m.reservationTTL,
		DispatchBackoff: m.dispatchBackoff, DispatchMaxAttempts: m.dispatchAttempts,
		Metrics: m.metrics,
	})
	if err != nil {
		return fmt.Errorf("global mod: activity: %w", err)
	}
	m.service, m.activity = service, activity
	// Registered as one batch, so a name collision is caught before either is
	// published rather than leaving the registry half-updated.
	return mods.RegisterAll(r,
		mods.Capability{Name: servicemods.ModGlobal, Value: service},
		mods.Capability{Name: servicemods.ModGlobalActivity, Value: activity},
	)
}

// Start implements app.Mod.
//
// Nothing is started. Lapsed leases and expired activities are resolved by the
// callers' sweeps — the cadence is a deployment decision, and a goroutine a
// Mod starts silently is one nobody can see failing.
func (m *Mod) Start() error { return nil }

// Stop implements app.Mod. Nothing to stop.
func (m *Mod) Stop() {}

var _ app.Mod = (*Mod)(nil)

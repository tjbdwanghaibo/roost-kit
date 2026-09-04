package match

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/tjbdwanghaibo/roost-core/app"
	"github.com/tjbdwanghaibo/roost-kit/mods"

	"github.com/tjbdwanghaibo/roost-service/servicemetrics"
	"github.com/tjbdwanghaibo/roost-service/servicemods"
)

// Mod wires a match Store into an app and registers it as a capability.
//
// Grouping is a constructor argument because "which candidates form a match"
// is the whole of a game's matchmaking policy. A nil Grouping selects
// FirstComeGrouping, which is a real strategy rather than a stand-in, so the
// default is honest.
type Mod struct {
	grouping Grouping
	metrics  servicemetrics.Reporter

	prefix    string
	ticketTTL time.Duration
	store     Store
}

// NewMod returns a match Mod. grouping may be nil for FirstComeGrouping.
func NewMod(grouping Grouping, reporter servicemetrics.Reporter) *Mod {
	return &Mod{grouping: grouping, metrics: reporter}
}

// Name implements app.Mod.
func (m *Mod) Name() app.ModName { return servicemods.ModMatch }

// DependsOn implements app.ModDependencyProvider.
func (m *Mod) DependsOn() []app.ModName { return []app.ModName{mods.ModRedis} }

// Init reads configuration.
//
//	match:
//	  key_prefix: roost:match   # required, no default
//	  ticket_ttl: 2m            # optional, defaults to DefaultTicketTTL
func (m *Mod) Init(cfg *viper.Viper) error {
	prefix, err := servicemods.KeyPrefix(cfg, "match")
	if err != nil {
		return err
	}
	ttl, err := servicemods.Duration(cfg, "match.ticket_ttl", DefaultTicketTTL)
	if err != nil {
		return err
	}
	m.prefix, m.ticketTTL = prefix, ttl
	return nil
}

// Provide builds the store and registers it.
func (m *Mod) Provide(r *app.Registry) error {
	client, err := servicemods.Redis(r)
	if err != nil {
		return err
	}
	store, err := NewRedisStore(client, m.prefix, Config{
		TicketTTL: m.ticketTTL, Grouping: m.grouping, Metrics: m.metrics,
	})
	if err != nil {
		return fmt.Errorf("match mod: %w", err)
	}
	m.store = store
	return mods.RegisterAll(r, mods.Capability{Name: servicemods.ModMatch, Value: store})
}

// Start implements app.Mod.
//
// Nothing is started here, and that is worth being explicit about: expired
// tickets are resolved by Sweep, which the caller drives. This package does
// not own a ticker, because "how often does this deployment sweep" is a
// deployment decision — and because a background goroutine that a Mod starts
// silently is one nobody can see failing.
func (m *Mod) Start() error { return nil }

// Stop implements app.Mod. Nothing to stop, for the same reason.
func (m *Mod) Stop() {}

var _ app.Mod = (*Mod)(nil)

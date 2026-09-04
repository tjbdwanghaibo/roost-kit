package mail

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/tjbdwanghaibo/roost-core/app"
	"github.com/tjbdwanghaibo/roost-kit/mods"

	"github.com/tjbdwanghaibo/roost-service/servicemetrics"
	"github.com/tjbdwanghaibo/roost-service/servicemods"
)

// Mod wires a mail Service into an app and registers it as a capability.
//
// The broadcast Deliverer is optional, and it is the one optional collaborator
// in this repository that is safe to leave out — because leaving it out does
// not weaken anything: Send refuses AudienceBroadcast outright when it is
// absent, rather than accepting the send and delivering to nobody. A
// deployment that only sends direct mail therefore needs no fanout, and one
// that forgot to wire it up finds out on its first broadcast instead of
// discovering later that a month of announcements went nowhere.
type Mod struct {
	broadcast Deliverer
	metrics   servicemetrics.Reporter

	prefix     string
	sendTTL    time.Duration
	claimLease time.Duration
	service    *Service
}

// NewMod returns a mail Mod. broadcast may be nil; see the type comment.
func NewMod(broadcast Deliverer, reporter servicemetrics.Reporter) *Mod {
	return &Mod{broadcast: broadcast, metrics: reporter}
}

// Name implements app.Mod.
func (m *Mod) Name() app.ModName { return servicemods.ModMail }

// DependsOn implements app.ModDependencyProvider.
func (m *Mod) DependsOn() []app.ModName { return []app.ModName{mods.ModRedis} }

// Init reads configuration.
//
//	mail:
//	  key_prefix: roost:mail   # required, no default
//	  send_ttl: 24h            # required; see below
//	  claim_lease: 30s         # optional, defaults to DefaultClaimLease
func (m *Mod) Init(cfg *viper.Viper) error {
	prefix, err := servicemods.KeyPrefix(cfg, "mail")
	if err != nil {
		return err
	}
	// Required, with no default. It must exceed the longest client retry
	// horizon: past it a retried send is indistinguishable from a new one and
	// the recipient gets the mail twice. That horizon is a property of the
	// caller's transport, so this package cannot pick it.
	sendTTL, err := servicemods.RequiredDuration(cfg, "mail.send_ttl")
	if err != nil {
		return err
	}
	claimLease, err := servicemods.Duration(cfg, "mail.claim_lease", DefaultClaimLease)
	if err != nil {
		return err
	}
	m.prefix, m.sendTTL, m.claimLease = prefix, sendTTL, claimLease
	return nil
}

// Provide builds the service and registers it.
func (m *Mod) Provide(r *app.Registry) error {
	client, err := servicemods.Redis(r)
	if err != nil {
		return err
	}
	// The clock is passed to both halves from one place, so the envelope
	// store's key ttls and the service's expiry comparisons cannot disagree
	// about what time it is. They did once, and the result was a mail that
	// read as live and had already been evicted.
	now := time.Now
	stores, err := NewRedisStores(client, RedisConfig{
		Prefix: m.prefix, SendTTL: m.sendTTL, Now: now,
	})
	if err != nil {
		return fmt.Errorf("mail mod: %w", err)
	}
	service, err := New(Config{
		Envelopes: stores.Envelopes, Mailboxes: stores.Mailboxes, Sends: stores.Sends,
		Broadcast: m.broadcast, ClaimLease: m.claimLease, Now: now, Metrics: m.metrics,
	})
	if err != nil {
		return fmt.Errorf("mail mod: %w", err)
	}
	m.service = service
	return mods.RegisterAll(r, mods.Capability{Name: servicemods.ModMail, Value: service})
}

// Start implements app.Mod. Nothing to start.
func (m *Mod) Start() error { return nil }

// Stop implements app.Mod. Nothing to stop.
func (m *Mod) Stop() {}

var _ app.Mod = (*Mod)(nil)

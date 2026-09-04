package chat

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
	"github.com/tjbdwanghaibo/roost-core/app"
	"github.com/tjbdwanghaibo/roost-kit/mods"

	"github.com/tjbdwanghaibo/roost-service/servicemods"
)

// Mod wires a chat Service into an app and registers it as a capability.
//
// Policy, Bodies and System are required constructor arguments with no
// defaults. The first is the one that matters: a permissive default policy is
// precisely what the client-settable Trusted bool amounted to in the
// implementation this replaces — a way for a caller to be treated as
// privileged without anything having decided that it was.
//
// System turns a transport-level identity into a SystemToken. It has no
// default because minting that token IS the privilege: a default that granted
// it would put back the field this package removed.
type Mod struct {
	policy ChannelPolicy
	bodies *BodyRegistry
	system SystemAuthenticator
	rules  []ChannelRule

	metrics Metrics

	prefix       string
	retentionAge time.Duration
	service      *Service
}

// NewMod returns a chat Mod. policy, bodies and system are required; rules
// may be empty to use DefaultChannelRules.
func NewMod(
	policy ChannelPolicy,
	bodies *BodyRegistry,
	system SystemAuthenticator,
	rules []ChannelRule,
	reporter Metrics,
) *Mod {
	return &Mod{
		policy: policy, bodies: bodies, system: system,
		rules: append([]ChannelRule(nil), rules...), metrics: reporter,
	}
}

// Name implements app.Mod.
func (m *Mod) Name() app.ModName { return CapabilityName }

// DependsOn implements app.ModDependencyProvider.
func (m *Mod) DependsOn() []app.ModName { return []app.ModName{mods.ModRedis} }

// Init reads configuration.
//
//	chat:
//	  key_prefix: roost:chat   # required, no default
//	  retention_age: 168h      # optional; zero means retain by count only
func (m *Mod) Init(cfg *viper.Viper) error {
	missing := []string{}
	if m.policy == nil {
		missing = append(missing, "channel policy")
	}
	if m.bodies == nil {
		missing = append(missing, "body registry")
	}
	if m.system == nil {
		missing = append(missing, "system authenticator")
	}
	if len(missing) > 0 {
		return fmt.Errorf("chat mod: %v are required and have no defaults; a permissive policy "+
			"is what the client-settable Trusted bool amounted to", missing)
	}
	prefix, err := servicemods.KeyPrefix(cfg, "chat")
	if err != nil {
		return err
	}
	retention, err := servicemods.Duration(cfg, "chat.retention_age", 0)
	if err != nil {
		return err
	}
	m.prefix, m.retentionAge = prefix, retention
	return nil
}

// Provide builds the service and registers it.
func (m *Mod) Provide(r *app.Registry) error {
	client, err := servicemods.Redis(r)
	if err != nil {
		return err
	}
	store, err := NewRedisStore(client, m.prefix, Config{
		Policy: m.policy, Bodies: m.bodies, Rules: m.rules,
		RetentionAge: m.retentionAge, Metrics: m.metrics,
	})
	if err != nil {
		return fmt.Errorf("chat mod: %w", err)
	}
	service, err := NewService(ServiceConfig{Store: store, System: m.system})
	if err != nil {
		return fmt.Errorf("chat mod: %w", err)
	}
	m.service = service
	// Two capabilities, from one generated call so they cannot be published
	// apart: the interface consumers look up, and the owner-only name the
	// Server looks up to know this process holds the implementation.
	return mods.RegisterAll(r, OwnerCapabilities(service)...)
}

// Start implements app.Mod. Nothing to start.
func (m *Mod) Start() error { return nil }

// Stop implements app.Mod. Nothing to stop.
func (m *Mod) Stop() {}

var _ app.Mod = (*Mod)(nil)

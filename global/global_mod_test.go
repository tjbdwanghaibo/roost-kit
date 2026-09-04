package global

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/tjbdwanghaibo/roost-core/app"
	"github.com/tjbdwanghaibo/roost-kit/mods"

	"github.com/tjbdwanghaibo/roost-service/servicemods"
)

func modConfig() *viper.Viper {
	cfg := viper.New()
	cfg.Set("global.key_prefix", "roost:global")
	cfg.Set("global.reservation_ttl", 30*time.Minute)
	return cfg
}

// The reservation ttl has no default. It must exceed the longest client retry
// horizon: past it a replayed progress request is indistinguishable from a new
// one and the progress is applied twice.
func TestModRequiresAReservationTTL(t *testing.T) {
	cfg := viper.New()
	cfg.Set("global.key_prefix", "roost:global")
	if err := NewMod(nil).Init(cfg); err == nil {
		t.Fatal("Init accepted a missing reservation ttl")
	}
	cfg.Set("global.reservation_ttl", time.Duration(0))
	if err := NewMod(nil).Init(cfg); err == nil {
		t.Fatal("Init accepted a zero reservation ttl")
	}
}

func TestModRefusesANonPositiveDispatchBudget(t *testing.T) {
	cfg := modConfig()
	cfg.Set("global.dispatch_attempts", 0)
	if err := NewMod(nil).Init(cfg); err == nil {
		t.Fatal("Init accepted a zero dispatch budget; an unbounded retry queue never drains")
	}
}

func TestModInitAndProvideContract(t *testing.T) {
	mod := NewMod(nil)
	if err := mod.Init(modConfig()); err != nil {
		t.Fatal(err)
	}
	if got := mod.Name(); got != servicemods.ModGlobal {
		t.Fatalf("the mod is named %q", got)
	}
	var asAny any = mod
	provider, ok := asAny.(app.ModDependencyProvider)
	if !ok {
		t.Fatal("the Mod does not declare its dependencies")
	}
	if len(provider.DependsOn()) == 0 || provider.DependsOn()[0] != mods.ModRedis {
		t.Fatalf("DependsOn = %v, want redis", provider.DependsOn())
	}
	if err := mod.Provide(app.NewRegistry(viper.New())); err == nil {
		t.Fatal("Provide succeeded with no Redis capability")
	} else if !strings.Contains(err.Error(), string(mods.ModRedis)) {
		t.Fatalf("the error does not name the missing capability: %v", err)
	}
	mod.Stop()
	if err := mod.Start(); err != nil {
		t.Fatalf("Start returned %v", err)
	}
}

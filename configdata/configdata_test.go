package configdata

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/tjbdwanghaibo/cube-core/app"
	fconfigdata "github.com/tjbdwanghaibo/cube-core/configdata"
	"github.com/tjbdwanghaibo/cube-kit/mods"
)

func newProvidedConfigDataMod(t *testing.T, cfg *viper.Viper) (*Mod, *app.Registry) {
	t.Helper()
	mod := NewConfigDataMod()
	if err := mod.Init(cfg); err != nil {
		t.Fatal(err)
	}
	// app.NewRegistry pre-registers the platform capabilities (obs,
	// lifecycle) that Provide looks up.
	registry := app.NewRegistry(viper.New())
	if err := mod.Provide(registry); err != nil {
		t.Fatal(err)
	}
	return mod, registry
}

func TestConfigDataModInitHonorsConfiguredDir(t *testing.T) {
	cfg := viper.New()
	cfg.Set(cfgKeyDir, "custom/data")
	mod := NewConfigDataMod()
	if err := mod.Init(cfg); err != nil {
		t.Fatal(err)
	}
	if mod.dir != "custom/data" {
		t.Fatalf("dir=%q", mod.dir)
	}
	if mod.Store() == nil {
		t.Fatal("Init must create the store")
	}

	fallback := NewConfigDataMod()
	if err := fallback.Init(viper.New()); err != nil {
		t.Fatal(err)
	}
	if fallback.dir != "configs/data" {
		t.Fatalf("default dir=%q", fallback.dir)
	}
}

func TestConfigDataModProvidesStoreAndStopUnregistersHooks(t *testing.T) {
	cfg := viper.New()
	cfg.Set(cfgKeyDir, t.TempDir()) // empty directory: a valid, empty dataset
	mod, registry := newProvidedConfigDataMod(t, cfg)

	store, ok := app.Lookup[*fconfigdata.Store](registry, mods.ModConfigData)
	if !ok || store == nil || store != mod.Store() {
		t.Fatal("store capability not registered")
	}
	if err := mod.Start(); err != nil {
		t.Fatalf("Start on an empty data dir: %v", err)
	}
	if len(mod.unregisters) == 0 {
		t.Fatal("Provide must register the metrics reload hook")
	}
	mod.Stop()
	if mod.unregisters != nil {
		t.Fatal("Stop must release the reload listeners")
	}
}

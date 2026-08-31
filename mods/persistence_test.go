package mods

import (
	"errors"
	"testing"

	"github.com/spf13/viper"
)

func TestResolvePersistenceEngineDefaultsToCheckpointAndSelectsExactlyOneWriter(t *testing.T) {
	defaults, err := ResolvePersistenceEngine(viper.New())
	if err != nil {
		t.Fatal(err)
	}
	if defaults.Engine != PersistenceCheckpoint || !defaults.CheckpointEnabled || defaults.DataEngineEnabled {
		t.Fatalf("defaults=%+v", defaults)
	}
	cfg := viper.New()
	cfg.Set("persistence.engine", "dataengine")
	selected, err := ResolvePersistenceEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Engine != PersistenceDataEngine || selected.CheckpointEnabled || !selected.DataEngineEnabled {
		t.Fatalf("selected=%+v", selected)
	}
}

func TestResolvePersistenceEngineRejectsDoubleWriteAndNoWriter(t *testing.T) {
	for _, configure := range []func(*viper.Viper){
		func(cfg *viper.Viper) { cfg.Set("checkpoint.enabled", true); cfg.Set("dataengine.enabled", true) },
		func(cfg *viper.Viper) { cfg.Set("checkpoint.enabled", false); cfg.Set("dataengine.enabled", false) },
	} {
		cfg := viper.New()
		configure(cfg)
		if _, err := ResolvePersistenceEngine(cfg); !errors.Is(err, ErrPersistenceEngineSelection) {
			t.Fatalf("err=%v", err)
		}
	}
}

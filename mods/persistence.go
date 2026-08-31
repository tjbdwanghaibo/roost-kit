package mods

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

const (
	PersistenceCheckpoint = "checkpoint"
	PersistenceDataEngine = "dataengine"
)

var ErrPersistenceEngineSelection = errors.New("persistence: exactly one engine must be enabled")

type PersistenceSelection struct {
	Engine            string
	CheckpointEnabled bool
	DataEngineEnabled bool
}

func ResolvePersistenceEngine(cfg *viper.Viper) (PersistenceSelection, error) {
	if cfg == nil {
		cfg = viper.New()
	}
	engine := strings.ToLower(strings.TrimSpace(cfg.GetString("persistence.engine")))
	if engine == "" {
		engine = PersistenceCheckpoint
	}
	if engine != PersistenceCheckpoint && engine != PersistenceDataEngine {
		return PersistenceSelection{}, fmt.Errorf("%w: unsupported engine %q", ErrPersistenceEngineSelection, engine)
	}
	selection := PersistenceSelection{
		Engine: engine, CheckpointEnabled: engine == PersistenceCheckpoint, DataEngineEnabled: engine == PersistenceDataEngine,
	}
	if cfg.IsSet("checkpoint.enabled") {
		selection.CheckpointEnabled = cfg.GetBool("checkpoint.enabled")
	}
	if cfg.IsSet("dataengine.enabled") {
		selection.DataEngineEnabled = cfg.GetBool("dataengine.enabled")
	}
	if selection.CheckpointEnabled == selection.DataEngineEnabled {
		return PersistenceSelection{}, fmt.Errorf("%w: checkpoint=%t dataengine=%t", ErrPersistenceEngineSelection, selection.CheckpointEnabled, selection.DataEngineEnabled)
	}
	if selection.DataEngineEnabled {
		selection.Engine = PersistenceDataEngine
	} else {
		selection.Engine = PersistenceCheckpoint
	}
	return selection, nil
}

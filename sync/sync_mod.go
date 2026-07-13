package sync

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/app"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/health"
	fnats "github.com/tjbdwanghaibo/cube-core/nats"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
	"github.com/tjbdwanghaibo/cube-kit/mods"
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/viper"
)

// SyncMod implements app.Mod, providing ISyncBus over NATS.
// Depends on: "nats" (fnats.IClient).
type SyncMod struct {
	bus       fsync.ISyncBus
	localSid  int32
	prefix    string
	transport string
	jsCfg     JetStreamSyncConfig
}

func NewSyncMod(localSid int32) *SyncMod {
	return &SyncMod{localSid: localSid}
}

func (m *SyncMod) Name() app.ModName { return mods.ModSync }
func (m *SyncMod) Init(cfg *viper.Viper) error {
	if m.localSid == 0 {
		m.localSid = cfg.GetInt32("sid")
	}
	m.prefix = cfg.GetString("sync.prefix")
	if m.prefix == "" {
		m.prefix = "cube.sync"
	}
	m.transport = strings.ToLower(strings.TrimSpace(cfg.GetString("sync.transport")))
	m.jsCfg = JetStreamSyncConfig{
		LocalSid:     m.localSid,
		Prefix:       m.prefix,
		Stream:       cfg.GetString("sync.stream"),
		Storage:      parseJetStreamSyncStorage(cfg.GetString("sync.storage")),
		AckWait:      cfg.GetDuration("sync.ack_wait"),
		MaxDeliver:   cfg.GetInt("sync.max_deliver"),
		StreamMaxAge: cfg.GetDuration("sync.stream_max_age"),
		Duplicates:   cfg.GetDuration("sync.duplicates"),
		Replicas:     cfg.GetInt("sync.replicas"),
		MaxBytes:     cfg.GetInt64("sync.max_bytes"),
		SetupTimeout: cfg.GetDuration("sync.setup_timeout"),
		PublishTime:  cfg.GetDuration("sync.publish_timeout"),
	}
	return nil
}

func (m *SyncMod) Provide(r *app.Registry) error {
	healthReg, ok := app.Lookup[*health.Registry](r, mods.ModHealth)
	if !ok || healthReg == nil {
		return fmt.Errorf("sync mod: capability %q not found", mods.ModHealth)
	}
	if m.useJetStream() {
		js, ok := app.Lookup[fnats.IJetStream](r, mods.ModNatsJetStream)
		if !ok || js == nil {
			return fmt.Errorf("sync mod: required capability %q not found", mods.ModNatsJetStream)
		}
		bus, err := NewJetStreamSyncBus(fctx.BaseContext(), js, m.jsCfg)
		if err != nil {
			return err
		}
		m.bus = bus
		m.registerHealth(healthReg, "jetstream")
		return r.Register(mods.ModSync, m.bus)
	}
	client, ok := app.Lookup[fnats.IClient](r, mods.ModNats)
	if !ok {
		return fmt.Errorf("sync mod: required capability %q not found", mods.ModNats)
	}
	m.bus = NewNatsSyncBus(client, m.localSid, m.prefix)
	m.registerHealth(healthReg, "nats")
	return r.Register(mods.ModSync, m.bus)
}

func (m *SyncMod) DependsOn() []app.ModName {
	return []app.ModName{mods.ModNats}
}

func (m *SyncMod) Start() error {
	transport := "nats"
	if m.useJetStream() {
		transport = "jetstream"
	}
	slog.Info("sync mod: started", "transport", transport)
	return nil
}

func (m *SyncMod) Stop() {
	if err := m.StopWithContext(fctx.BaseContext()); err != nil {
		slog.Warn("sync mod: stop interrupted", "err", err)
	}
}

func (m *SyncMod) StopWithContext(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	if stopper, ok := m.bus.(interface{ StopWithContext(context.Context) error }); ok {
		if err := stopper.StopWithContext(ctx); err != nil {
			return err
		}
	} else if stopper, ok := m.bus.(interface{ Stop() }); ok {
		stopper.Stop()
	}
	m.bus = nil
	slog.Info("sync mod: stopped")
	return nil
}

func (m *SyncMod) useJetStream() bool {
	return m != nil && (m.transport == "jetstream" || m.transport == "js")
}

func (m *SyncMod) registerHealth(reg *health.Registry, transport string) {
	if reg == nil {
		return
	}
	reg.Register("sync", health.CheckerFunc(func(context.Context) health.Result {
		if m == nil || m.bus == nil {
			return health.Result{Status: health.StatusFail, Message: "bus not initialized"}
		}
		return health.Result{Status: health.StatusOK, Message: transport}
	}))
}

func parseJetStreamSyncStorage(value string) fnats.JetStreamStorage {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case string(fnats.JetStreamStorageMemory):
		return fnats.JetStreamStorageMemory
	case string(fnats.JetStreamStorageFile):
		return fnats.JetStreamStorageFile
	default:
		return ""
	}
}

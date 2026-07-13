package remote_entity

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/app"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
	"github.com/tjbdwanghaibo/cube-core/replica"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
	"github.com/tjbdwanghaibo/cube-kit/mods"
	"fmt"
	"log/slog"

	"github.com/spf13/viper"
)

// RemoteEntityMod implements app.Mod for remote entity lifecycle management.
// Depends on: redis mod (for versioned locks and marker storage).
type RemoteEntityMod struct {
	mgr      *remoteEntityManager
	cfg      *Config
	localSid int32
	registry *app.Registry
	syncRep  *replica.Replicator
}

func NewRemoteEntityMod(localSid int32) *RemoteEntityMod {
	return &RemoteEntityMod{localSid: localSid}
}

func (m *RemoteEntityMod) Name() app.ModName { return mods.ModRemoteEntity }

func (m *RemoteEntityMod) Init(cfg *viper.Viper) error {
	m.cfg = DefaultConfig()
	if m.localSid == 0 {
		m.localSid = cfg.GetInt32("sid")
	}

	if ttl := cfg.GetDuration("remote_entity.lock_ttl"); ttl > 0 {
		m.cfg.LockTTL = ttl
	}
	if key := cfg.GetString("remote_entity.lock_key"); key != "" {
		m.cfg.LockKey = key
	}
	if retry := cfg.GetInt("remote_entity.retry_count"); retry > 0 {
		m.cfg.RetryCount = retry
	}
	if delay := cfg.GetDuration("remote_entity.retry_delay"); delay > 0 {
		m.cfg.RetryDelay = delay
	}
	if timeout := cfg.GetDuration("remote_entity.op_timeout"); timeout > 0 {
		m.cfg.OpTimeout = timeout
	}
	if vttl := cfg.GetDuration("remote_entity.version_ttl"); vttl > 0 {
		m.cfg.VersionTTL = vttl
	}
	if uRetry := cfg.GetInt("remote_entity.unlock_retry_count"); uRetry > 0 {
		m.cfg.UnlockRetryCount = uRetry
	}
	if uInterval := cfg.GetDuration("remote_entity.unlock_retry_interval"); uInterval > 0 {
		m.cfg.UnlockRetryInterval = uInterval
	}
	if cap := cfg.GetInt("remote_entity.sync_retry_queue_cap"); cap > 0 {
		m.cfg.SyncRetryQueueCap = cap
	}
	if interval := cfg.GetDuration("remote_entity.sync_retry_interval"); interval > 0 {
		m.cfg.SyncRetryInterval = interval
	}
	if attempts := cfg.GetInt("remote_entity.sync_retry_max_attempts"); attempts > 0 {
		m.cfg.SyncRetryMaxAttempts = attempts
	}

	return nil
}

func (m *RemoteEntityMod) Provide(r *app.Registry) error {
	redis, ok := app.Lookup[fredis.IRedis](r, mods.ModRedis)
	if !ok {
		return fmt.Errorf("remote_entity mod: required capability %q not found", mods.ModRedis)
	}
	lockFactory := newVersionedLockFactory(redis)

	m.mgr = newRemoteEntityManager(lockFactory, m.cfg, m.localSid)

	// Set up default marker store (Redis-based)
	marker := newRedisMarker(redis, "")
	m.mgr.SetMarkerStore(marker)

	// Register into app registry
	if err := r.Register(mods.ModRemoteEntity, entity.IRemoteEntityManager(m.mgr)); err != nil {
		return err
	}
	if err := r.Register(mods.ModRedisVLock, fredis.IVersionedLockFactory(lockFactory)); err != nil {
		return err
	}
	m.registry = r
	return nil
}

func (m *RemoteEntityMod) DependsOn() []app.ModName {
	return []app.ModName{mods.ModRedis}
}

func (m *RemoteEntityMod) Start() error {
	if err := m.bindSyncer(); err != nil {
		return err
	}
	m.mgr.startSyncRetry()
	entity.BindRemoteEntityManager(m.mgr)
	slog.Info("remote_entity mod: started",
		"sid", m.localSid,
		"lock_key", m.cfg.LockKey,
		"lock_ttl", m.cfg.LockTTL,
		"op_timeout", m.cfg.OpTimeout,
	)
	return nil
}

func (m *RemoteEntityMod) Stop() {
	if err := m.StopWithContext(fctx.BaseContext()); err != nil {
		slog.Warn("remote_entity mod: stop failed", "err", err)
	}
}

func (m *RemoteEntityMod) StopWithContext(ctx context.Context) error {
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	var err error
	if m.mgr != nil {
		err = m.mgr.stopSyncRetryWithContext(ctx)
	}
	if m.syncRep != nil {
		m.syncRep.Stop()
		m.syncRep = nil
	}
	entity.UnbindRemoteEntityManager(m.mgr)
	slog.Info("remote_entity mod: stopped")
	return err
}

func (m *RemoteEntityMod) bindSyncer() error {
	if m.registry == nil {
		return nil
	}
	bus, ok := app.Lookup[fsync.ISyncBus](m.registry, mods.ModSync)
	if !ok {
		return nil
	}
	rep := replica.New(bus, syncTopicRemoteEntity, remoteReplicaStore{mgr: m.mgr})
	syncer := newRemoteSyncer(rep)
	m.mgr.SetSyncer(syncer)

	if err := rep.Start(); err != nil {
		return fmt.Errorf("remote_entity mod: start replica: %w", err)
	}
	m.syncRep = rep
	return nil
}

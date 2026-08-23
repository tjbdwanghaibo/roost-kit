package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/viper"
	"github.com/tjbdwanghaibo/cube-core/app"
	corecheckpoint "github.com/tjbdwanghaibo/cube-core/checkpoint"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/health"
	fmongo "github.com/tjbdwanghaibo/cube-core/mongo"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
	"github.com/tjbdwanghaibo/cube-kit/mods"
)

type Mod struct {
	cfg        modConfig
	backend    *MongoBackend
	checkpoint *corecheckpoint.Checkpoint
	wal        *corecheckpoint.RedisSnapshotWAL
	redis      fredis.IRedis
	access     *entity.ManagerAccess
	repository *EntityRepository

	unregisterRelease func()
	unregisterRemove  func()
	unregisterLoader  func()
	submitErrors      atomic.Int64
	removeErrors      atomic.Int64

	retryMu         sync.Mutex
	pendingSaves    map[retrySaveKey]corecheckpoint.SaveItem
	pendingDeletes  map[retrySaveKey]corecheckpoint.SaveItem
	retryWake       chan struct{}
	retryCancel     context.CancelFunc
	retryWG         sync.WaitGroup
	runtimeFailure  *app.RuntimeFailure
	admissionFenced atomic.Bool
}

type ModOption func(*Mod)

// WithEntityAccess enables atomic on-demand aggregate loading for Nest and
// Remote Entity. The same service-scoped access instance must be supplied to
// those mods so no package-global loader is involved.
func WithEntityAccess(access *entity.ManagerAccess) ModOption {
	return func(mod *Mod) { mod.access = access }
}

type retrySaveKey struct {
	db         string
	dbScope    corecheckpoint.DatabaseScope
	collection string
	id         int64
}

type modConfig struct {
	database            string
	sid                 int32
	journalCap          int
	flushWorkers        int
	batchSize           int
	batchBytes          int
	flushInterval       time.Duration
	retryBackoff        time.Duration
	retryMaxBack        time.Duration
	submitTimeout       time.Duration
	loadConcurrency     int
	maxConcurrentGroups int
	receiptTTL          time.Duration
	warnFillRatio       float64
	retryInterval       time.Duration
	pendingCapacity     int
	walConfig           corecheckpoint.RedisSnapshotWALConfig
	walDurableTimeout   time.Duration
}

func NewMod(options ...ModOption) *Mod {
	mod := &Mod{}
	for _, option := range options {
		if option != nil {
			option(mod)
		}
	}
	return mod
}

func (m *Mod) Name() app.ModName { return mods.ModCheckpoint }

func (m *Mod) DependsOn() []app.ModName {
	return []app.ModName{mods.ModMongo, mods.ModRedis, mods.ModHealth}
}

func (m *Mod) Init(cfg *viper.Viper) error {
	if cfg == nil {
		cfg = viper.New()
	}
	m.cfg = modConfig{
		database:            valueOr(cfg.GetString("checkpoint.database"), "game"),
		sid:                 cfg.GetInt32("sid"),
		journalCap:          positiveOr(cfg.GetInt("checkpoint.journal_capacity"), 10000),
		flushWorkers:        positiveOr(cfg.GetInt("checkpoint.flush_workers"), 4),
		batchSize:           positiveOr(cfg.GetInt("checkpoint.batch_size"), 200),
		batchBytes:          positiveOr(cfg.GetInt("checkpoint.batch_bytes"), 512*1024),
		flushInterval:       durationOr(cfg.GetDuration("checkpoint.flush_interval"), time.Second),
		retryBackoff:        durationOr(cfg.GetDuration("checkpoint.retry_backoff"), 100*time.Millisecond),
		retryMaxBack:        durationOr(cfg.GetDuration("checkpoint.retry_max_backoff"), 5*time.Second),
		submitTimeout:       durationOr(cfg.GetDuration("checkpoint.submit_timeout"), 50*time.Millisecond),
		loadConcurrency:     positiveOr(cfg.GetInt("checkpoint.load_concurrency"), 4),
		maxConcurrentGroups: positiveOr(cfg.GetInt("checkpoint.mongo_max_concurrent_groups"), 8),
		receiptTTL:          durationOr(cfg.GetDuration("checkpoint.transaction_receipt_ttl"), 30*24*time.Hour),
		warnFillRatio:       cfg.GetFloat64("checkpoint.health_warn_fill_ratio"),
		retryInterval:       durationOr(cfg.GetDuration("checkpoint.admission_retry_interval"), 100*time.Millisecond),
		pendingCapacity:     positiveOr(cfg.GetInt("checkpoint.admission_pending_capacity"), 10000),
		walDurableTimeout:   durationOr(cfg.GetDuration("checkpoint.wal.durable_timeout"), 5*time.Second),
		walConfig: corecheckpoint.RedisSnapshotWALConfig{
			Prefix:          valueOr(cfg.GetString("checkpoint.wal.prefix"), "roost:checkpoint:wal"),
			Shards:          positiveOr(cfg.GetInt("checkpoint.wal.shards"), 16),
			WorkerCount:     positiveOr(cfg.GetInt("checkpoint.wal.workers"), 4),
			QueueCap:        positiveOr(cfg.GetInt("checkpoint.wal.queue_capacity"), 4096),
			TTL:             cfg.GetDuration("checkpoint.wal.ttl"),
			ReplayBatchSize: positiveOr(cfg.GetInt("checkpoint.wal.replay_batch_size"), 200),
			RequireAOF:      true,
			AOFLocal:        1,
			AOFReplicas:     nonNegative(cfg.GetInt("checkpoint.wal.aof_replicas")),
			AOFTimeout:      durationOr(cfg.GetDuration("checkpoint.wal.aof_timeout"), 3*time.Second),
		},
	}
	if m.cfg.warnFillRatio <= 0 || m.cfg.warnFillRatio >= 1 {
		m.cfg.warnFillRatio = 0.8
	}
	if m.cfg.retryMaxBack < m.cfg.retryBackoff {
		return fmt.Errorf("checkpoint mod: retry_max_backoff must be >= retry_backoff")
	}
	if m.cfg.walDurableTimeout <= m.cfg.walConfig.AOFTimeout {
		return fmt.Errorf("checkpoint mod: wal durable_timeout must be greater than aof_timeout")
	}
	return nil
}

func (m *Mod) Provide(registry *app.Registry) error {
	if registry == nil {
		return fmt.Errorf("checkpoint mod: nil registry")
	}
	if m.access == nil || m.access.Manager() == nil {
		return fmt.Errorf("checkpoint mod: service-scoped entity access is required")
	}
	m.runtimeFailure, _ = app.Lookup[*app.RuntimeFailure](registry, app.ModRuntimeFailure)
	mongoClient, ok := app.Lookup[fmongo.IMongo](registry, mods.ModMongo)
	if !ok || mongoClient == nil {
		return fmt.Errorf("checkpoint mod: capability %q not found", mods.ModMongo)
	}
	redisClient, ok := app.Lookup[fredis.IRedis](registry, mods.ModRedis)
	if !ok || redisClient == nil {
		return fmt.Errorf("checkpoint mod: capability %q not found", mods.ModRedis)
	}
	backend, err := NewMongoBackend(mongoClient, MongoBackendConfig{
		DefaultDatabase: m.cfg.database, ServerID: m.cfg.sid, MaxConcurrentGroups: m.cfg.maxConcurrentGroups,
		TransactionReceiptTTL: m.cfg.receiptTTL,
	})
	if err != nil {
		return err
	}
	m.backend = backend
	{
		repository, err := NewEntityRepository(m.access.Manager(), backend)
		if err != nil {
			return err
		}
		unregister, err := m.access.ConfigureLoader(repository)
		if err != nil {
			return err
		}
		m.repository = repository
		m.unregisterLoader = unregister
	}
	m.wal = corecheckpoint.NewRedisSnapshotWAL(redisClient, m.cfg.walConfig)
	m.redis = redisClient
	m.checkpoint = corecheckpoint.New(backend,
		corecheckpoint.WithJournalCap(m.cfg.journalCap),
		corecheckpoint.WithFlushWorkers(m.cfg.flushWorkers),
		corecheckpoint.WithBatchSize(m.cfg.batchSize),
		corecheckpoint.WithBatchBytes(m.cfg.batchBytes),
		corecheckpoint.WithFlushInterval(m.cfg.flushInterval),
		corecheckpoint.WithRetryBackoff(m.cfg.retryBackoff, m.cfg.retryMaxBack),
		corecheckpoint.WithJournalSubmitTimeout(m.cfg.submitTimeout),
		corecheckpoint.WithLoadConcurrency(m.cfg.loadConcurrency),
		corecheckpoint.WithSnapshotWAL(m.wal),
		corecheckpoint.WithSnapshotWALRequired(true),
		corecheckpoint.WithSnapshotWALMode(corecheckpoint.SnapshotWALModeDurable),
		corecheckpoint.WithSnapshotWALDurableTimeout(m.cfg.walDurableTimeout),
	)
	m.pendingSaves = make(map[retrySaveKey]corecheckpoint.SaveItem)
	m.pendingDeletes = make(map[retrySaveKey]corecheckpoint.SaveItem)
	m.retryWake = make(chan struct{}, 1)
	// Register the owning capability rather than the inner Checkpoint. Flush on
	// Mod also drains admission retries, so callers cannot accidentally report
	// success while snapshots are still waiting outside the journal.
	if err := registry.Register(mods.ModCheckpoint, m); err != nil {
		return err
	}
	healthRegistry, ok := app.Lookup[*health.Registry](registry, mods.ModHealth)
	if !ok || healthRegistry == nil {
		return fmt.Errorf("checkpoint mod: capability %q not found", mods.ModHealth)
	}
	healthRegistry.Register("checkpoint", health.CheckerFunc(m.checkHealth))
	return nil
}

func validateRedisWALDurability(parent context.Context, client fredis.IRedis, cfg corecheckpoint.RedisSnapshotWALConfig, deadline time.Duration) error {
	durable, ok := client.(fredis.DurableEvaler)
	if !ok {
		return errors.New("redis client does not support same-connection WAITAOF")
	}
	ctx, cancel := context.WithTimeout(parent, deadline)
	defer cancel()
	key := fmt.Sprintf("%s:{aof-probe}:%d:%d", cfg.Prefix, time.Now().UnixNano(), cfg.AOFReplicas)
	const probeScript = `return redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])`
	_, local, replicas, err := durable.EvalDurable(ctx, probeScript, []string{key}, cfg.AOFLocal, cfg.AOFReplicas, cfg.AOFTimeout, "1", 1000)
	if err != nil {
		return err
	}
	if local < int64(cfg.AOFLocal) || replicas < int64(cfg.AOFReplicas) {
		return fmt.Errorf("AOF threshold not met: local=%d/%d replicas=%d/%d", local, cfg.AOFLocal, replicas, cfg.AOFReplicas)
	}
	return nil
}

func (m *Mod) Backend() *MongoBackend {
	if m == nil {
		return nil
	}
	return m.backend
}

func (m *Mod) Checkpoint() *corecheckpoint.Checkpoint {
	if m == nil {
		return nil
	}
	return m.checkpoint
}

func (m *Mod) Start() error {
	if m == nil || m.checkpoint == nil {
		return fmt.Errorf("checkpoint mod: not provided")
	}
	startupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := validateRedisWALDurability(startupCtx, m.redis, m.cfg.walConfig, m.cfg.walDurableTimeout); err != nil {
		return fmt.Errorf("checkpoint mod: redis WAL durability validation failed: %w", err)
	}
	if err := m.backend.EnsureInfrastructure(startupCtx); err != nil {
		return fmt.Errorf("checkpoint mod: ensure mongo infrastructure: %w", err)
	}
	if err := m.checkpoint.Start(startupCtx); err != nil {
		return err
	}
	retryCtx, cancel := context.WithCancel(context.Background())
	m.retryCancel = cancel
	m.retryWG.Add(1)
	go m.runAdmissionRetry(retryCtx)
	var err error
	m.unregisterRelease, err = m.access.RegisterOnEntityRelease(m.onEntityRelease)
	if err != nil {
		m.retryCancel()
		m.retryCancel = nil
		m.retryWG.Wait()
		_ = m.checkpoint.Stop(context.Background())
		return err
	}
	m.unregisterRemove, err = m.access.RegisterOnEntityRemoveFromDB(m.onEntityRemove)
	if err != nil {
		m.unregisterRelease()
		m.unregisterRelease = nil
		m.retryCancel()
		m.retryCancel = nil
		m.retryWG.Wait()
		_ = m.checkpoint.Stop(context.Background())
		return err
	}
	return nil
}

func (m *Mod) Stop() { _ = m.StopWithContext(context.Background()) }

func (m *Mod) StopWithContext(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if m.unregisterRelease != nil {
		m.unregisterRelease()
		m.unregisterRelease = nil
	}
	if m.unregisterRemove != nil {
		m.unregisterRemove()
		m.unregisterRemove = nil
	}
	if m.unregisterLoader != nil {
		m.unregisterLoader()
		m.unregisterLoader = nil
	}
	if m.checkpoint == nil {
		return nil
	}
	flushErr := m.Flush(ctx)
	if m.retryCancel != nil {
		m.retryCancel()
		m.retryCancel = nil
	}
	m.retryWG.Wait()
	stopErr := m.checkpoint.Stop(ctx)
	return errors.Join(flushErr, stopErr)
}

func (m *Mod) Flush(ctx context.Context) error {
	if m == nil || m.checkpoint == nil {
		return corecheckpoint.ErrCheckpointNotRunning
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := m.retryAdmission(ctx); err != nil {
			return err
		}
		if m.pendingCount() == 0 {
			return m.checkpoint.Flush(ctx)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.cfg.retryInterval):
		}
	}
}

func (m *Mod) onEntityRelease(ent entity.IThreadSafeEntity) {
	if ent == nil || !ent.AutoPersist() || entity.IsEntityKindRemoteManaged(ent.GetEntityKind()) {
		return
	}
	snapshotter, ok := ent.(corecheckpoint.EntitySnapshotter)
	if !ok {
		return
	}
	items := snapshotter.Snapshot()
	if len(items) > 0 && !m.checkpoint.Submit(items) {
		corecheckpoint.RollbackSaveItems(items)
		m.submitErrors.Add(1)
		m.enqueueSaveRetry(items)
	} else if len(items) > 0 {
		m.clearSaveRetry(items)
	}
}

func (m *Mod) onEntityRemove(ent entity.IThreadSafeEntity) {
	if ent == nil || !ent.AutoPersist() || entity.IsEntityKindRemoteManaged(ent.GetEntityKind()) {
		return
	}
	remover, ok := ent.(corecheckpoint.EntityRemoveSnapshotter)
	if !ok {
		m.removeErrors.Add(1)
		return
	}
	if items := remover.RemoveSnapshot(); len(items) > 0 {
		if !m.checkpoint.SubmitRemoveItems(items) {
			m.removeErrors.Add(1)
			m.enqueueDeleteRetry(items)
		} else {
			m.clearDeleteRetry(items)
		}
	}
}

func (m *Mod) checkHealth(context.Context) health.Result {
	if m == nil || m.checkpoint == nil || !m.checkpoint.Running() {
		return health.Result{Status: health.StatusFail, Message: "not running"}
	}
	if m.admissionFenced.Load() {
		return health.Result{Status: health.StatusFail, Message: "checkpoint admission fenced"}
	}
	if pending := m.pendingCount(); pending > 0 {
		return health.Result{Status: health.StatusFail, Message: fmt.Sprintf("admission_pending=%d admission_errors=%d", pending, m.submitErrors.Load()+m.removeErrors.Load())}
	}
	walHealth := m.wal.CheckHealth(corecheckpoint.RedisSnapshotWALHealthPolicy{})
	if walHealth.Status != health.StatusOK {
		return walHealth
	}
	stats := m.checkpoint.JournalStats()
	status := health.StatusOK
	if stats.FillRatio >= m.cfg.warnFillRatio {
		status = health.StatusDegraded
	}
	return health.Result{Status: status, Message: fmt.Sprintf("pending=%d capacity=%d fill=%.2f", stats.Len, stats.Cap, stats.FillRatio)}
}

func retryKey(item corecheckpoint.SaveItem) retrySaveKey {
	return retrySaveKey{db: item.Db, dbScope: item.DbScope, collection: item.Collection, id: item.ID}
}

func (m *Mod) enqueueSaveRetry(items []corecheckpoint.SaveItem) {
	m.retryMu.Lock()
	overloaded := false
	for _, item := range items {
		key := retryKey(item)
		current, exists := m.pendingSaves[key]
		if !exists && m.pendingRetryCountLocked() >= m.cfg.pendingCapacity {
			overloaded = true
			continue
		}
		if !exists || item.Version >= current.Version {
			m.pendingSaves[key] = item
		}
	}
	m.retryMu.Unlock()
	if overloaded {
		m.fenceAdmission(fmt.Errorf("checkpoint: admission retry capacity exceeded: capacity=%d", m.cfg.pendingCapacity))
	}
	m.signalRetry()
}

func (m *Mod) enqueueDeleteRetry(items []corecheckpoint.SaveItem) {
	m.retryMu.Lock()
	overloaded := false
	for _, item := range items {
		key := retryKey(item)
		if _, exists := m.pendingDeletes[key]; !exists && m.pendingRetryCountLocked() >= m.cfg.pendingCapacity {
			overloaded = true
			continue
		}
		m.pendingDeletes[key] = item
	}
	m.retryMu.Unlock()
	if overloaded {
		m.fenceAdmission(fmt.Errorf("checkpoint: delete admission retry capacity exceeded: capacity=%d", m.cfg.pendingCapacity))
	}
	m.signalRetry()
}

func (m *Mod) clearSaveRetry(items []corecheckpoint.SaveItem) {
	m.retryMu.Lock()
	for _, item := range items {
		key := retryKey(item)
		if current, ok := m.pendingSaves[key]; ok && current.Version <= item.Version {
			delete(m.pendingSaves, key)
		}
	}
	m.retryMu.Unlock()
}

func (m *Mod) clearDeleteRetry(items []corecheckpoint.SaveItem) {
	m.retryMu.Lock()
	for _, item := range items {
		delete(m.pendingDeletes, retryKey(item))
	}
	m.retryMu.Unlock()
}

func (m *Mod) pendingCount() int {
	m.retryMu.Lock()
	count := len(m.pendingSaves) + len(m.pendingDeletes)
	m.retryMu.Unlock()
	return count
}

func (m *Mod) pendingRetryCountLocked() int {
	return len(m.pendingSaves) + len(m.pendingDeletes)
}

func (m *Mod) fenceAdmission(err error) {
	if m == nil || err == nil || !m.admissionFenced.CompareAndSwap(false, true) {
		return
	}
	if m.runtimeFailure != nil {
		m.runtimeFailure.Fail(err)
	}
}

func (m *Mod) signalRetry() {
	select {
	case m.retryWake <- struct{}{}:
	default:
	}
}

func (m *Mod) runAdmissionRetry(ctx context.Context) {
	defer m.retryWG.Done()
	ticker := time.NewTicker(m.cfg.retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-m.retryWake:
		}
		_ = m.retryAdmission(ctx)
	}
}

func (m *Mod) retryAdmission(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.retryMu.Lock()
	saves := make(map[retrySaveKey]corecheckpoint.SaveItem, len(m.pendingSaves))
	deletes := make(map[retrySaveKey]corecheckpoint.SaveItem, len(m.pendingDeletes))
	for key, item := range m.pendingSaves {
		saves[key] = item
	}
	for key, item := range m.pendingDeletes {
		deletes[key] = item
	}
	m.retryMu.Unlock()
	for key, item := range saves {
		if !m.checkpoint.Submit([]corecheckpoint.SaveItem{item}) {
			continue
		}
		m.retryMu.Lock()
		if current, ok := m.pendingSaves[key]; ok && current.Version <= item.Version {
			delete(m.pendingSaves, key)
		}
		m.retryMu.Unlock()
	}
	for key, item := range deletes {
		if !m.checkpoint.SubmitRemoveItems([]corecheckpoint.SaveItem{item}) {
			continue
		}
		m.retryMu.Lock()
		delete(m.pendingDeletes, key)
		m.retryMu.Unlock()
	}
	return nil
}

func positiveOr(value, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func durationOr(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func valueOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

var _ app.Mod = (*Mod)(nil)
var _ app.ModStopperWithContext = (*Mod)(nil)

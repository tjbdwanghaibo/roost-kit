package remote_entity

import (
	"context"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/obs"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
)

// remoteEntityManager implements entity.IRemoteEntityManager.
type remoteEntityManager struct {
	mu          sync.RWMutex
	wrappers    map[int64]*remoteEntityWrapper
	lockFactory fredis.IVersionedLockFactory
	loader      entity.IRemoteEntityLoader
	markerStore entity.IRemoteEntityMarkerStore
	syncer      entity.IRemoteEntitySyncer
	syncRetry   *syncRetryQueue
	cfg         *Config
	localSid    int32
}

var _ entity.IRemoteEntityManager = (*remoteEntityManager)(nil)

func newRemoteEntityManager(lockFactory fredis.IVersionedLockFactory, cfg *Config, localSid int32) *remoteEntityManager {
	mgr := &remoteEntityManager{
		wrappers:    make(map[int64]*remoteEntityWrapper),
		lockFactory: lockFactory,
		cfg:         cfg,
		localSid:    localSid,
	}
	mgr.syncRetry = newSyncRetryQueue(mgr, cfg)
	return mgr
}

// --- IRemoteEntityWrapperManager ---

func (m *remoteEntityManager) GetOrCreate(id int64, category entity.EntityCategory, kind entity.EntityKind) entity.IRemoteEntityWrapper {
	meta := resolveRemoteWrapperID(id, category, kind)
	if meta.FullID == 0 {
		return nil
	}
	return m.getOrCreate(meta.FullID, meta.Category, meta.Kind)
}

func (m *remoteEntityManager) getOrCreate(id int64, category entity.EntityCategory, kind entity.EntityKind) *remoteEntityWrapper {
	meta := resolveRemoteWrapperID(id, category, kind)
	if meta.FullID == 0 {
		return nil
	}
	id = meta.FullID
	category = meta.Category
	kind = meta.Kind

	m.mu.RLock()
	w, ok := m.wrappers[id]
	m.mu.RUnlock()
	if ok {
		return w
	}

	// Create new wrapper
	opts := fredis.VersionedLockOptions{
		Key:           m.cfg.LockKey,
		TTL:           m.cfg.LockTTL,
		RetryInterval: m.cfg.RetryDelay,
		RetryCount:    m.cfg.RetryCount,
	}
	rMu := m.lockFactory.NewVersionedLock(id, opts)
	newW := newRemoteEntityWrapper(id, category, kind, rMu, m)

	// Sync initial mark state from store
	if err := newW.refreshMarked(fctx.BaseContext()); err != nil {
		slog.Warn("remote_entity: initial mark refresh failed, using remote path",
			"id", id, "err", err)
	}

	// Double-check under write lock
	m.mu.Lock()
	if existing, ok := m.wrappers[id]; ok {
		m.mu.Unlock()
		return existing
	}
	m.wrappers[id] = newW
	m.mu.Unlock()
	return newW
}

func resolveRemoteWrapperID(id int64, category entity.EntityCategory, kind entity.EntityKind) entity.EntityIDMeta {
	fullID, err := entity.NormalizeFullID(id, kind)
	if err != nil {
		return entity.EntityIDMeta{}
	}
	meta := entity.ResolveEntityID(fullID)
	if category != entity.EntityCategoryNone && meta.Category != category {
		return entity.EntityIDMeta{}
	}
	return meta
}

func (m *remoteEntityManager) Get(id int64) (entity.IRemoteEntityWrapper, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	w, ok := m.wrappers[id]
	if !ok {
		return nil, false
	}
	return w, true
}

func (m *remoteEntityManager) Remove(id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.wrappers, id)
}

func (m *remoteEntityManager) IsRemoteMarked(id int64) bool {
	if m == nil || id == 0 {
		return false
	}
	if w, ok := m.Get(id); ok && w != nil {
		return w.IsMarked()
	}
	if m.markerStore == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(fctx.BaseContext(), m.cfg.OpTimeout)
	defer cancel()
	marked, err := m.markerStore.IsMarked(ctx, id)
	if err != nil {
		slog.Warn("remote_entity: marker check failed, using marked path", "id", id, "err", err)
		return true
	}
	return marked
}

// PrepareRemoteEntities is the batch entry point for nest dispatch.
func (m *remoteEntityManager) PrepareRemoteEntities(ids []int64) (release func(), err error) {
	if len(ids) == 0 {
		return func() {}, nil
	}

	l := len(ids)

	// Separate marked (need dist lock, slow) vs non-marked (local, fast)
	var remoteIds []int64
	var localIds []int64
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		meta := entity.ResolveEntityID(id)
		if _, ok := seen[meta.FullID]; ok {
			continue
		}
		seen[meta.FullID] = struct{}{}
		w := m.getOrCreate(meta.FullID, meta.Category, meta.Kind)
		if w == nil {
			err = fmt.Errorf("remote_entity: invalid id %d", id)
			return
		}
		if err := w.refreshMarked(fctx.BaseContext()); err != nil {
			slog.Warn("remote_entity: mark refresh failed, using remote path",
				"id", meta.FullID, "err", err)
		}
		if w.IsMarked() {
			remoteIds = append(remoteIds, meta.FullID)
		} else {
			localIds = append(localIds, meta.FullID)
		}
	}

	// Sort within each group to prevent deadlock
	sort.Slice(remoteIds, func(i, j int) bool { return remoteIds[i] < remoteIds[j] })
	sort.Slice(localIds, func(i, j int) bool { return localIds[i] < localIds[j] })

	// Order: remote first (slow) → local after (fast, minimize local lock hold time)
	ordered := make([]int64, 0, l)
	ordered = append(ordered, remoteIds...)
	ordered = append(ordered, localIds...)

	// TryCastEntity for each, collect release funcs
	rlsList := make([]func(), 0, l)
	for _, id := range ordered {
		meta := entity.ResolveEntityID(id)
		w := m.getOrCreate(meta.FullID, meta.Category, meta.Kind)
		if w == nil {
			err = fmt.Errorf("remote_entity: invalid id %d", id)
			return
		}
		rlsF, castErr := w.TryCastEntity()
		rlsList = append(rlsList, rlsF)
		if castErr != nil {
			// Release all acquired so far (including current failed one)
			for j := len(rlsList) - 1; j >= 0; j-- {
				if rlsList[j] != nil {
					rlsList[j]()
				}
			}
			err = fmt.Errorf("remote_entity: prepare %d: %w", meta.FullID, castErr)
			return
		}
	}

	release = func() {
		for i := len(rlsList) - 1; i >= 0; i-- {
			if rlsList[i] != nil {
				rlsList[i]()
			}
		}
	}
	return
}

// --- IRemoteEntityManager ---

func (m *remoteEntityManager) SetLoader(loader entity.IRemoteEntityLoader) {
	m.loader = loader
}

func (m *remoteEntityManager) SetMarkerStore(store entity.IRemoteEntityMarkerStore) {
	m.markerStore = store
}

func (m *remoteEntityManager) SetSyncer(syncer entity.IRemoteEntitySyncer) {
	m.syncer = syncer
}

func (m *remoteEntityManager) Loader() entity.IRemoteEntityLoader {
	return m.loader
}

func (m *remoteEntityManager) MarkerStore() entity.IRemoteEntityMarkerStore {
	return m.markerStore
}

func (m *remoteEntityManager) Syncer() entity.IRemoteEntitySyncer {
	return m.syncer
}

func (m *remoteEntityManager) ResolveRemoteSnapshot(req entity.RemoteSnapshotResolveRequest) (entity.RemoteSnapshot, error) {
	if m == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: manager is nil")
	}
	if !req.Ref.Valid() {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: invalid remote snapshot ref: %+v", req.Ref)
	}
	meta := entity.ResolveEntityID(req.Ref.EntityID)
	w := m.getOrCreate(meta.FullID, meta.Category, meta.Kind)
	if w == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: invalid remote snapshot entity %d", req.Ref.EntityID)
	}
	snapshotReq := entity.RemoteSnapshotRequest{
		Scope:      req.Scope,
		RouteEpoch: req.RouteEpoch,
		Option:     req.Option,
	}
	switch req.Mode {
	case entity.RemoteAcquireCache:
		snapshot, err := w.TryCachedSnapshot(snapshotReq)
		m.observeResolve(req.Mode, snapshot.Source, err)
		return snapshot, err
	case entity.RemoteAcquireReadOnly:
		snapshot, err := w.TryReadOnlySnapshot(snapshotReq)
		m.observeResolve(req.Mode, snapshot.Source, err)
		return snapshot, err
	default:
		err := fmt.Errorf("remote_entity: remote snapshot mode %d is not readable", req.Mode)
		m.observeResolve(req.Mode, entity.RemoteSnapshotSourceUnknown, err)
		return entity.RemoteSnapshot{}, err
	}
}

func (m *remoteEntityManager) observeResolve(mode entity.RemoteAcquireMode, source entity.RemoteSnapshotSource, err error) {
	result := "ok"
	if err != nil {
		result = "error"
		if errors.Is(err, entity.ErrRemoteSnapshotStale) {
			result = "stale"
		}
	}
	obs.IncCounter("remote_entity.snapshot.resolve_total", obs.Labels{
		"mode":   remoteAcquireModeLabel(mode),
		"source": source.String(),
		"result": result,
	}, 1)
}

func remoteAcquireModeLabel(mode entity.RemoteAcquireMode) string {
	switch mode {
	case entity.RemoteAcquireCache:
		return "cache"
	case entity.RemoteAcquireReadOnly:
		return "read_only"
	case entity.RemoteAcquireWrite:
		return "write"
	default:
		return "unknown"
	}
}

func (m *remoteEntityManager) startSyncRetry() {
	if m.syncRetry != nil {
		m.syncRetry.Start()
	}
}

func (m *remoteEntityManager) stopSyncRetry() {
	if err := m.stopSyncRetryWithContext(fctx.BaseContext()); err != nil {
		slog.Warn("remote_entity: stop sync retry failed", "err", err)
	}
}

func (m *remoteEntityManager) stopSyncRetryWithContext(ctx context.Context) error {
	if m.syncRetry != nil {
		return m.syncRetry.StopWithContext(ctx)
	}
	return nil
}

package remote_entity

import (
	"context"
	"fmt"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/lock"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// remoteEntityWrapper implements entity.IRemoteEntityWrapper.
type remoteEntityWrapper struct {
	id          int64
	category    entity.EntityCategory
	kind        entity.EntityKind
	e           entity.IThreadSafeRemoteEntity
	rMu         fredis.IVersionedLock // distributed versioned lock
	mu          lock.Mutex            // local mutex
	marker      atomic.Uint32         // unknown, local, or shared marker cache
	markerAt    atomic.Int64          // marker cache refresh time
	markerLease entity.RemoteEntityMarkerLease
	markerMu    sync.RWMutex
	ownershipMu sync.RWMutex         // gates local writes while ownership changes
	mgr         *remoteEntityManager // back-reference
}

var _ entity.IRemoteEntityWrapper = (*remoteEntityWrapper)(nil)

const (
	markerUnknown uint32 = iota
	markerLocal
	markerShared
)

func newRemoteEntityWrapper(id int64, category entity.EntityCategory, kind entity.EntityKind, rMu fredis.IVersionedLock, mgr *remoteEntityManager) *remoteEntityWrapper {
	var mu lock.Mutex
	meta := resolveRemoteWrapperID(id, category, kind)
	guid := meta.FullID
	if lock.Mgr != nil {
		mu = lock.Mgr.GetLock(guid)
	} else {
		mu = lock.NewReentrantMutex(guid)
	}
	return &remoteEntityWrapper{
		id:       guid,
		category: meta.Category,
		kind:     meta.Kind,
		rMu:      rMu,
		mu:       mu,
		mgr:      mgr,
	}
}

// TryCastEntity acquires lock (if marked) → version check → load → returns release func.
func (w *remoteEntityWrapper) TryCastEntity() (release func(), err error) {
	if w == nil || w.mgr == nil {
		return func() {}, fmt.Errorf("remote_entity: wrapper is nil")
	}
	w.ownershipMu.RLock()
	releaseOwnership := func() { w.ownershipMu.RUnlock() }
	ctx, cancel := context.WithTimeout(fctx.BaseContext(), w.mgr.cfg.OpTimeout)
	defer cancel()
	// Mutations refresh ownership on every entry. The cache remains useful for
	// read-only paths, but must never admit a write after a Mark transition.
	if err = w.refreshMarked(ctx); err != nil {
		releaseOwnership()
		return func() {}, fmt.Errorf("remote_entity: resolve marker %d: %w", w.id, err)
	}
	marked := w.IsMarked()
	if !marked && !w.isLocalOwner() {
		releaseOwnership()
		return func() {}, fmt.Errorf("remote_entity: local entity %d is owned by sid %d", w.id, w.leaseOwner())
	}
	if marked {
		// === Marked path: dist lock → version check → load if needed ===
		if err = w.rMu.Lock(ctx); err != nil {
			release = w.releaseWithOwnership(w.genReleaseFunc(), releaseOwnership)
			return
		}
		if err = w.refreshMarked(ctx); err != nil {
			release = w.releaseWithOwnership(w.genLockReleaseFunc(), releaseOwnership)
			return
		}
		if !w.IsMarked() {
			unlockVersion := w.rMu.Version()
			if unlockErr := w.rMu.UnlockWithRetry(ctx, unlockVersion, w.mgr.cfg.VersionTTL, w.mgr.cfg.UnlockRetryCount, w.mgr.cfg.UnlockRetryInterval); unlockErr != nil {
				return w.releaseWithOwnership(w.genLockReleaseFunc(), releaseOwnership), fmt.Errorf("remote_entity: release stale marker lock %d: %w", w.id, unlockErr)
			}
			if !w.isLocalOwner() {
				releaseOwnership()
				return func() {}, fmt.Errorf("remote_entity: local entity %d is owned by sid %d", w.id, w.leaseOwner())
			}
			w.e = w.getLocalEntity()
			if w.e == nil {
				releaseOwnership()
				return func() {}, fmt.Errorf("remote_entity: local entity %d not found", w.id)
			}
			w.e.SetExcludeSId(w.mgr.localSid)
			return w.releaseWithOwnership(nil, releaseOwnership), nil
		}

		rVersion := w.rMu.Version()

		// Try local entity with version hit
		if localE := w.getLocalEntity(); localE != nil && localE.EntityVersion() == rVersion && rVersion > 0 {
			w.e = localE
			w.e.SetExcludeSId(0)
		} else {
			// Version miss or no local entity: load from DB
			if w.mgr.loader != nil {
				w.e = w.mgr.loader.LoadRemoteEntity(w.id, w.kind)
			}
			if w.e != nil {
				w.e.SetEntityVersion(rVersion)
				w.e.SetExcludeSId(0)
			}
		}

		if w.e == nil {
			err = fmt.Errorf("remote_entity: entity %d not found", w.id)
			release = w.releaseWithOwnership(w.genReleaseFunc(), releaseOwnership)
			return
		}
	} else {
		// === Non-marked path: no dist lock, local entity only ===
		w.e = w.getLocalEntity()
		if w.e == nil {
			err = fmt.Errorf("remote_entity: local entity %d not found", w.id)
			release = w.releaseWithOwnership(w.genReleaseFunc(), releaseOwnership)
			return
		}
		w.e.SetExcludeSId(w.mgr.localSid)
	}
	release = w.releaseWithOwnership(w.genReleaseFunc(), releaseOwnership)
	return
}

func (w *remoteEntityWrapper) TryReadOnlyEntity(option entity.RemoteReadOption) (entity.IThreadSafeRemoteEntity, func(), error) {
	if w == nil {
		return nil, func() {}, fmt.Errorf("remote_entity: read-only wrapper is nil")
	}
	option = entity.NormalizeRemoteReadOption(option)
	if err := w.ensureMarker(fctx.BaseContext()); err != nil {
		return nil, func() {}, fmt.Errorf("remote_entity: resolve read-only marker %d: %w", w.id, err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if localE := w.getLocalEntity(); localE != nil && option.Accepts(localE.EntityVersion()) {
		w.e = localE
		w.e.SetExcludeSId(w.localExcludeSid())
		return localE, func() {}, nil
	}
	if w.mgr == nil || w.mgr.loader == nil {
		return nil, func() {}, fmt.Errorf("remote_entity: read-only loader is nil for %d", w.id)
	}
	loaded := w.mgr.loader.LoadRemoteEntity(w.id, w.kind)
	if loaded == nil {
		return nil, func() {}, fmt.Errorf("remote_entity: read-only entity %d not found", w.id)
	}
	if !option.Accepts(loaded.EntityVersion()) {
		return nil, func() {}, fmt.Errorf("%w: remote_entity read-only entity %d version=%d min=%d", entity.ErrRemoteSnapshotStale, w.id, loaded.EntityVersion(), option.MinVersion)
	}
	w.e = loaded
	w.e.SetExcludeSId(w.localExcludeSid())
	return loaded, func() {}, nil
}

func (w *remoteEntityWrapper) TryReadOnlySnapshot(req entity.RemoteSnapshotRequest) (entity.RemoteSnapshot, error) {
	if w == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: read-only snapshot wrapper is nil")
	}
	option := entity.NormalizeRemoteReadOption(req.Option)
	if err := w.ensureMarker(fctx.BaseContext()); err != nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: resolve snapshot marker %d: %w", w.id, err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if localE := w.getLocalEntity(); localE != nil && option.Accepts(localE.EntityVersion()) {
		w.e = localE
		w.e.SetExcludeSId(w.localExcludeSid())
		return w.snapshotFromEntity(localE, req, entity.RemoteSnapshotSourceLocal)
	}
	if w.mgr == nil || w.mgr.loader == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: read-only snapshot loader is nil for %d", w.id)
	}
	loaded := w.mgr.loader.LoadRemoteEntity(w.id, w.kind)
	if loaded == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: read-only snapshot entity %d not found", w.id)
	}
	if !option.Accepts(loaded.EntityVersion()) {
		return entity.RemoteSnapshot{}, fmt.Errorf("%w: remote_entity read-only snapshot entity %d version=%d min=%d", entity.ErrRemoteSnapshotStale, w.id, loaded.EntityVersion(), option.MinVersion)
	}
	w.e = loaded
	w.e.SetExcludeSId(w.localExcludeSid())
	return w.snapshotFromEntity(loaded, req, entity.RemoteSnapshotSourceLoaded)
}

func (w *remoteEntityWrapper) TryCachedSnapshot(req entity.RemoteSnapshotRequest) (entity.RemoteSnapshot, error) {
	if w == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: cached snapshot wrapper is nil")
	}
	option := entity.NormalizeRemoteReadOption(req.Option)
	if err := w.ensureMarker(fctx.BaseContext()); err != nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: resolve cached snapshot marker %d: %w", w.id, err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if localE := w.getLocalEntity(); localE != nil && option.Accepts(localE.EntityVersion()) {
		w.e = localE
		w.e.SetExcludeSId(w.localExcludeSid())
		return w.snapshotFromEntity(localE, req, entity.RemoteSnapshotSourceLocal)
	}
	if w.e != nil && option.Accepts(w.e.EntityVersion()) {
		w.e.SetExcludeSId(w.localExcludeSid())
		return w.snapshotFromEntity(w.e, req, entity.RemoteSnapshotSourceCache)
	}
	if w.mgr == nil || w.mgr.loader == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: cached snapshot loader is nil for %d", w.id)
	}
	loaded := w.mgr.loader.LoadRemoteEntity(w.id, w.kind)
	if loaded == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: cached snapshot entity %d not found", w.id)
	}
	if !option.Accepts(loaded.EntityVersion()) {
		return entity.RemoteSnapshot{}, fmt.Errorf("%w: remote_entity cached snapshot entity %d version=%d min=%d", entity.ErrRemoteSnapshotStale, w.id, loaded.EntityVersion(), option.MinVersion)
	}
	w.e = loaded
	w.e.SetExcludeSId(w.localExcludeSid())
	return w.snapshotFromEntity(loaded, req, entity.RemoteSnapshotSourceLoaded)
}

func (w *remoteEntityWrapper) snapshotFromEntity(e entity.IThreadSafeRemoteEntity, req entity.RemoteSnapshotRequest, source entity.RemoteSnapshotSource) (entity.RemoteSnapshot, error) {
	if e == nil {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: snapshot entity is nil for %d", w.id)
	}
	provider, ok := e.(entity.RemoteSnapshotProvider)
	if !ok {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: entity %d does not provide remote snapshot", w.id)
	}
	data, ok := provider.RemoteSnapshot(req.Scope)
	if !ok {
		return entity.RemoteSnapshot{}, fmt.Errorf("remote_entity: entity %d remote snapshot scope %d unavailable", w.id, req.Scope)
	}
	readAt := entity.RemoteSnapshotReadAt(req.Option)
	return entity.RemoteSnapshot{
		EntityID:   w.id,
		Kind:       w.kind,
		Scope:      req.Scope,
		Version:    uint64(e.EntityVersion()),
		RouteEpoch: req.RouteEpoch,
		Source:     source,
		ReadAt:     readAt,
		ExpiresAt:  entity.RemoteSnapshotExpiresAt(readAt, req.Option),
		Data:       data,
	}, nil
}

// MarkRemote marks entity as remote-owned.
func (w *remoteEntityWrapper) MarkRemote(ctx context.Context) (func(), error) {
	if w.mgr == nil || w.mgr.markerStore == nil {
		return nil, fmt.Errorf("remote_entity: marker store is required for mark %d", w.id)
	}
	w.ownershipMu.Lock()
	defer w.ownershipMu.Unlock()
	if err := w.refreshMarked(ctx); err != nil {
		return nil, fmt.Errorf("remote_entity: refresh mark %d: %w", w.id, err)
	}
	if w.IsMarked() {
		return nil, fmt.Errorf("remote_entity: entity %d already marked", w.id)
	}

	// Acquire dist lock
	if err := w.rMu.Lock(ctx); err != nil {
		return nil, fmt.Errorf("remote_entity: mark lock %d: %w", w.id, err)
	}
	if err := w.refreshMarked(ctx); err != nil {
		unlockErr := w.rMu.Unlock(ctx, w.rMu.Version(), w.mgr.cfg.VersionTTL)
		if unlockErr != nil {
			return nil, fmt.Errorf("remote_entity: refresh locked mark %d: %w; unlock: %v", w.id, err, unlockErr)
		}
		return nil, fmt.Errorf("remote_entity: refresh locked mark %d: %w", w.id, err)
	}
	if w.IsMarked() {
		unlockErr := w.rMu.Unlock(ctx, w.rMu.Version(), w.mgr.cfg.VersionTTL)
		if unlockErr != nil {
			return nil, fmt.Errorf("remote_entity: entity %d already marked; unlock: %v", w.id, unlockErr)
		}
		return nil, fmt.Errorf("remote_entity: entity %d already marked", w.id)
	}
	if !w.isLocalOwner() {
		owner := w.leaseOwner()
		unlockErr := w.rMu.Unlock(ctx, w.rMu.Version(), w.mgr.cfg.VersionTTL)
		if unlockErr != nil {
			return nil, fmt.Errorf("remote_entity: local entity %d is owned by sid %d; unlock: %v", w.id, owner, unlockErr)
		}
		return nil, fmt.Errorf("remote_entity: local entity %d is owned by sid %d", w.id, owner)
	}

	// Write mark
	var lease entity.RemoteEntityMarkerLease
	var err error
	w.markerMu.RLock()
	expected := w.markerLease
	w.markerMu.RUnlock()
	expected.OwnerSid = w.mgr.localSid
	if store, ok := w.mgr.markerStore.(expectedMarkerStore); ok {
		lease, err = store.MarkExpected(ctx, w.id, expected)
	} else {
		lease, err = w.mgr.markerStore.Mark(ctx, w.id, w.mgr.localSid)
	}
	if err != nil {
		// Release lock on failure
		if unlockErr := w.rMu.Unlock(ctx, w.rMu.Version(), w.mgr.cfg.VersionTTL); unlockErr != nil {
			return nil, fmt.Errorf("remote_entity: mark store %d: %w; unlock: %v", w.id, err, unlockErr)
		}
		return nil, fmt.Errorf("remote_entity: mark store %d: %w", w.id, err)
	}
	w.markerMu.Lock()
	w.markerLease = lease
	w.markerMu.Unlock()

	// Update entity state
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.e != nil {
		oldExclude := w.e.ExcludeSId()
		oldVersion := w.e.EntityVersion()
		w.e.SetExcludeSId(0)
		w.e.SetEntityVersion(oldVersion + 1)
		if w.mgr.loader != nil {
			w.markerMu.RLock()
			saveLease := w.markerLease
			w.markerMu.RUnlock()
			if err := w.mgr.loader.SaveRemoteEntity(w.e, saveLease); err != nil {
				w.e.SetExcludeSId(oldExclude)
				w.e.SetEntityVersion(oldVersion)
				var rollbackErr error
				_, rollbackErr = w.mgr.markerStore.Unmark(ctx, w.id, lease)
				unlockErr := w.rMu.Unlock(ctx, w.rMu.Version(), w.mgr.cfg.VersionTTL)
				if rollbackErr != nil || unlockErr != nil {
					return nil, fmt.Errorf("remote_entity: mark save %d: %w; rollback marker: %v; unlock: %v", w.id, err, rollbackErr, unlockErr)
				}
				return nil, fmt.Errorf("remote_entity: mark save %d: %w", w.id, err)
			}
		}
	}

	w.SetMarked(true)
	return w.genReleaseFunc(), nil
}

// UnmarkRemote clears remote mark.
func (w *remoteEntityWrapper) UnmarkRemote(ctx context.Context) error {
	if w == nil || w.mgr == nil {
		return nil
	}
	if w.mgr.markerStore == nil {
		return fmt.Errorf("remote_entity: marker store is required for unmark %d", w.id)
	}
	w.ownershipMu.Lock()
	defer w.ownershipMu.Unlock()
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, w.mgr.cfg.OpTimeout)
		defer cancel()
	}
	if err := w.rMu.Lock(ctx); err != nil {
		return fmt.Errorf("remote_entity: unmark lock %d: %w", w.id, err)
	}
	unlockVersion := w.rMu.Version()
	unlocked := false
	defer func() {
		if !unlocked && w.rMu.IsAcquired() {
			releaseCtx, cancel := context.WithTimeout(fctx.BaseContext(), w.mgr.cfg.OpTimeout)
			defer cancel()
			if err := w.rMu.UnlockWithRetry(releaseCtx, unlockVersion, w.mgr.cfg.VersionTTL, w.mgr.cfg.UnlockRetryCount, w.mgr.cfg.UnlockRetryInterval); err != nil {
				slog.Error("remote_entity: unmark lock release failed", "id", w.id, "err", err)
			}
		}
	}()
	if err := w.refreshMarked(ctx); err != nil {
		return fmt.Errorf("remote_entity: refresh unmark %d: %w", w.id, err)
	}
	oldMarked := w.IsMarked()
	var err error
	w.markerMu.RLock()
	unmarkLease := w.markerLease
	w.markerMu.RUnlock()
	if !oldMarked || unmarkLease.OwnerSid != w.mgr.localSid {
		return fmt.Errorf("remote_entity: sid %d cannot unmark entity %d", w.mgr.localSid, w.id)
	}
	var nextLease entity.RemoteEntityMarkerLease
	nextLease, err = w.mgr.markerStore.Unmark(ctx, w.id, unmarkLease)
	if err != nil {
		return fmt.Errorf("remote_entity: unmark %d: %w", w.id, err)
	}
	w.markerMu.Lock()
	w.markerLease = nextLease
	w.markerMu.Unlock()
	w.SetMarked(false)

	w.mu.Lock()
	var syncSnap entity.RemoteSyncSnapshot
	if w.e != nil {
		oldExclude := w.e.ExcludeSId()
		oldVersion := w.e.EntityVersion()
		w.e.SetExcludeSId(w.mgr.localSid)
		nextVersion := oldVersion + 1
		w.e.SetEntityVersion(nextVersion)
		if w.mgr.loader != nil {
			w.markerMu.RLock()
			saveLease := w.markerLease
			w.markerMu.RUnlock()
			if err := w.mgr.loader.SaveRemoteEntity(w.e, saveLease); err != nil {
				w.e.SetExcludeSId(oldExclude)
				w.e.SetEntityVersion(oldVersion)
				if oldMarked && w.mgr.markerStore != nil {
					var rollbackErr error
					var rollbackLease entity.RemoteEntityMarkerLease
					rollbackLease, rollbackErr = w.mgr.markerStore.Mark(ctx, w.id, w.mgr.localSid)
					if rollbackErr == nil {
						w.markerMu.Lock()
						w.markerLease = rollbackLease
						w.markerMu.Unlock()
						w.SetMarked(true)
					}
					if rollbackErr != nil {
						w.mu.Unlock()
						unlockErr := w.rMu.UnlockWithRetry(ctx, unlockVersion, w.mgr.cfg.VersionTTL, w.mgr.cfg.UnlockRetryCount, w.mgr.cfg.UnlockRetryInterval)
						unlocked = unlockErr == nil
						return fmt.Errorf("remote_entity: unmark save %d: %w; rollback marker: %v; unlock: %v", w.id, err, rollbackErr, unlockErr)
					}
				}
				w.mu.Unlock()
				unlockErr := w.rMu.UnlockWithRetry(ctx, unlockVersion, w.mgr.cfg.VersionTTL, w.mgr.cfg.UnlockRetryCount, w.mgr.cfg.UnlockRetryInterval)
				unlocked = unlockErr == nil
				if unlockErr != nil {
					return fmt.Errorf("remote_entity: unmark save %d: %w; unlock: %v", w.id, err, unlockErr)
				}
				return fmt.Errorf("remote_entity: unmark save %d: %w", w.id, err)
			}
			syncSnap = w.mgr.loader.SnapshotRemoteEntitySync(w.e)
		}
		unlockVersion = nextVersion
	}
	w.mu.Unlock()
	syncRemoteItems(w.mgr, w.id, unlockVersion, syncSnap)
	if err := w.rMu.UnlockWithRetry(ctx, unlockVersion, w.mgr.cfg.VersionTTL, w.mgr.cfg.UnlockRetryCount, w.mgr.cfg.UnlockRetryInterval); err != nil {
		return fmt.Errorf("remote_entity: unmark unlock %d: %w", w.id, err)
	}
	unlocked = true
	return nil
}

func (w *remoteEntityWrapper) IsMarked() bool {
	return w != nil && w.marker.Load() == markerShared
}

func (w *remoteEntityWrapper) SetMarked(v bool) {
	if w == nil {
		return
	}
	if v {
		w.marker.Store(markerShared)
	} else {
		w.marker.Store(markerLocal)
	}
	w.markerAt.Store(time.Now().UnixNano())
}

func (w *remoteEntityWrapper) Entity() entity.IThreadSafeRemoteEntity { return w.e }

func (w *remoteEntityWrapper) ensureLoadedForReplica() {
	if w == nil || w.mgr == nil || w.mgr.loader == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.e != nil {
		return
	}
	if loaded := w.mgr.loader.LoadRemoteEntity(w.id, w.kind); loaded != nil {
		w.e = loaded
		w.e.SetExcludeSId(w.localExcludeSid())
	}
}

// EvictEntity removes local entity from memory.
func (w *remoteEntityWrapper) EvictEntity() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.e != nil {
		entity.Mgr.Remove(w.e, entity.DestroyReasonCommon, false)
		w.e = nil
	}
	w.mgr.Remove(w.id)
}

// TryUpdateEntity applies sync data from another server.
func (w *remoteEntityWrapper) TryUpdateEntity(version int64, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.e != nil {
		cVersion := w.e.EntityVersion()
		if version < cVersion {
			return nil
		}
		if version > cVersion {
			w.e.SetEntityVersion(version)
		}
		payload, err := entity.DecodeRemoteSyncPayload(data)
		if err != nil {
			return fmt.Errorf("remote_entity: decode sync payload %d: %w", w.id, err)
		}
		if applier, ok := w.e.(entity.RemoteSyncApplier); ok {
			if err := applier.ApplyRemoteSync(payload.Collection, payload.Data, version); err != nil {
				return err
			}
		} else {
			w.e.OnDataChange(payload.Data, version)
		}
		return nil
	}
	return fmt.Errorf("remote_entity: no entity held for %d", w.id)
}

// TryDelEntity applies remote deletion notification.
func (w *remoteEntityWrapper) TryDelEntity() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.e != nil {
		entity.Mgr.Remove(w.e, entity.DestroyReasonCommon, false)
		w.e = nil
		w.mgr.Remove(w.id)
		return nil
	}
	return fmt.Errorf("remote_entity: no entity held for %d", w.id)
}

// genLockReleaseFunc only releases a lock acquired before an operation failed.
func (w *remoteEntityWrapper) genLockReleaseFunc() func() {
	return func() {
		if !w.rMu.IsAcquired() {
			return
		}
		ctx, cancel := context.WithTimeout(fctx.BaseContext(), w.mgr.cfg.OpTimeout)
		defer cancel()
		if err := w.rMu.UnlockWithRetry(ctx, w.rMu.Version(), w.mgr.cfg.VersionTTL, w.mgr.cfg.UnlockRetryCount, w.mgr.cfg.UnlockRetryInterval); err != nil {
			slog.Error("remote_entity: stale lock release failed", "id", w.id, "err", err)
		}
	}
}

// genReleaseFunc creates the release function: save → sync → unlock.
func (w *remoteEntityWrapper) genReleaseFunc() func() {
	id := w.id
	e := w.e
	mu := w.mu
	rMu := w.rMu
	mgr := w.mgr
	w.markerMu.RLock()
	lease := w.markerLease
	w.markerMu.RUnlock()

	return func() {
		needUnlock := rMu.IsAcquired()
		var eVersion int64

		if e != nil {
			mu.Lock()
			defer mu.Unlock()

			eVersion = e.EntityVersion()

			if e.IsRemoved() {
				// Entity was deleted during dispatch
				deleteOK := true
				if mgr.loader != nil {
					if err := mgr.loader.DelRemoteEntity(e, lease); err != nil {
						slog.Warn("remote_entity: delete failed, skip del sync",
							"id", id, "version", eVersion, "err", err)
						deleteOK = false
					}
				}
				if deleteOK {
					if needUnlock {
						syncRemoteDelete(mgr, id, eVersion)
					}
					mgr.Remove(id)
				}
			} else if hasEntityDirty(e) {
				if needUnlock {
					nextVersion := eVersion + 1
					e.SetEntityVersion(nextVersion)
					saveOK := true
					if mgr.loader != nil {
						if err := mgr.loader.SaveRemoteEntity(e, lease); err != nil {
							e.SetEntityVersion(eVersion)
							saveOK = false
							slog.Warn("remote_entity: save failed, skip sync",
								"id", id, "version", nextVersion, "err", err)
						}
					}
					if saveOK {
						eVersion = nextVersion
						syncSnap := entity.RemoteSyncSnapshot{}
						if mgr.loader != nil {
							syncSnap = mgr.loader.SnapshotRemoteEntitySync(e)
						}
						syncRemoteItems(mgr, id, eVersion, syncSnap)
					}
				} else {
					if mgr.loader != nil {
						if err := mgr.loader.SaveRemoteEntity(e, lease); err != nil {
							slog.Warn("remote_entity: save failed",
								"id", id, "version", eVersion, "err", err)
						}
					}
				}
			}
		} else {
			eVersion = rMu.Version()
		}

		if needUnlock {
			ctx, cancel := context.WithTimeout(fctx.BaseContext(), mgr.cfg.OpTimeout)
			defer cancel()
			_ = rMu.UnlockWithRetry(ctx, eVersion, mgr.cfg.VersionTTL, mgr.cfg.UnlockRetryCount, mgr.cfg.UnlockRetryInterval)
		}
	}
}

func (w *remoteEntityWrapper) releaseWithOwnership(release func(), unlock func()) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			if release != nil {
				release()
			}
			unlock()
		})
	}
}

func hasEntityDirty(e entity.IThreadSafeRemoteEntity) bool {
	if e == nil {
		return false
	}
	guardable, ok := e.(entity.Guardable)
	if !ok {
		return false
	}
	dirty := false
	guardable.RangeDao(func(dao entity.DaoInterface) {
		if dirty {
			return
		}
		if d := dao.Dirty(); d != nil && d.Dirty() {
			dirty = true
		}
	})
	return dirty
}

func syncRemoteItems(mgr *remoteEntityManager, id int64, version int64, snap entity.RemoteSyncSnapshot) {
	for _, item := range snap.Items {
		if len(item.Data) == 0 {
			if item.Commit != nil {
				item.Commit()
			}
			continue
		}
		if mgr.syncer == nil || mgr.syncer.SyncEntity(id, version, item.Collection, item.Data) != nil {
			if mgr.syncRetry != nil {
				mgr.syncRetry.Enqueue(remoteSyncRetryItem{
					id:         id,
					version:    version,
					collection: item.Collection,
					data:       item.Data,
				})
			}
			if item.Rollback != nil {
				item.Rollback()
			}
			continue
		}
		if item.Commit != nil {
			item.Commit()
		}
	}
}

func syncRemoteDelete(mgr *remoteEntityManager, id int64, version int64) {
	if mgr.syncer != nil {
		if err := mgr.syncer.SyncDelEntity(id, version); err == nil {
			return
		}
	}
	if mgr.syncRetry != nil {
		mgr.syncRetry.Enqueue(remoteSyncRetryItem{
			id:      id,
			version: version,
			del:     true,
		})
	}
}

// getLocalEntity retrieves entity from EntityManager and asserts IThreadSafeRemoteEntity.
func (w *remoteEntityWrapper) getLocalEntity() entity.IThreadSafeRemoteEntity {
	if entity.Mgr == nil {
		return nil
	}
	meta := entity.ResolveEntityID(w.id)
	e := entity.Mgr.Get(meta.FullID)
	if e == nil {
		return nil
	}
	re, ok := e.(entity.IThreadSafeRemoteEntity)
	if !ok {
		return nil
	}
	return re
}

func (w *remoteEntityWrapper) refreshMarked(ctx context.Context) error {
	if w.mgr.markerStore == nil {
		if w.marker.Load() == markerUnknown {
			w.marker.Store(markerLocal)
		}
		w.markerAt.Store(time.Now().UnixNano())
		return nil
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, w.mgr.cfg.OpTimeout)
		defer cancel()
	}
	marked := false
	var lease entity.RemoteEntityMarkerLease
	var err error
	marked, lease, err = w.mgr.markerStore.GetMarker(ctx, w.id)
	if err != nil {
		return err
	}
	lease.Shared = marked
	w.markerMu.Lock()
	w.markerLease = lease
	w.markerMu.Unlock()
	w.SetMarked(marked)
	return nil
}

func (w *remoteEntityWrapper) ensureMarker(ctx context.Context) error {
	const markerCacheTTL = time.Second
	if w.marker.Load() != markerUnknown && time.Since(time.Unix(0, w.markerAt.Load())) < markerCacheTTL {
		return nil
	}
	return w.refreshMarked(ctx)
}

func (w *remoteEntityWrapper) markerKnown() bool {
	return w.marker.Load() != markerUnknown
}

func (w *remoteEntityWrapper) localExcludeSid() int32 {
	if w.IsMarked() {
		return 0
	}
	return w.mgr.localSid
}

func (w *remoteEntityWrapper) leaseOwner() int32 {
	w.markerMu.RLock()
	defer w.markerMu.RUnlock()
	return w.markerLease.OwnerSid
}

func (w *remoteEntityWrapper) isLocalOwner() bool {
	owner := w.leaseOwner()
	return owner == 0 || owner == w.mgr.localSid
}

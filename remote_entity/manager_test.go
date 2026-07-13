package remote_entity

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/obs"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
	"errors"
	"sync"
	"testing"
	"time"
)

// --- Mock Versioned Lock ---

type mockVersionedLock struct {
	mu       sync.Mutex
	acquired bool
	version  int64
	locks    int
}

func (l *mockVersionedLock) TryLock(_ context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.acquired {
		return ErrVersionedLockNotAcquired
	}
	l.acquired = true
	l.locks++
	return nil
}

func (l *mockVersionedLock) Lock(ctx context.Context) error {
	return l.TryLock(ctx)
}

func (l *mockVersionedLock) Unlock(_ context.Context, newVersion int64, _ time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.acquired {
		return ErrVersionedLockNotOwned
	}
	l.acquired = false
	l.version = newVersion
	return nil
}

func (l *mockVersionedLock) UnlockWithRetry(ctx context.Context, newVersion int64, versionTTL time.Duration, _ int, _ time.Duration) error {
	return l.Unlock(ctx, newVersion, versionTTL)
}

func (l *mockVersionedLock) Version() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.version
}

func (l *mockVersionedLock) IsAcquired() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquired
}

func (l *mockVersionedLock) LockCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.locks
}

func (l *mockVersionedLock) Touch(_ context.Context, _ time.Duration) error { return nil }
func (l *mockVersionedLock) Refresh(_ context.Context) error                { return nil }
func (l *mockVersionedLock) Close() error                                   { return nil }

// --- Mock Lock Factory ---

type mockVersionedLockFactory struct {
	mu    sync.Mutex
	locks map[int64]*mockVersionedLock
}

func newMockVersionedLockFactory() *mockVersionedLockFactory {
	return &mockVersionedLockFactory{locks: make(map[int64]*mockVersionedLock)}
}

func (f *mockVersionedLockFactory) NewVersionedLock(id int64, _ fredis.VersionedLockOptions) fredis.IVersionedLock {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := &mockVersionedLock{}
	f.locks[id] = l
	return l
}

// --- Mock Loader ---

type mockLoader struct {
	mu       sync.Mutex
	loaded   []int64
	saved    []int64
	deleted  []int64
	loadErr  bool
	saveErr  error
	delErr   error
	entities map[int64]entity.IThreadSafeRemoteEntity
}

func newMockLoader() *mockLoader {
	return &mockLoader{entities: make(map[int64]entity.IThreadSafeRemoteEntity)}
}

func (l *mockLoader) LoadRemoteEntity(id int64, _ entity.EntityKind) entity.IThreadSafeRemoteEntity {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.loadErr {
		return nil
	}
	l.loaded = append(l.loaded, id)
	e := l.entities[id]
	return e
}

func (l *mockLoader) SaveRemoteEntity(e entity.IThreadSafeRemoteEntity) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.saveErr != nil {
		return l.saveErr
	}
	l.saved = append(l.saved, e.ID())
	return nil
}

func (l *mockLoader) SnapshotRemoteEntitySync(_ entity.IThreadSafeRemoteEntity) entity.RemoteSyncSnapshot {
	return entity.RemoteSyncSnapshot{Items: []entity.RemoteSyncItem{{Collection: "test", Data: []byte("sync")}}}
}

func (l *mockLoader) DelRemoteEntity(e entity.IThreadSafeRemoteEntity) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.delErr != nil {
		return l.delErr
	}
	l.deleted = append(l.deleted, e.ID())
	return nil
}

func (l *mockLoader) CheckEntityExist(_ int64, _ entity.EntityKind) bool {
	return true
}

// --- Mock Marker Store ---

type mockMarkerStore struct {
	mu    sync.Mutex
	marks map[int64]bool
	err   error
}

func newMockMarkerStore() *mockMarkerStore {
	return &mockMarkerStore{marks: make(map[int64]bool)}
}

func (s *mockMarkerStore) IsMarked(_ context.Context, id int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return false, s.err
	}
	return s.marks[id], nil
}

func (s *mockMarkerStore) Mark(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marks[id] = true
	return nil
}

func (s *mockMarkerStore) Unmark(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	delete(s.marks, id)
	return nil
}

// --- Mock Syncer ---

type mockSyncer struct {
	mu            sync.Mutex
	synced        []int64
	deleted       []int64
	failSyncCount int
	failDelCount  int
	syncErr       error
	delErr        error
}

func (s *mockSyncer) SyncEntity(id int64, _ int64, _ string, _ []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSyncCount > 0 {
		s.failSyncCount--
		if s.syncErr != nil {
			return s.syncErr
		}
		return errors.New("sync failed")
	}
	s.synced = append(s.synced, id)
	return nil
}

func (s *mockSyncer) SyncDelEntity(id int64, _ int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failDelCount > 0 {
		s.failDelCount--
		if s.delErr != nil {
			return s.delErr
		}
		return errors.New("delete sync failed")
	}
	s.deleted = append(s.deleted, id)
	return nil
}

type testDirty struct {
	dirty bool
}

func (d *testDirty) Dirty() bool { return d.dirty }
func (d *testDirty) SelfClean()  { d.dirty = false }

type testRemoteEntity struct {
	entity.RemoteEntityBase
	dirty testDirty
	dao   testRemoteDao
}

type testRemoteDao struct {
	id    int64
	dirty *testDirty
}

func newTestRemoteEntity(id int64, category entity.EntityCategory, kind entity.EntityKind) *testRemoteEntity {
	e := &testRemoteEntity{}
	e.RemoteEntityBase.EntityBase = *entity.NewEntityBase(testRemoteFullIDWithKind(id, category, kind), category, false, kind)
	e.dao = testRemoteDao{id: e.ID(), dirty: &e.dirty}
	return e
}

func testRemoteFullID(id int64) int64 {
	return testRemoteFullIDWithKind(id, 1, 1)
}

func testRemoteFullIDWithKind(id int64, category entity.EntityCategory, kind entity.EntityKind) int64 {
	entity.MustRegisterEntityKindCategory(kind, category)
	fullID, err := entity.BuildEntityID(id, kind)
	if err != nil {
		panic(err)
	}
	return fullID
}

func (e *testRemoteEntity) Base() *entity.EntityBase       { return &e.RemoteEntityBase.EntityBase }
func (e *testRemoteEntity) OnDataChange(_ []byte, _ int64) {}
func (e *testRemoteEntity) RemoteSnapshot(scope uint64) (any, bool) {
	return map[string]any{
		"id":      e.ID(),
		"scope":   scope,
		"version": e.EntityVersion(),
	}, true
}
func (e *testRemoteEntity) RangeDao(fn func(entity.DaoInterface)) {
	if fn != nil {
		fn(&e.dao)
	}
}

func (d *testRemoteDao) Id() int64            { return d.id }
func (d *testRemoteDao) SetId(id int64)       { d.id = id }
func (d *testRemoteDao) DbName() string       { return "test" }
func (d *testRemoteDao) CollName() string     { return "remote" }
func (d *testRemoteDao) Dirty() entity.IDirty { return d.dirty }
func (d *testRemoteDao) CleanDirty()          { d.dirty.SelfClean() }

// --- Tests ---

func TestManager_GetOrCreate(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)

	fullID1 := testRemoteFullID(100)
	fullID2 := testRemoteFullID(200)
	w1 := mgr.GetOrCreate(fullID1, 1, 1)
	w2 := mgr.GetOrCreate(fullID1, 1, 1)
	if w1 != w2 {
		t.Error("GetOrCreate should return same wrapper for same ID")
	}

	w3 := mgr.GetOrCreate(fullID2, 1, 1)
	if w1 == w3 {
		t.Error("GetOrCreate should return different wrapper for different ID")
	}
}

func TestManager_IsMarkedReadsMarkerStoreWithoutWrapper(t *testing.T) {
	factory := newMockVersionedLockFactory()
	mgr := newRemoteEntityManager(factory, DefaultConfig(), 1000)
	marker := newMockMarkerStore()
	mgr.SetMarkerStore(marker)
	kind := entity.EntityKind(71)
	category := entity.EntityCategory(1)
	entity.MustRegisterEntityKindDefs(entity.EntityKindDef{
		Kind:         kind,
		Category:     category,
		RemotePolicy: entity.RemotePolicyManaged,
	})
	fullID := testRemoteFullIDWithKind(7100, category, kind)
	if err := marker.Mark(context.Background(), fullID); err != nil {
		t.Fatal(err)
	}

	entity.BindRemoteEntityManager(mgr)
	t.Cleanup(func() { entity.UnbindRemoteEntityManager(mgr) })

	if !entity.IsRemoteMarkedEntityID(fullID) {
		t.Fatal("remote mark should be read from marker store even when wrapper is not cached")
	}
}

func TestWrapper_UnmarkRemotePersistsAndSyncs(t *testing.T) {
	factory := newMockVersionedLockFactory()
	mgr := newRemoteEntityManager(factory, DefaultConfig(), 3001)
	loader := newMockLoader()
	marker := newMockMarkerStore()
	syncer := &mockSyncer{}
	mgr.SetLoader(loader)
	mgr.SetMarkerStore(marker)
	mgr.SetSyncer(syncer)

	fullID := testRemoteFullID(900)
	e := newTestRemoteEntity(900, 1, 1)
	e.SetExcludeSId(0)
	e.SetEntityVersion(4)
	loader.entities[fullID] = e
	if err := marker.Mark(context.Background(), fullID); err != nil {
		t.Fatal(err)
	}
	w := mgr.GetOrCreate(fullID, 1, 1).(*remoteEntityWrapper)
	w.e = e
	w.SetMarked(true)

	if err := w.UnmarkRemote(context.Background()); err != nil {
		t.Fatalf("UnmarkRemote error: %v", err)
	}
	if marked, err := marker.IsMarked(context.Background(), fullID); err != nil || marked {
		t.Fatalf("marker marked=%v err=%v, want false", marked, err)
	}
	if e.ExcludeSId() != 3001 {
		t.Fatalf("ExcludeSId=%d, want local sid", e.ExcludeSId())
	}
	if e.EntityVersion() != 5 {
		t.Fatalf("EntityVersion=%d, want 5", e.EntityVersion())
	}
	if len(loader.saved) != 1 || loader.saved[0] != fullID {
		t.Fatalf("saved=%v, want [%d]", loader.saved, fullID)
	}
	if len(syncer.synced) != 1 || syncer.synced[0] != fullID {
		t.Fatalf("synced=%v, want [%d]", syncer.synced, fullID)
	}
}

func TestManager_Get(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)

	_, ok := mgr.Get(100)
	if ok {
		t.Error("Get should return false for non-existent ID")
	}

	fullID := testRemoteFullID(100)
	mgr.GetOrCreate(fullID, 1, 1)
	_, ok = mgr.Get(fullID)
	if !ok {
		t.Error("Get should return true after GetOrCreate")
	}
}

func TestManager_Remove(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)

	fullID := testRemoteFullID(100)
	mgr.GetOrCreate(fullID, 1, 1)
	mgr.Remove(fullID)
	_, ok := mgr.Get(fullID)
	if ok {
		t.Error("Get should return false after Remove")
	}
}

func TestManager_SetLoader(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)

	if mgr.Loader() != nil {
		t.Error("Loader should be nil initially")
	}
	loader := newMockLoader()
	mgr.SetLoader(loader)
	if mgr.Loader() != loader {
		t.Error("Loader not set correctly")
	}
}

func TestManager_SetMarkerStore(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)

	store := newMockMarkerStore()
	mgr.SetMarkerStore(store)
	if mgr.MarkerStore() != store {
		t.Error("MarkerStore not set correctly")
	}
}

func TestManager_SetSyncer(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)

	syncer := &mockSyncer{}
	mgr.SetSyncer(syncer)
	if mgr.Syncer() != syncer {
		t.Error("Syncer not set correctly")
	}
}

func TestWrapperRelease_SaveFailureSkipsSyncAndKeepsVersion(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)
	loader := newMockLoader()
	loader.saveErr = errors.New("save failed")
	syncer := &mockSyncer{}
	mgr.SetLoader(loader)
	mgr.SetSyncer(syncer)

	const id int64 = 100
	fullID := testRemoteFullID(id)
	w := mgr.GetOrCreate(fullID, 1, 1).(*remoteEntityWrapper)
	e := newTestRemoteEntity(id, 1, 1)
	e.SetEntityVersion(7)
	e.dirty.dirty = true
	w.e = e

	if err := w.rMu.Lock(context.Background()); err != nil {
		t.Fatalf("lock failed: %v", err)
	}
	w.genReleaseFunc()()

	if e.EntityVersion() != 7 {
		t.Fatalf("version changed after failed save: got %d", e.EntityVersion())
	}
	if w.rMu.Version() != 7 {
		t.Fatalf("lock version changed after failed save: got %d", w.rMu.Version())
	}
	if len(syncer.synced) != 0 {
		t.Fatalf("sync should be skipped after failed save, got %d syncs", len(syncer.synced))
	}
}

func TestWrapperRelease_SaveSuccessSyncs(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)
	loader := newMockLoader()
	syncer := &mockSyncer{}
	mgr.SetLoader(loader)
	mgr.SetSyncer(syncer)

	const id int64 = 101
	fullID := testRemoteFullID(id)
	w := mgr.GetOrCreate(fullID, 1, 1).(*remoteEntityWrapper)
	e := newTestRemoteEntity(id, 1, 1)
	e.SetEntityVersion(3)
	e.dirty.dirty = true
	w.e = e

	if err := w.rMu.Lock(context.Background()); err != nil {
		t.Fatalf("lock failed: %v", err)
	}
	w.genReleaseFunc()()

	if e.EntityVersion() != 4 {
		t.Fatalf("version after save: got %d, want 4", e.EntityVersion())
	}
	if w.rMu.Version() != 4 {
		t.Fatalf("lock version after save: got %d, want 4", w.rMu.Version())
	}
	if len(syncer.synced) != 1 {
		t.Fatalf("sync count: got %d, want 1", len(syncer.synced))
	}
}

func TestPrepareRemoteEntities_RefreshesMarkerStore(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)
	loader := newMockLoader()
	store := newMockMarkerStore()
	mgr.SetLoader(loader)
	mgr.SetMarkerStore(store)

	const id int64 = 102
	fullID := testRemoteFullID(id)
	e := newTestRemoteEntity(id, 1, 1)
	loader.entities[fullID] = e

	w := mgr.GetOrCreate(fullID, 1, 1)
	if w.IsMarked() {
		t.Fatal("wrapper should start unmarked")
	}
	store.marks[fullID] = true

	release, err := mgr.PrepareRemoteEntities([]int64{fullID})
	if err != nil {
		t.Fatalf("PrepareRemoteEntities failed: %v", err)
	}
	release()

	if !w.IsMarked() {
		t.Fatal("wrapper should refresh marked state from store")
	}
	if len(loader.loaded) != 1 {
		t.Fatalf("remote path should load entity, got %d loads", len(loader.loaded))
	}
}

func TestPrepareRemoteEntities_AlignsLoadedEntityVersion(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)
	loader := newMockLoader()
	store := newMockMarkerStore()
	mgr.SetLoader(loader)
	mgr.SetMarkerStore(store)

	const id int64 = 104
	const version int64 = 12
	fullID := testRemoteFullID(id)
	e := newTestRemoteEntity(id, 1, 1)
	loader.entities[fullID] = e
	store.marks[fullID] = true

	w := mgr.GetOrCreate(fullID, 1, 1).(*remoteEntityWrapper)
	w.rMu.(*mockVersionedLock).version = version

	release, err := mgr.PrepareRemoteEntities([]int64{fullID})
	if err != nil {
		t.Fatalf("PrepareRemoteEntities failed: %v", err)
	}
	release()

	if e.EntityVersion() != version {
		t.Fatalf("loaded entity version: got %d, want %d", e.EntityVersion(), version)
	}
}

func TestRemoteSyncRetryPublishesAfterInitialFailure(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	cfg.SyncRetryInterval = time.Millisecond
	cfg.SyncRetryQueueCap = 4
	cfg.SyncRetryMaxAttempts = 3
	mgr := newRemoteEntityManager(factory, cfg, 1000)
	syncer := &mockSyncer{failSyncCount: 1}
	mgr.SetSyncer(syncer)
	mgr.startSyncRetry()
	defer mgr.stopSyncRetry()

	rolledBack := make(chan struct{}, 1)
	syncRemoteItems(mgr, 105, 9, entity.RemoteSyncSnapshot{Items: []entity.RemoteSyncItem{{
		Collection: "test",
		Data:       []byte("payload"),
		Rollback: func() {
			rolledBack <- struct{}{}
		},
	}}})

	select {
	case <-rolledBack:
	case <-time.After(time.Second):
		t.Fatal("expected dirty rollback after initial sync failure")
	}

	deadline := time.After(time.Second)
	for {
		syncer.mu.Lock()
		got := len(syncer.synced)
		syncer.mu.Unlock()
		if got == 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("expected retry sync success, got %d", got)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestRemoteSyncRetryStopWithContextCancelsInFlightPublish(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	cfg.SyncRetryQueueCap = 1
	cfg.SyncRetryInterval = time.Hour
	mgr := newRemoteEntityManager(factory, cfg, 1000)
	syncer := &blockingContextSyncer{started: make(chan struct{}), cancelled: make(chan struct{})}
	mgr.SetSyncer(syncer)
	mgr.startSyncRetry()
	if !mgr.syncRetry.Enqueue(remoteSyncRetryItem{id: 106, version: 1, collection: "test", data: []byte("payload")}) {
		t.Fatal("Enqueue returned false")
	}
	<-syncer.started

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := mgr.stopSyncRetryWithContext(stopCtx); err != nil {
		t.Fatalf("stopSyncRetryWithContext: %v", err)
	}
	select {
	case <-syncer.cancelled:
	default:
		t.Fatal("in-flight publish context was not cancelled")
	}
}

type blockingContextSyncer struct {
	started   chan struct{}
	cancelled chan struct{}
}

func (s *blockingContextSyncer) SyncEntity(id int64, version int64, collection string, data []byte) error {
	return s.SyncEntityWithContext(context.Background(), id, version, collection, data)
}

func (s *blockingContextSyncer) SyncDelEntity(id int64, version int64) error {
	return s.SyncDelEntityWithContext(context.Background(), id, version)
}

func (s *blockingContextSyncer) SyncEntityWithContext(ctx context.Context, _ int64, _ int64, _ string, _ []byte) error {
	close(s.started)
	<-ctx.Done()
	close(s.cancelled)
	return ctx.Err()
}

func (s *blockingContextSyncer) SyncDelEntityWithContext(ctx context.Context, _ int64, _ int64) error {
	close(s.started)
	<-ctx.Done()
	close(s.cancelled)
	return ctx.Err()
}

func TestWrapper_TryReadOnlyEntityLoadsWithoutWriteLockOrSave(t *testing.T) {
	factory := newMockVersionedLockFactory()
	mgr := newRemoteEntityManager(factory, DefaultConfig(), 1000)
	loader := newMockLoader()
	rawID := int64(1201)
	id := testRemoteFullIDWithKind(rawID, 1, 1)
	loaded := newTestRemoteEntity(rawID, 1, 1)
	loaded.SetEntityVersion(9)
	loader.entities[id] = loaded
	mgr.SetLoader(loader)

	w := mgr.GetOrCreate(id, 1, 1)
	got, release, err := w.TryReadOnlyEntity(entity.RemoteReadOption{MinVersion: 9})
	if err != nil {
		t.Fatalf("read-only load error: %v", err)
	}
	if got == nil || got.ID() != id {
		t.Fatalf("read-only entity = %#v, want id %d", got, id)
	}
	if release != nil {
		release()
	}
	lock := factory.locks[id]
	if lock == nil {
		t.Fatalf("expected wrapper lock to be allocated")
	}
	if lock.LockCount() != 0 {
		t.Fatalf("read-only load should not acquire write lock, got %d locks", lock.LockCount())
	}
	if len(loader.saved) != 0 {
		t.Fatalf("read-only release should not save entity, saved=%v", loader.saved)
	}
}

func TestWrapper_TryReadOnlySnapshotLoadsWithoutReturningMutableEntity(t *testing.T) {
	factory := newMockVersionedLockFactory()
	mgr := newRemoteEntityManager(factory, DefaultConfig(), 1000)
	loader := newMockLoader()
	rawID := int64(1211)
	id := testRemoteFullIDWithKind(rawID, 1, 1)
	loaded := newTestRemoteEntity(rawID, 1, 1)
	loaded.SetEntityVersion(9)
	loader.entities[id] = loaded
	mgr.SetLoader(loader)

	w := mgr.GetOrCreate(id, 1, 1)
	snapshot, err := w.TryReadOnlySnapshot(entity.RemoteSnapshotRequest{
		Scope: 3,
		Option: entity.RemoteReadOption{
			MinVersion: 9,
		},
	})
	if err != nil {
		t.Fatalf("read-only snapshot error: %v", err)
	}
	if snapshot.EntityID != id || snapshot.Scope != 3 || snapshot.Version != 9 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if snapshot.Source != entity.RemoteSnapshotSourceLoaded {
		t.Fatalf("snapshot source = %v, want loaded", snapshot.Source)
	}
	data, ok := snapshot.Data.(map[string]any)
	if !ok || data["scope"] != uint64(3) {
		t.Fatalf("snapshot data = %#v", snapshot.Data)
	}
	lock := factory.locks[id]
	if lock == nil {
		t.Fatalf("expected wrapper lock to be allocated")
	}
	if lock.LockCount() != 0 {
		t.Fatalf("read-only snapshot should not acquire write lock, got %d locks", lock.LockCount())
	}
	if len(loader.saved) != 0 {
		t.Fatalf("read-only snapshot should not save entity, saved=%v", loader.saved)
	}
}

func TestWrapper_TryCachedSnapshotUsesLocalMaterialization(t *testing.T) {
	factory := newMockVersionedLockFactory()
	mgr := newRemoteEntityManager(factory, DefaultConfig(), 1000)
	loader := newMockLoader()
	rawID := int64(1212)
	id := testRemoteFullIDWithKind(rawID, 1, 1)
	loader.entities[id] = newTestRemoteEntity(rawID, 1, 1)
	mgr.SetLoader(loader)

	entity.Mgr = entity.NewEntityManager()
	local := newTestRemoteEntity(rawID, 1, 1)
	local.SetEntityVersion(11)
	entity.Mgr.Add(local)
	t.Cleanup(func() { entity.Mgr = nil })

	w := mgr.GetOrCreate(id, 1, 1)
	snapshot, err := w.TryCachedSnapshot(entity.RemoteSnapshotRequest{
		Scope: 4,
		Option: entity.RemoteReadOption{
			MinVersion:     11,
			CacheTTLMillis: 50,
			NowMillis:      100,
		},
	})
	if err != nil {
		t.Fatalf("cached snapshot error: %v", err)
	}
	if snapshot.Version != 11 || snapshot.Scope != 4 {
		t.Fatalf("cached snapshot = %+v", snapshot)
	}
	if snapshot.Source != entity.RemoteSnapshotSourceLocal || snapshot.ReadAt != 100 || snapshot.ExpiresAt != 150 {
		t.Fatalf("cached snapshot cache metadata = source:%v read:%d expire:%d, want local/100/150", snapshot.Source, snapshot.ReadAt, snapshot.ExpiresAt)
	}
	if len(loader.loaded) != 0 {
		t.Fatalf("cached snapshot should use local materialization first, loaded=%v", loader.loaded)
	}
}

func TestManagerResolveRemoteSnapshotRecordsResultMetrics(t *testing.T) {
	obs.DefaultRegistry().Reset()
	t.Cleanup(func() { obs.DefaultRegistry().Reset() })

	factory := newMockVersionedLockFactory()
	mgr := newRemoteEntityManager(factory, DefaultConfig(), 1000)
	loader := newMockLoader()
	rawID := int64(1213)
	id := testRemoteFullIDWithKind(rawID, 1, 1)
	loaded := newTestRemoteEntity(rawID, 1, 1)
	loaded.SetEntityVersion(12)
	loader.entities[id] = loaded
	mgr.SetLoader(loader)

	ref, err := entity.NewRemoteViewRef(id, 1, 12)
	if err != nil {
		t.Fatalf("remote ref: %v", err)
	}
	snapshot, err := mgr.ResolveRemoteSnapshot(entity.RemoteSnapshotResolveRequest{
		Ref:   ref,
		Mode:  entity.RemoteAcquireCache,
		Scope: 5,
		Option: entity.RemoteReadOption{
			MinVersion:     12,
			CacheTTLMillis: 20,
			NowMillis:      200,
		},
	})
	if err != nil {
		t.Fatalf("resolve snapshot: %v", err)
	}
	if snapshot.Source != entity.RemoteSnapshotSourceLoaded || snapshot.ExpiresAt != 220 {
		t.Fatalf("snapshot metadata = source:%v expires:%d, want loaded/220", snapshot.Source, snapshot.ExpiresAt)
	}
	found := false
	for _, metric := range obs.Snapshot() {
		if metric.Name == "remote_entity.snapshot.resolve_total" &&
			metric.Labels["mode"] == "cache" &&
			metric.Labels["source"] == "loaded" &&
			metric.Labels["result"] == "ok" &&
			metric.Value == 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing remote snapshot resolve metric: %+v", obs.Snapshot())
	}
}

func TestWrapperRelease_DeleteFailureSkipsSyncAndKeepsWrapper(t *testing.T) {
	factory := newMockVersionedLockFactory()
	cfg := DefaultConfig()
	mgr := newRemoteEntityManager(factory, cfg, 1000)
	loader := newMockLoader()
	loader.delErr = errors.New("delete failed")
	syncer := &mockSyncer{}
	mgr.SetLoader(loader)
	mgr.SetSyncer(syncer)

	const id int64 = 103
	fullID := testRemoteFullID(id)
	w := mgr.GetOrCreate(fullID, 1, 1).(*remoteEntityWrapper)
	e := newTestRemoteEntity(id, 1, 1)
	e.SetEntityVersion(5)
	e.SetRemoved()
	w.e = e

	if err := w.rMu.Lock(context.Background()); err != nil {
		t.Fatalf("lock failed: %v", err)
	}
	w.genReleaseFunc()()

	if len(syncer.deleted) != 0 {
		t.Fatalf("delete sync should be skipped after delete failure, got %d", len(syncer.deleted))
	}
	if _, ok := mgr.Get(fullID); !ok {
		t.Fatal("wrapper should remain for retry after delete failure")
	}
}

func TestVersionedLockFactory(t *testing.T) {
	factory := newMockVersionedLockFactory()
	lock := factory.NewVersionedLock(42, fredis.DefaultVersionedLockOptions("e", 30*time.Second))
	if lock == nil {
		t.Fatal("NewVersionedLock returned nil")
	}

	// Should acquire
	err := lock.Lock(context.Background())
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}
	if !lock.IsAcquired() {
		t.Error("Lock should be acquired")
	}

	// Should unlock
	err = lock.Unlock(context.Background(), 5, time.Hour)
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
	if lock.IsAcquired() {
		t.Error("Lock should not be acquired after unlock")
	}
	if lock.Version() != 5 {
		t.Errorf("Version should be 5, got %d", lock.Version())
	}
}

func TestVersionedLock_Errors(t *testing.T) {
	factory := newMockVersionedLockFactory()
	lock := factory.NewVersionedLock(1, fredis.DefaultVersionedLockOptions("e", time.Second))

	// Unlock without acquire
	err := lock.Unlock(context.Background(), 0, 0)
	if !errors.Is(err, ErrVersionedLockNotOwned) {
		t.Errorf("expected ErrVersionedLockNotOwned, got %v", err)
	}

	// Double acquire
	_ = lock.Lock(context.Background())
	err = lock.Lock(context.Background())
	if err == nil {
		t.Error("expected error on double acquire")
	}
}

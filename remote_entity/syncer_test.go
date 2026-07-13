package remote_entity

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/replica"
	fsync "github.com/tjbdwanghaibo/cube-core/sync"
	"testing"
)

func TestRemoteSyncerUsesReplicaStore(t *testing.T) {
	bus := newRemoteSyncFakeBus()
	mgr := newRemoteEntityManager(newMockVersionedLockFactory(), DefaultConfig(), 1)
	fullID := testRemoteFullID(100)
	w := mgr.GetOrCreate(fullID, 1, 1).(*remoteEntityWrapper)
	e := newTestRemoteEntity(100, 1, 1)
	e.SetEntityVersion(1)
	w.e = e

	rep := replica.New(bus, syncTopicRemoteEntity, remoteReplicaStore{mgr: mgr})
	if err := rep.Start(); err != nil {
		t.Fatal(err)
	}
	defer rep.Stop()

	syncer := newRemoteSyncer(rep)
	if err := syncer.SyncEntity(fullID, 3, "test", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if e.EntityVersion() != 3 {
		t.Fatalf("entity version = %d, want 3", e.EntityVersion())
	}

	if err := syncer.SyncDelEntity(fullID, 4); err != nil {
		t.Fatal(err)
	}
	if _, ok := mgr.Get(fullID); ok {
		t.Fatal("wrapper should be removed after delete sync")
	}
}

type remoteSyncFakeBus struct {
	handlers map[string][]fsync.Handler
}

func newRemoteSyncFakeBus() *remoteSyncFakeBus {
	return &remoteSyncFakeBus{handlers: make(map[string][]fsync.Handler)}
}

func (b *remoteSyncFakeBus) Publish(msg *fsync.SyncMsg) error {
	for _, h := range b.handlers[msg.Topic] {
		if err := h(msg); err != nil {
			return err
		}
	}
	return nil
}

func (b *remoteSyncFakeBus) Subscribe(topic string, handler fsync.Handler) (func(), error) {
	b.handlers[topic] = append(b.handlers[topic], handler)
	idx := len(b.handlers[topic]) - 1
	return func() {
		handlers := b.handlers[topic]
		if idx < 0 || idx >= len(handlers) {
			return
		}
		b.handlers[topic] = append(handlers[:idx], handlers[idx+1:]...)
	}, nil
}

var _ fsync.ISyncBus = (*remoteSyncFakeBus)(nil)

var _ entity.IRemoteEntitySyncer = (*remoteSyncer)(nil)
var _ replica.Store = (*remoteReplicaStore)(nil)

func TestRemoteReplicaStoreLoadsUncachedWrapper(t *testing.T) {
	mgr := newRemoteEntityManager(newMockVersionedLockFactory(), DefaultConfig(), 1)
	loader := newMockLoader()
	mgr.SetLoader(loader)
	fullID := testRemoteFullID(404)
	e := newTestRemoteEntity(404, 1, 1)
	e.SetEntityVersion(1)
	loader.entities[fullID] = e
	store := remoteReplicaStore{mgr: mgr}
	payload, err := entity.EncodeRemoteSyncPayload("test", []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}

	if err := store.ApplyReplica(context.Background(), replica.Envelope{
		Key:     fullID,
		Version: 5,
		Op:      replica.OpUpsert,
		Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	if len(loader.loaded) != 1 || loader.loaded[0] != fullID {
		t.Fatalf("loaded=%v, want [%d]", loader.loaded, fullID)
	}
	if e.EntityVersion() != 5 {
		t.Fatalf("entity version=%d, want 5", e.EntityVersion())
	}
	if _, ok := mgr.Get(fullID); !ok {
		t.Fatal("wrapper should be cached after replica apply")
	}
}

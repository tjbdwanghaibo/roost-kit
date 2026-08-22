package remote_entity

import (
	"testing"
	"time"

	"github.com/tjbdwanghaibo/cube-core/entity"
)

func TestRemoteInterestRegistryIsScopedAndExpires(t *testing.T) {
	const kind entity.EntityKind = 128
	entity.MustRegisterEntityKindDefs(entity.EntityKindDef{Kind: kind, Category: 1, RemotePolicy: entity.RemotePolicyManaged})
	id, err := entity.BuildEntityID(1902, kind)
	if err != nil {
		t.Fatal(err)
	}
	registry := newRemoteInterestRegistry()
	key := entity.RemoteSnapshotKey{EntityID: id, Kind: kind, Scope: 3, Policy: 2}
	if err := registry.renew(entity.RemoteSnapshotInterest{ConsumerSID: 1001, Key: key, ExpiresAt: time.Now().Add(time.Second).UnixNano()}); err != nil {
		t.Fatal(err)
	}
	if !registry.interested(key) {
		t.Fatal("renewed interest was not visible")
	}
	otherPolicy := key
	otherPolicy.Policy++
	if registry.interested(otherPolicy) {
		t.Fatal("interest leaked into another LOD policy")
	}
	_ = registry.renew(entity.RemoteSnapshotInterest{ConsumerSID: 1001, Key: otherPolicy, ExpiresAt: time.Now().Add(-time.Second).UnixNano()})
	if registry.interested(otherPolicy) {
		t.Fatal("expired interest was retained")
	}
	registry.release(key, 1001)
	if registry.interested(key) {
		t.Fatal("released interest was retained")
	}
}

func TestRemoteInterestRegistryHasHardCapacityLimits(t *testing.T) {
	const kind entity.EntityKind = 129
	entity.MustRegisterEntityKindDefs(entity.EntityKindDef{Kind: kind, Category: 1, RemotePolicy: entity.RemotePolicyManaged})
	id, err := entity.BuildEntityID(1903, kind)
	if err != nil {
		t.Fatal(err)
	}
	registry := newRemoteInterestRegistry(1, 2)
	key := entity.RemoteSnapshotKey{EntityID: id, Kind: kind, Scope: 1}
	expires := time.Now().Add(time.Second).UnixNano()
	if err := registry.renew(entity.RemoteSnapshotInterest{ConsumerSID: 1, Key: key, ExpiresAt: expires}); err != nil {
		t.Fatal(err)
	}
	if err := registry.renew(entity.RemoteSnapshotInterest{ConsumerSID: 2, Key: key, ExpiresAt: expires}); err != nil {
		t.Fatal(err)
	}
	if err := registry.renew(entity.RemoteSnapshotInterest{ConsumerSID: 3, Key: key, ExpiresAt: expires}); err != entity.ErrRemoteOverloaded {
		t.Fatalf("subscription capacity error = %v, want %v", err, entity.ErrRemoteOverloaded)
	}
	other := key
	other.Scope++
	if err := registry.renew(entity.RemoteSnapshotInterest{ConsumerSID: 1, Key: other, ExpiresAt: expires}); err != entity.ErrRemoteOverloaded {
		t.Fatalf("key capacity error = %v, want %v", err, entity.ErrRemoteOverloaded)
	}
	registry.release(key, 2)
	if err := registry.renew(entity.RemoteSnapshotInterest{ConsumerSID: 1, Key: other, ExpiresAt: expires}); err != entity.ErrRemoteOverloaded {
		t.Fatalf("key limit must remain enforced, got %v", err)
	}
	registry.release(key, 1)
	if err := registry.renew(entity.RemoteSnapshotInterest{ConsumerSID: 1, Key: other, ExpiresAt: expires}); err != nil {
		t.Fatalf("capacity was not released: %v", err)
	}
}

func TestRemoteSnapshotReplicaKeyIncludesFullScope(t *testing.T) {
	base := entity.RemoteSnapshotKey{Tenant: 7, EntityID: 99, Kind: 12, Scope: 3, Policy: 2}
	variants := []entity.RemoteSnapshotKey{base}
	for _, mutate := range []func(*entity.RemoteSnapshotKey){
		func(k *entity.RemoteSnapshotKey) { k.Tenant++ },
		func(k *entity.RemoteSnapshotKey) { k.EntityID++ },
		func(k *entity.RemoteSnapshotKey) { k.Kind++ },
		func(k *entity.RemoteSnapshotKey) { k.Scope++ },
		func(k *entity.RemoteSnapshotKey) { k.Policy++ },
	} {
		v := base
		mutate(&v)
		variants = append(variants, v)
	}
	seen := make(map[int64]struct{}, len(variants))
	for _, key := range variants {
		hash := remoteSnapshotReplicaKey(key)
		if _, exists := seen[hash]; exists {
			t.Fatalf("unexpected replica key collision for %+v", key)
		}
		seen[hash] = struct{}{}
	}
}

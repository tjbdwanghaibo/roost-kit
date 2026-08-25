package checkpoint

import (
	"context"
	"sync/atomic"
	"testing"

	corecheckpoint "github.com/tjbdwanghaibo/cube-core/checkpoint"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

// gateTestEntity is a snapshotting entity whose snapshot count proves whether
// the release path consumed its dirty state.
type gateTestEntity struct {
	repositoryTestEntity
	snapshots atomic.Int32
}

func (e *gateTestEntity) Snapshot() []corecheckpoint.SaveItem {
	e.snapshots.Add(1)
	return []corecheckpoint.SaveItem{{Db: "game", Collection: "player", ID: e.ID(), Version: uint64(e.snapshots.Load())}}
}

type acceptingBackend struct{}

func (acceptingBackend) BulkSave(_ context.Context, ops []corecheckpoint.SaveOp) ([]corecheckpoint.SaveResult, error) {
	results := make([]corecheckpoint.SaveResult, len(ops))
	for i := range results {
		results[i] = corecheckpoint.SaveResult{OK: true}
	}
	return results, nil
}

func (acceptingBackend) BulkLoad(context.Context, corecheckpoint.LoadOp) ([]corecheckpoint.RawDoc, error) {
	return nil, nil
}

func (acceptingBackend) BulkRemove(context.Context, corecheckpoint.RemoveOp) error { return nil }

func TestReleaseGateDefersSnapshotUntilDurable(t *testing.T) {
	// Externalization gate: checkpoint must never persist an after-image
	// whose newest pipelined commit is not durable in the transaction WAL.
	// The deferred entity is snapshotted by the retry loop once the
	// watermark catches up.
	cp := corecheckpoint.New(acceptingBackend{}, corecheckpoint.WithFlushWorkers(0))
	if err := cp.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cp.Stop(context.Background()) }()
	mod := &Mod{
		cfg:             modConfig{pendingCapacity: 16},
		checkpoint:      cp,
		pendingSaves:    make(map[retrySaveKey]corecheckpoint.SaveItem),
		pendingReleases: make(map[int64]entity.IThreadSafeEntity),
		retryWake:       make(chan struct{}, 1),
	}
	var watermark atomic.Uint64
	mod.SetDurableWatermark(watermark.Load)

	ent := &gateTestEntity{repositoryTestEntity: repositoryTestEntity{EntityBase: entity.NewEntityBase(77, 1, false, repositoryTestKind)}}
	ent.Base().SetLastCommitLSN(9) // enqueued but not durable yet (watermark 0)

	mod.onEntityRelease(ent)
	if got := ent.snapshots.Load(); got != 0 {
		t.Fatalf("non-durable entity was snapshotted %d times", got)
	}
	if mod.pendingCount() != 1 || mod.gateDeferrals.Load() != 1 {
		t.Fatalf("deferral not recorded: pending=%d deferrals=%d", mod.pendingCount(), mod.gateDeferrals.Load())
	}

	// Watermark still behind: the retry loop must keep deferring.
	if err := mod.retryAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ent.snapshots.Load() != 0 || mod.pendingCount() != 1 {
		t.Fatalf("retry snapshotted a non-durable entity: snapshots=%d pending=%d", ent.snapshots.Load(), mod.pendingCount())
	}

	// fsync caught up: the retry loop snapshots and submits.
	watermark.Store(9)
	if err := mod.retryAdmission(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ent.snapshots.Load() != 1 || mod.pendingCount() != 0 {
		t.Fatalf("durable entity was not flushed by retry: snapshots=%d pending=%d", ent.snapshots.Load(), mod.pendingCount())
	}

	// A durable release goes straight through, and removing the gate
	// restores ungated behavior for entities with newer LSNs.
	mod.onEntityRelease(ent)
	if ent.snapshots.Load() != 2 {
		t.Fatalf("durable release did not snapshot: %d", ent.snapshots.Load())
	}
	mod.SetDurableWatermark(nil)
	ent.Base().SetLastCommitLSN(99)
	mod.onEntityRelease(ent)
	if ent.snapshots.Load() != 3 {
		t.Fatalf("ungated release did not snapshot: %d", ent.snapshots.Load())
	}
}

func TestReleaseGateDeferralCountsAgainstRetryCapacity(t *testing.T) {
	// Deferred releases share the bounded admission retry budget: a wedged
	// WAL must surface as a fail-stop, not unbounded memory growth.
	mod := &Mod{
		cfg:             modConfig{pendingCapacity: 1},
		pendingSaves:    make(map[retrySaveKey]corecheckpoint.SaveItem),
		pendingReleases: make(map[int64]entity.IThreadSafeEntity),
		retryWake:       make(chan struct{}, 1),
	}
	var watermark atomic.Uint64
	mod.SetDurableWatermark(watermark.Load)

	first := &gateTestEntity{repositoryTestEntity: repositoryTestEntity{EntityBase: entity.NewEntityBase(78, 1, false, repositoryTestKind)}}
	first.Base().SetLastCommitLSN(5)
	second := &gateTestEntity{repositoryTestEntity: repositoryTestEntity{EntityBase: entity.NewEntityBase(79, 1, false, repositoryTestKind)}}
	second.Base().SetLastCommitLSN(6)

	mod.onEntityRelease(first)
	// Re-deferring the same entity is idempotent and must not consume budget.
	mod.onEntityRelease(first)
	if mod.pendingCount() != 1 || mod.admissionFenced.Load() {
		t.Fatalf("idempotent deferral broke the budget: pending=%d fenced=%t", mod.pendingCount(), mod.admissionFenced.Load())
	}
	mod.onEntityRelease(second)
	if mod.pendingCount() != 1 || !mod.admissionFenced.Load() {
		t.Fatalf("capacity did not fail-stop: pending=%d fenced=%t", mod.pendingCount(), mod.admissionFenced.Load())
	}
}

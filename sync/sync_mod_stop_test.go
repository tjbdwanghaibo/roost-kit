package sync

import (
	"context"
	"testing"

	fsync "github.com/tjbdwanghaibo/cube-core/sync"
)

func TestSyncModStopWithContextPrefersContextStopper(t *testing.T) {
	bus := &contextStopSyncBus{}
	mod := &SyncMod{bus: bus}
	if err := mod.StopWithContext(context.Background()); err != nil {
		t.Fatalf("StopWithContext: %v", err)
	}
	if !bus.contextStopped {
		t.Fatal("StopWithContext should call bus StopWithContext")
	}
	if bus.legacyStopped {
		t.Fatal("legacy Stop should not be called when context stop is available")
	}
}

type contextStopSyncBus struct {
	contextStopped bool
	legacyStopped  bool
}

func (b *contextStopSyncBus) Publish(*fsync.SyncMsg) error { return nil }

func (b *contextStopSyncBus) Subscribe(string, fsync.Handler) (func(), error) {
	return func() {}, nil
}

func (b *contextStopSyncBus) Stop() {
	b.legacyStopped = true
}

func (b *contextStopSyncBus) StopWithContext(context.Context) error {
	b.contextStopped = true
	return nil
}

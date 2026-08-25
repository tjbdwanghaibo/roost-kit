package checkpoint

import (
	"context"
	"errors"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/app"
	corecheckpoint "github.com/tjbdwanghaibo/cube-core/checkpoint"
	"github.com/tjbdwanghaibo/cube-core/entity"
)

type rejectingDeleteWAL struct{}

func (*rejectingDeleteWAL) Start()                                {}
func (*rejectingDeleteWAL) Stop(context.Context) error            { return nil }
func (*rejectingDeleteWAL) Submit([]corecheckpoint.SaveItem) bool { return false }
func (*rejectingDeleteWAL) SubmitDurable(context.Context, []corecheckpoint.SaveItem) bool {
	return false
}
func (*rejectingDeleteWAL) SubmitDeleteDurable(context.Context, []corecheckpoint.SaveItem) bool {
	return false
}
func (*rejectingDeleteWAL) Ack(context.Context, []corecheckpoint.SaveItem) error        { return nil }
func (*rejectingDeleteWAL) Replay(context.Context, corecheckpoint.StorageBackend) error { return nil }
func (*rejectingDeleteWAL) Stats() corecheckpoint.SnapshotWALStats {
	return corecheckpoint.SnapshotWALStats{}
}

func TestAdmissionRetryCapacityTriggersFailStop(t *testing.T) {
	failure := app.NewRuntimeFailure()
	mod := &Mod{
		cfg:            modConfig{pendingCapacity: 1},
		pendingSaves:   make(map[retrySaveKey]corecheckpoint.SaveItem),
		runtimeFailure: failure,
	}
	mod.enqueueSaveRetry([]corecheckpoint.SaveItem{
		{Db: "game", Collection: "player", ID: 1, Version: 1},
		{Db: "game", Collection: "player", ID: 2, Version: 1},
	})
	if mod.pendingCount() != 1 || !mod.admissionFenced.Load() || failure.Err() == nil {
		t.Fatalf("capacity did not fail-stop: pending=%d fenced=%t err=%v", mod.pendingCount(), mod.admissionFenced.Load(), failure.Err())
	}
}

func TestDurableDeleteAdmissionFailureFencesRuntime(t *testing.T) {
	failure := app.NewRuntimeFailure()
	mod := &Mod{
		checkpoint: corecheckpoint.New(nil,
			corecheckpoint.WithSnapshotWAL(&rejectingDeleteWAL{}),
			corecheckpoint.WithSnapshotWALMode(corecheckpoint.SnapshotWALModeDurable),
		),
		runtimeFailure: failure,
	}
	value := &repositoryTestEntity{EntityBase: entity.NewEntityBase(99, 1, false, repositoryTestKind)}
	err := mod.admitEntityDelete(context.Background(), value)
	if err == nil || !mod.admissionFenced.Load() || failure.Err() == nil {
		t.Fatalf("indeterminate delete did not fence: err=%v fenced=%t failure=%v", err, mod.admissionFenced.Load(), failure.Err())
	}
	if !errors.Is(failure.Err(), err) && failure.Err().Error() != err.Error() {
		t.Fatalf("runtime failure = %v, admission error = %v", failure.Err(), err)
	}
}

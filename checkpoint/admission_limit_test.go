package checkpoint

import (
	"testing"

	"github.com/tjbdwanghaibo/cube-core/app"
	corecheckpoint "github.com/tjbdwanghaibo/cube-core/checkpoint"
)

func TestAdmissionRetryCapacityTriggersFailStop(t *testing.T) {
	failure := app.NewRuntimeFailure()
	mod := &Mod{
		cfg:            modConfig{pendingCapacity: 1},
		pendingSaves:   make(map[retrySaveKey]corecheckpoint.SaveItem),
		pendingDeletes: make(map[retrySaveKey]corecheckpoint.SaveItem),
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

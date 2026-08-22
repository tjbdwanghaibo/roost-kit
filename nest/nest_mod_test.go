package nest

import (
	"context"
	"testing"

	"github.com/spf13/viper"
	"github.com/tjbdwanghaibo/cube-core/app"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/health"
	corenest "github.com/tjbdwanghaibo/cube-core/nest"
	"github.com/tjbdwanghaibo/cube-kit/mods"
	"github.com/tjbdwanghaibo/cube-kit/nestwal"
)

type emptyGetter struct{}

func (emptyGetter) Get(context.Context, int64, entity.EntityCategory) (entity.IThreadSafeEntity, error) {
	return nil, corenest.ErrEntityNotFound
}
func (emptyGetter) GetMany(context.Context, []int64, []entity.EntityCategory) ([]entity.IThreadSafeEntity, error) {
	return nil, corenest.ErrEntityNotFound
}

func TestModProvidesInstanceClientAndHealth(t *testing.T) {
	cfg := viper.New()
	cfg.Set("nest.worker_num", 1)
	cfg.Set("nest.queue_capacity", 8)
	registry := app.NewRegistry(cfg)
	runtime, err := nestwal.OpenRuntime(nestwal.DefaultOptions(t.TempDir()), nil, nil, nestwal.DefaultCommitterOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown(context.Background()) })
	if err := registry.Register(mods.ModNestWAL, runtime); err != nil {
		t.Fatal(err)
	}
	mod := NewMod(emptyGetter{})
	if err := mod.Init(cfg); err != nil {
		t.Fatal(err)
	}
	if err := mod.Provide(registry); err != nil {
		t.Fatal(err)
	}
	client, ok := app.Lookup[corenest.Client](registry, mods.ModNest)
	if !ok || client == nil {
		t.Fatal("nest.Client not registered")
	}
	if err := mod.Start(); err != nil {
		t.Fatal(err)
	}
	if !mod.Engine().Running() {
		t.Fatal("engine not running")
	}
	healthRegistry := app.MustLookup[*health.Registry](registry, mods.ModHealth)
	snapshot := healthRegistry.Snapshot(context.Background())
	if !snapshot.OK {
		t.Fatalf("health=%+v", snapshot)
	}
	if err := mod.StopWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if mod.Engine().Running() {
		t.Fatal("engine still running")
	}
}

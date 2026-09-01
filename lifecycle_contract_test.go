package kit_test

import (
	"testing"

	"github.com/tjbdwanghaibo/cube-core/app"
	kitcheckpoint "github.com/tjbdwanghaibo/cube-kit/checkpoint"
	kitconfigdata "github.com/tjbdwanghaibo/cube-kit/configdata"
	kitetcd "github.com/tjbdwanghaibo/cube-kit/etcd"
	kitlock "github.com/tjbdwanghaibo/cube-kit/lock"
	kitmongo "github.com/tjbdwanghaibo/cube-kit/mongo"
	kitnats "github.com/tjbdwanghaibo/cube-kit/nats"
	kitnest "github.com/tjbdwanghaibo/cube-kit/nest"
	kitnestwal "github.com/tjbdwanghaibo/cube-kit/nestwal"
	kitops "github.com/tjbdwanghaibo/cube-kit/ops"
	kitredis "github.com/tjbdwanghaibo/cube-kit/redis"
	kitremoteentity "github.com/tjbdwanghaibo/cube-kit/remote_entity"
	kitsaga "github.com/tjbdwanghaibo/cube-kit/saga"
	kitstatslog "github.com/tjbdwanghaibo/cube-kit/statslog"
	kitsync "github.com/tjbdwanghaibo/cube-kit/sync"
)

// This compile-time list is the lifecycle gate for every infrastructure Mod
// shipped by cube-kit. New built-ins must join it instead of relying on App's
// legacy unbounded Stop fallback.
func TestBuiltInModsImplementContextStop(t *testing.T) {
	implementations := []app.ModStopperWithContext{
		(*kitcheckpoint.Mod)(nil),
		(*kitconfigdata.Mod)(nil),
		(*kitetcd.EtcdMod)(nil),
		(*kitlock.LockMod)(nil),
		(*kitmongo.MongoMod)(nil),
		(*kitnats.NatsMod)(nil),
		(*kitnest.Mod)(nil),
		(*kitnestwal.Mod)(nil),
		(*kitops.OpsMod)(nil),
		(*kitredis.RedisMod)(nil),
		(*kitremoteentity.RemoteEntityMod)(nil),
		(*kitsaga.Mod)(nil),
		(*kitstatslog.StatsLogMod)(nil),
		(*kitsync.SyncMod)(nil),
	}
	if len(implementations) != 14 {
		t.Fatalf("lifecycle contract list = %d, want 14", len(implementations))
	}
}

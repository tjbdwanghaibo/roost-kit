package lock

import (
	"github.com/tjbdwanghaibo/cube-core/app"
	flock "github.com/tjbdwanghaibo/cube-core/lock"
	"github.com/tjbdwanghaibo/cube-kit/mods"

	"github.com/spf13/viper"
)

// LockMod implements app.Mod, initializing the global lock manager.
type LockMod struct{}

func NewLockMod() *LockMod {
	return &LockMod{}
}

func (m *LockMod) Name() app.ModName         { return mods.ModLock }
func (m *LockMod) Init(_ *viper.Viper) error { return nil }

func (m *LockMod) Provide(r *app.Registry) error {
	flock.Mgr = flock.NewLockManager(nil)
	return r.Register(mods.ModLock, flock.Mgr)
}

func (m *LockMod) Start() error { return nil }
func (m *LockMod) Stop()        {}

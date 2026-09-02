// Package manager provides ManagerMod, the app.Mod that owns a service's
// in-memory singleton managers.
//
// A manager is process-wide singleton logic — a scene registry, a routing
// table, a mail cache — with a Start/Stop lifecycle and no persistent state of
// its own. cube-core declares the contract (app.IManager,
// app.ManagerDependencyProvider, app.IManagerStopperWithContext) and names
// ManagerMod as its owner; this package is that owner.
//
// Managers are per service, not per process: the same singleton may appear in
// several services' manager sets, and only the service actually running starts
// it. That is why ManagerMod is constructed per service rather than shared.
package manager

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/tjbdwanghaibo/cube-core/app"
	fctx "github.com/tjbdwanghaibo/cube-core/fctx"
	"github.com/tjbdwanghaibo/cube-core/metrics"
	"github.com/tjbdwanghaibo/cube-kit/mods"

	"github.com/spf13/viper"
)

// ErrManagerRegisterAfterStart is returned by Register once Start has run. A
// manager added after Start would never be started and never be stopped, and
// the only symptom would be a nil dependency somewhere later — so the mod
// refuses instead of silently dropping it.
var ErrManagerRegisterAfterStart = errors.New("manager: Register after Start")

// ManagerMod starts a service's managers in dependency order and stops them in
// reverse. Layers: Service -> Mod (ManagerMod) -> IManager.
type ManagerMod struct {
	mu       sync.Mutex
	managers []app.IManager
	started  []app.IManager
	registry *app.Registry
	stopping bool
}

// NewManagerMod builds a mod for the given managers, in registration order.
// Order among managers that declare no dependency on each other is preserved.
func NewManagerMod(managers ...app.IManager) *ManagerMod {
	return &ManagerMod{managers: append([]app.IManager(nil), managers...)}
}

// Register appends a manager. Assembly normally happens through
// NewManagerMod; Register exists for conditional wiring.
func (m *ManagerMod) Register(manager app.IManager) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started != nil {
		return fmt.Errorf("%w: %q", ErrManagerRegisterAfterStart, managerName(manager))
	}
	m.managers = append(m.managers, manager)
	return nil
}

// MustRegister is Register for assembly code that has no error path. It panics
// rather than continue with a manager that will never start.
func (m *ManagerMod) MustRegister(manager app.IManager) {
	if err := m.Register(manager); err != nil {
		panic(err)
	}
}

// Managers returns the registered managers in registration order. The slice is
// a copy: the mod's own list drives the lifecycle and must not be editable
// from outside it.
func (m *ManagerMod) Managers() []app.IManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]app.IManager(nil), m.managers...)
}

// Manager returns the registered manager with the given name.
func (m *ManagerMod) Manager(name string) (app.IManager, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, manager := range m.managers {
		if manager != nil && manager.Name() == name {
			return manager, true
		}
	}
	return nil, false
}

func (m *ManagerMod) Name() app.ModName         { return mods.ModManager }
func (m *ManagerMod) Init(_ *viper.Viper) error { return nil }

func (m *ManagerMod) Provide(r *app.Registry) error {
	if r == nil {
		return fmt.Errorf("manager mod: registry is nil")
	}
	m.mu.Lock()
	m.registry = r
	m.mu.Unlock()
	return r.Register(mods.ModManager, m)
}

func (m *ManagerMod) Start() error {
	m.mu.Lock()
	registry := m.registry
	pending := append([]app.IManager(nil), m.managers...)
	m.mu.Unlock()

	// A manager's only handle on the rest of the service is the registry it
	// receives from Start. Starting without one hands every manager a nil
	// registry, which fails much later and far from the cause.
	if registry == nil {
		return fmt.Errorf("manager mod: Start before Provide, no registry available")
	}

	ordered, err := sortManagers(pending)
	if err != nil {
		return err
	}

	for _, manager := range ordered {
		// Shutdown can arrive while startup is still working through the
		// list — a SIGTERM during a slow start. Abort here instead of racing
		// it: continuing would start managers that Stop has already passed,
		// leaving them running after shutdown reported success.
		if m.stopRequested() {
			if stopErr := m.stopStarted(fctx.BaseContext()); stopErr != nil {
				return fmt.Errorf("manager mod: start aborted by shutdown, rollback: %w", stopErr)
			}
			return fmt.Errorf("manager mod: start aborted by shutdown")
		}
		begin := time.Now()
		slog.Info("manager start", "name", manager.Name())
		// A manager whose Start fails is deliberately NOT stopped: it never
		// completed, so Stop would have to cope with a half-built object.
		// Start owns cleanup of its own failure; the mod rolls back only the
		// managers that reported success.
		if err := manager.Start(registry); err != nil {
			startErr := fmt.Errorf("manager %s start: %w", manager.Name(), err)
			if stopErr := m.stopStarted(fctx.BaseContext()); stopErr != nil {
				return errors.Join(startErr, fmt.Errorf("rollback: %w", stopErr))
			}
			return startErr
		}
		elapsed := time.Since(begin)
		metrics.ObserveHistogram("manager.start.duration", metrics.Labels{"manager": manager.Name()}, elapsed)
		slog.Info("manager started", "name", manager.Name(), "elapsed", elapsed)

		m.mu.Lock()
		m.started = append(m.started, manager)
		m.mu.Unlock()
		metrics.SetGauge("manager.started", nil, int64(m.startedCount()))
	}
	return nil
}

func (m *ManagerMod) Stop() {
	if err := m.StopWithContext(fctx.BaseContext()); err != nil {
		slog.Error("manager stop failed", "err", err)
	}
}

func (m *ManagerMod) StopWithContext(ctx context.Context) error {
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	return m.stopStarted(ctx)
}

// stopStarted stops the managers that actually started, newest first, and is
// idempotent: the started list is taken under the lock so a second Stop (or a
// Stop racing the rollback inside Start) finds nothing left to do rather than
// stopping a manager twice.
func (m *ManagerMod) stopStarted(ctx context.Context) error {
	m.mu.Lock()
	started := m.started
	// A non-nil empty slice, not nil: it marks the lifecycle as over, so a
	// later Register is still refused even when nothing had started yet.
	m.started = []app.IManager{}
	m.stopping = true
	m.mu.Unlock()

	var joined error
	for i := len(started) - 1; i >= 0; i-- {
		manager := started[i]
		slog.Info("manager stop", "name", manager.Name())
		if stopper, ok := manager.(app.IManagerStopperWithContext); ok {
			if err := stopper.StopWithContext(ctx); err != nil {
				joined = errors.Join(joined, fmt.Errorf("manager %s stop: %w", manager.Name(), err))
			}
			continue
		}
		manager.Stop()
	}
	metrics.SetGauge("manager.started", nil, 0)
	return joined
}

func (m *ManagerMod) stopRequested() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopping
}

func (m *ManagerMod) startedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.started)
}

func managerName(manager app.IManager) string {
	if manager == nil {
		return "<nil>"
	}
	return manager.Name()
}

var (
	_ app.Mod                   = (*ManagerMod)(nil)
	_ app.ModStopperWithContext = (*ManagerMod)(nil)
)

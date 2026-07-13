package ops

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/app"
	"github.com/tjbdwanghaibo/cube-core/health"

	"github.com/spf13/viper"
)

func TestOpsAdminRequiresExplicitSecureToken(t *testing.T) {
	cfg := viper.New()
	cfg.Set("ops.admin_enabled", true)
	cfg.Set("ops.admin_token", "dev-token")

	if err := NewOpsMod().Init(cfg); err == nil {
		t.Fatal("expected dev admin token to be rejected by default")
	}

	cfg.Set("ops.allow_dev_token", true)
	if err := NewOpsMod().Init(cfg); err != nil {
		t.Fatalf("Init with explicit dev allowance: %v", err)
	}
}

func TestOpsAdminEndpointIsHiddenWhenDisabled(t *testing.T) {
	m := NewOpsMod()
	cfg := viper.New()
	cfg.Set("ops.admin_enabled", false)
	if err := m.Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/admin/commands", nil)
	rec := httptest.NewRecorder()

	m.handleAdminCommands(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestOpsReadyReflectsLifecycleState(t *testing.T) {
	m := NewOpsMod()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	m.handleReady(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("initial ready status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	m.setReady(true, "ok")
	rec = httptest.NewRecorder()
	m.handleReady(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestOpsReadyIncludesDependencyHealth(t *testing.T) {
	reg := app.NewRegistry(viper.New())
	healthReg := app.MustLookup[*health.Registry](reg, app.ModHealth)
	healthReg.Register("redis", health.CheckerFunc(func(ctx context.Context) health.Result {
		return health.Result{Status: health.StatusFail, Message: "ping failed"}
	}))

	m := NewOpsMod()
	if err := m.Provide(reg); err != nil {
		t.Fatalf("Provide: %v", err)
	}
	m.setReady(true, "ok")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	m.handleReady(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode ready: %v", err)
	}
	deps, ok := body["dependencies"].([]any)
	if !ok || len(deps) != 1 {
		t.Fatalf("dependencies = %+v", body["dependencies"])
	}
}

func TestOpsModStopWithContextUsesCallerContext(t *testing.T) {
	mod := &OpsMod{server: &http.Server{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := mod.StopWithContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("StopWithContext err = %v, want context canceled", err)
	}
}

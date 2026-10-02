package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/fill"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/pressure"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

type failOnceTransport struct{ failed bool }

func (t *failOnceTransport) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	if !t.failed {
		t.failed = true
		return model.ValveReceipt{}, errors.New("timeout")
	}
	return model.ValveReceipt{
		CommandID: command.ID, Executed: true, Message: "accepted",
		ReceivedAt: time.Now().UTC(),
	}, nil
}

func newTestServer(t *testing.T, transport valve.Transport) (*Server, *fill.Service) {
	t.Helper()
	now := func() time.Time { return time.Unix(1000, 0).UTC() }
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	vreg := vessel.NewRegistry()
	v, _ := model.NewVessel("V-01", "Vessel one", "north", now())
	_ = vreg.Register(v)
	vessels := vessel.NewStateService(vreg, now)
	creg := circuit.NewRegistry(circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true})
	manager := circuit.NewManager("C-A")
	controller, err := valve.NewController(transport, now)
	if err != nil {
		t.Fatal(err)
	}
	box, err := valve.NewOutbox(store, controller, now)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := valve.NewRouteService(box, store, now)
	if err != nil {
		t.Fatal(err)
	}
	coord := fill.NewCoordinator(manager, creg, routes, vessels, store, now)
	pm := pressure.NewMonitor(now)
	_ = pm.AddVessel("V-01", 2, 1.0)
	completion := fill.NewCompletionService(coord, pm, store, now)
	abort := fill.NewAbortService(coord, now)
	rec := fill.NewReconcileService(coord, routes, box, store, now)
	if err := rec.Restore(); err != nil {
		t.Fatal(err)
	}
	svc, err := fill.NewService(coord, completion, abort, rec, 2.0)
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(Dependencies{Fills: svc, Now: now})
	return server, svc
}

func do(t *testing.T, handler http.Handler, method, path string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		encoded, _ := json.Marshal(body)
		reader = bytes.NewReader(encoded)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	var parsed map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &parsed)
	}
	return rec, parsed
}

// 开阀超时 -> 202 待确认；命令查询可见未决命令；人工对账后命令确认执行。
func TestValveCommandAwaitingAndReconcileFlow(t *testing.T) {
	server, _ := newTestServer(t, &failOnceTransport{})
	handler := server.Handler()

	rec, body := do(t, handler, http.MethodPost, "/api/fill-sessions",
		map[string]any{"vessel_id": "V-01", "liters": 2.0})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 on unconfirmed open, got %d: %s", rec.Code, rec.Body.String())
	}
	if body["awaiting"] != true {
		t.Fatalf("expected awaiting=true, got %v", body["awaiting"])
	}
	sessionID := body["session"].(map[string]any)["id"].(string)

	rec, body = do(t, handler, http.MethodGet, "/api/valve-commands", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("list status %d", rec.Code)
	}
	items, ok := body["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one pending command, got %v", body["items"])
	}
	first := items[0].(map[string]any)
	commandID := first["command"].(map[string]any)["id"].(string)
	if first["status"] != "inflight" {
		t.Fatalf("expected inflight, got %v", first["status"])
	}

	rec, body = do(t, handler, http.MethodGet, "/api/valve-commands/"+commandID, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get status %d", rec.Code)
	}
	if body["status"] != "inflight" {
		t.Fatalf("expected inflight detail, got %v", body["status"])
	}

	// 人工现场确认阀门确已打开。
	rec, body = do(t, handler, http.MethodPost, "/api/valve-commands/"+commandID+"/resolve",
		map[string]any{"outcome": "executed", "operator": "ops-1", "note": "valve open on site"})
	if rec.Code != http.StatusOK {
		t.Fatalf("resolve status %d: %s", rec.Code, rec.Body.String())
	}
	if body["status"] != "resolved" {
		t.Fatalf("expected resolved, got %v", body["status"])
	}
	// 对账后关联会话应已从 awaiting 收敛到 filling。
	rec, body = do(t, handler, http.MethodGet, "/api/fill-sessions", nil)
	var state string
	for _, item := range body["items"].([]any) {
		session := item.(map[string]any)
		if session["id"] == sessionID {
			state = session["state"].(string)
		}
	}
	if state != string(model.FillFilling) {
		t.Fatalf("expected session filling after manual resolve, got %q", state)
	}
}

// 缺少操作员身份的对账请求必须被拒绝。
func TestResolveRequiresOperator(t *testing.T) {
	server, _ := newTestServer(t, &failOnceTransport{})
	handler := server.Handler()
	_, _ = do(t, handler, http.MethodPost, "/api/fill-sessions",
		map[string]any{"vessel_id": "V-01", "liters": 2.0})
	rec, body := do(t, handler, http.MethodGet, "/api/valve-commands", nil)
	commandID := body["items"].([]any)[0].(map[string]any)["command"].(map[string]any)["id"].(string)

	rec, _ = do(t, handler, http.MethodPost, "/api/valve-commands/"+commandID+"/resolve",
		map[string]any{"outcome": "executed"})
	if rec.Code != http.StatusConflict && rec.Code != http.StatusBadRequest {
		t.Fatalf("missing operator must be rejected, got %d", rec.Code)
	}
}

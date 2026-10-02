package fill

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/pressure"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

// gateTransport 按命令 ID 控制前若干次下发失败（模拟超时），之后返回执行回执。
// executed 集合可跨实例共享，用来模拟设备侧对命令 ID 的持久幂等记忆。
type gateTransport struct {
	mu        sync.Mutex
	failLeft  map[uuid.UUID]int
	failFirst int
	executed  map[uuid.UUID]bool
	calls     []uuid.UUID
	now       func() time.Time
}

func newGateTransport(now func() time.Time) *gateTransport {
	return &gateTransport{
		failLeft: make(map[uuid.UUID]int),
		executed: make(map[uuid.UUID]bool),
		now:      now,
	}
}

func (g *gateTransport) failNext(id uuid.UUID, times int) {
	g.mu.Lock()
	g.failLeft[id] += times
	g.mu.Unlock()
}

func (g *gateTransport) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.calls = append(g.calls, command.ID)
	if g.failFirst > 0 {
		g.failFirst--
		return model.ValveReceipt{}, errors.New("simulated device timeout")
	}
	if g.failLeft[command.ID] > 0 {
		g.failLeft[command.ID]--
		return model.ValveReceipt{}, errors.New("simulated device timeout")
	}
	duplicate := g.executed[command.ID]
	g.executed[command.ID] = true
	return model.ValveReceipt{
		CommandID:  command.ID,
		Executed:   true,
		Duplicate:  duplicate,
		Message:    "accepted",
		ReceivedAt: g.now().UTC(),
	}, nil
}

func (g *gateTransport) callCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.calls)
}

type harness struct {
	store     *journal.Store
	registry  *vessel.Registry
	vessels   *vessel.StateService
	circReg   *circuit.Registry
	manager   *circuit.Manager
	transport *gateTransport
	outbox    *valve.Outbox
	routes    *valve.RouteService
	coord     *Coordinator
	svc       *Service
	pressure  *pressure.Monitor
}

func newHarness(t *testing.T, now func() time.Time) *harness {
	t.Helper()
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	vreg := vessel.NewRegistry()
	v, err := model.NewVessel("V-01", "Vessel one", "north", now())
	if err != nil {
		t.Fatal(err)
	}
	if err := vreg.Register(v); err != nil {
		t.Fatal(err)
	}
	vessels := vessel.NewStateService(vreg, now)
	creg := circuit.NewRegistry(circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true})
	manager := circuit.NewManager("C-A")
	transport := newGateTransport(now)
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
	coord := NewCoordinator(manager, creg, routes, vessels, store, now)
	pm := pressure.NewMonitor(now)
	if err := pm.AddVessel("V-01", 2, 1.0); err != nil {
		t.Fatal(err)
	}
	completion := NewCompletionService(coord, pm, store, now)
	abort := NewAbortService(coord, now)
	reconcile := NewReconcileService(coord, routes, box, store, now)
	if err := reconcile.Restore(); err != nil {
		t.Fatal(err)
	}
	svc, err := NewService(coord, completion, abort, reconcile, 2.0)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		store: store, registry: vreg, vessels: vessels, circReg: creg, manager: manager,
		transport: transport, outbox: box, routes: routes, coord: coord, svc: svc, pressure: pm,
	}
}

// 开阀成功：进入 filling，回路保留给本会话、容器处于 filling。
func TestFillStartSuccessReservesCircuit(t *testing.T) {
	now := func() time.Time { return time.Unix(1000, 0).UTC() }
	h := newHarness(t, now)

	outcome, err := h.svc.StartManual(context.Background(), "V-01", 2.0)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Awaiting {
		t.Fatal("successful open must not be awaiting")
	}
	if outcome.Session.State != model.FillFilling {
		t.Fatalf("state=%s", outcome.Session.State)
	}
	res, occupied := h.manager.Get("C-A")
	if !occupied || res.SessionID != outcome.Session.ID {
		t.Fatal("circuit must stay reserved to the session")
	}
	vs, _ := h.registry.Get("V-01")
	if vs.Status != model.VesselFilling {
		t.Fatalf("vessel status=%s", vs.Status)
	}
}

// 真正的超时→保留→同 ID 补发成功：让首次开阀就失败来验证。
func TestFillStartTimeoutThenSameIDResend(t *testing.T) {
	base := time.Unix(2000, 0).UTC()
	now := func() time.Time { return base }
	h := newHarness(t, now)

	// 在开阀命令发出前无法知道其 ID；改为让传输对“第一个见到的命令”失败一次。
	h.transport.mu.Lock()
	h.transport.failFirst = 1
	h.transport.mu.Unlock()

	outcome, err := h.svc.StartManual(context.Background(), "V-01", 2.0)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.Awaiting || outcome.Session.State != model.FillAwaitingValve {
		t.Fatalf("expected awaiting_valve on timeout, got awaiting=%v state=%s", outcome.Awaiting, outcome.Session.State)
	}
	// 关键：回路与容器状态必须保留。
	if _, occupied := h.manager.Get("C-A"); !occupied {
		t.Fatal("circuit reservation must be retained while open is unconfirmed")
	}
	vs, _ := h.registry.Get("V-01")
	if vs.Status != model.VesselFilling {
		t.Fatalf("vessel must remain filling, got %s", vs.Status)
	}

	pending := h.svc.PendingCommands()
	if len(pending) != 1 || pending[0].Settled() {
		t.Fatalf("expected one unconfirmed command, got %+v", pending)
	}
	openID := pending[0].Command.ID

	// 恢复：同一 ID 补发，这次设备确认执行。
	if err := h.svc.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	ids := map[uuid.UUID]bool{}
	for _, id := range h.transport.calls {
		ids[id] = true
	}
	if len(ids) != 1 {
		t.Fatalf("recovery must reuse the same command id, saw %d ids: %v", len(ids), ids)
	}
	if _, ok := ids[openID]; !ok {
		t.Fatalf("resent id %v differs from original %v", ids, openID)
	}
	session, _ := h.svc.Get(outcome.Session.ID)
	if session.State != model.FillFilling {
		t.Fatalf("session should advance to filling after confirmed open, got %s", session.State)
	}
}

// 开阀未知时人工对账确认“未执行”：预留释放、容器回 ready、会话失败。
func TestFillStartTimeoutManualResolveFailedReleases(t *testing.T) {
	base := time.Unix(3000, 0).UTC()
	now := func() time.Time { return base }
	h := newHarness(t, now)
	h.transport.mu.Lock()
	h.transport.failFirst = 1
	h.transport.mu.Unlock()

	outcome, err := h.svc.StartManual(context.Background(), "V-01", 2.0)
	if err != nil {
		t.Fatal(err)
	}
	openID := h.mustOpenCommandID(t, outcome.Session.ID)

	if _, err := h.svc.ResolveCommand(context.Background(), openID, valve.ResolutionFailed, "ops-1", "valve verified closed on site"); err != nil {
		t.Fatal(err)
	}
	if _, occupied := h.manager.Get("C-A"); occupied {
		t.Fatal("circuit must be released once open is confirmed not executed")
	}
	vs, _ := h.registry.Get("V-01")
	if vs.Status != model.VesselReady {
		t.Fatalf("vessel must return to ready, got %s", vs.Status)
	}
	session, _ := h.svc.Get(outcome.Session.ID)
	if session.State != model.FillFailed {
		t.Fatalf("session should be failed, got %s", session.State)
	}
}

// 关阀超时：完成流程不得宣告成功，回路保留为故障；同 ID 补发成功后才释放。
func TestFillCompleteCloseTimeoutRetainsThenRecovers(t *testing.T) {
	base := time.Unix(4000, 0).UTC()
	now := func() time.Time { return base }
	h := newHarness(t, now)

	outcome, err := h.svc.StartManual(context.Background(), "V-01", 2.0)
	if err != nil {
		t.Fatal(err)
	}
	sid := outcome.Session.ID
	if _, err := h.svc.BeginSoak(sid); err != nil {
		t.Fatal(err)
	}
	// 压力窗口稳定所需样本。
	for i := 0; i < 2; i++ {
		if err := h.pressure.Add(model.Sample{
			ProbeID: "P-P01", VesselID: "V-01", Kind: model.ProbePressure,
			Value: 100.0, ObservedAt: base.Add(time.Duration(i) * time.Second),
		}); err != nil {
			t.Fatal(err)
		}
	}

	route, ok, err := h.routes.Get(sid)
	if err != nil || !ok {
		t.Fatalf("route missing: %v", err)
	}
	h.transport.failNext(route.CloseCommandID, 1)

	if _, err := h.svc.Complete(context.Background(), sid); !errors.Is(err, ErrCommandUnconfirmed) {
		t.Fatalf("expected ErrCommandUnconfirmed, got %v", err)
	}
	res, occupied := h.manager.Get("C-A")
	if !occupied || !res.Faulted {
		t.Fatal("circuit must be retained as faulted while close is unconfirmed")
	}
	vs, _ := h.registry.Get("V-01")
	if vs.Status != model.VesselFilling {
		t.Fatalf("vessel must remain filling while close unconfirmed, got %s", vs.Status)
	}

	// 同 ID 补发关阀成功后，会话完成、回路释放、容器 ready。
	if err := h.svc.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	session, _ := h.svc.Get(sid)
	if session.State != model.FillCompleted {
		t.Fatalf("expected completed after confirmed close, got %s", session.State)
	}
	if _, occupied := h.manager.Get("C-A"); occupied {
		t.Fatal("circuit must be released after confirmed close")
	}
	vs, _ = h.registry.Get("V-01")
	if vs.Status != model.VesselReady {
		t.Fatalf("vessel ready expected, got %s", vs.Status)
	}
}

// 进程重启：用同一持久存储重建全部组件，开阀已确认的会话恢复为 awaiting 视图，
// Restore 后回路与容器状态被重建；Recover 凭借设备侧幂等记忆取回执行回执并收敛回 filling。
func TestFillRestartRebuildsReservationAndReconciles(t *testing.T) {
	base := time.Unix(5000, 0).UTC()
	now := func() time.Time { return base }
	h := newHarness(t, now)
	outcome, err := h.svc.StartManual(context.Background(), "V-01", 2.0)
	if err != nil {
		t.Fatal(err)
	}
	sid := outcome.Session.ID

	// 模拟重启：全新 registry/manager/coordinator，但复用 journal 和设备幂等记忆。
	vreg2 := vessel.NewRegistry()
	v2, _ := model.NewVessel("V-01", "Vessel one", "north", base)
	_ = vreg2.Register(v2)
	vessels2 := vessel.NewStateService(vreg2, now)
	creg2 := circuit.NewRegistry(circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true})
	manager2 := circuit.NewManager("C-A")
	controller2, _ := valve.NewController(h.transport, now)
	box2, err := valve.NewOutbox(h.store, controller2, now)
	if err != nil {
		t.Fatal(err)
	}
	routes2, err := valve.NewRouteService(box2, h.store, now)
	if err != nil {
		t.Fatal(err)
	}
	coord2 := NewCoordinator(manager2, creg2, routes2, vessels2, h.store, now)
	pm2 := pressure.NewMonitor(now)
	_ = pm2.AddVessel("V-01", 2, 1.0)
	completion2 := NewCompletionService(coord2, pm2, h.store, now)
	abort2 := NewAbortService(coord2, now)
	rec2 := NewReconcileService(coord2, routes2, box2, h.store, now)
	if err := rec2.Restore(); err != nil {
		t.Fatal(err)
	}
	svc2, err := NewService(coord2, completion2, abort2, rec2, 2.0)
	if err != nil {
		t.Fatal(err)
	}

	// Restore 已重建回路预留与容器 filling。
	res, occupied := manager2.Get("C-A")
	if !occupied || res.SessionID != sid {
		t.Fatal("restart must rebuild the circuit reservation for the session")
	}
	vs2, _ := vreg2.Get("V-01")
	if vs2.Status != model.VesselFilling {
		t.Fatalf("restart must rebuild vessel filling state, got %s", vs2.Status)
	}
	recovered, exists := svc2.Get(sid)
	if !exists || recovered.State != model.FillAwaitingValve {
		t.Fatalf("restart must restore session as awaiting_valve, got %+v", recovered)
	}

	// Recover 用原开阀 ID 补发，设备凭持久记忆返回 duplicate executed，会话收敛回 filling。
	if err := svc2.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	final, _ := svc2.Get(sid)
	if final.State != model.FillFilling {
		t.Fatalf("expected filling after recovery, got %s", final.State)
	}
}

func (h *harness) mustOpenCommandID(t *testing.T, sessionID uuid.UUID) uuid.UUID {
	t.Helper()
	route, ok, err := h.routes.Get(sessionID)
	if err != nil || !ok {
		t.Fatalf("route missing: %v", err)
	}
	return route.OpenCommandID
}

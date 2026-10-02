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
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

type flakyTransport struct {
	mu        sync.Mutex
	failOpen  bool
	failClose bool
	sent      []model.ValveCommand
}

func (t *flakyTransport) Execute(_ context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sent = append(t.sent, command)
	if command.Action == model.ValveOpen && t.failOpen {
		return model.ValveReceipt{}, errors.New("transport timeout")
	}
	if command.Action == model.ValveClose && t.failClose {
		return model.ValveReceipt{}, errors.New("transport timeout")
	}
	return model.ValveReceipt{
		CommandID:  command.ID,
		Executed:   true,
		Message:    "accepted",
		ReceivedAt: time.Now().UTC(),
	}, nil
}

func (t *flakyTransport) setFailOpen(fail bool) {
	t.mu.Lock()
	t.failOpen = fail
	t.mu.Unlock()
}

func (t *flakyTransport) setFailClose(fail bool) {
	t.mu.Lock()
	t.failClose = fail
	t.mu.Unlock()
}

func (t *flakyTransport) sentCommands() []model.ValveCommand {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]model.ValveCommand(nil), t.sent...)
}

type fillHarness struct {
	coordinator *Coordinator
	reconciler  *ReconcileService
	aborts      *AbortService
	outbox      *valve.Outbox
	circuits    *circuit.Manager
	vessels     *vessel.StateService
	transport   *flakyTransport
}

func newFillHarness(t *testing.T) *fillHarness {
	t.Helper()
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := valve.NewOutbox(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &flakyTransport{}
	controller, err := valve.NewController(transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	commands, err := valve.NewCommandService(controller, outbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	routes := valve.NewRouteService(commands)
	circuits := circuit.NewManager("C-A")
	circuitList := circuit.NewRegistry(circuit.Circuit{ID: "C-A", Zone: "north", ValveID: "VALVE-A", Enabled: true})
	registry := vessel.NewRegistry()
	item, err := model.NewVessel("V-01", "vessel", "north", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(item); err != nil {
		t.Fatal(err)
	}
	vessels := vessel.NewStateService(registry, nil)
	coordinator := NewCoordinator(circuits, circuitList, routes, vessels, nil)
	retry, err := NewRetryService(commands, RetryPolicy{Attempts: 2, Delay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.SetRetryService(retry)
	return &fillHarness{
		coordinator: coordinator,
		reconciler:  NewReconcileService(coordinator, outbox, nil),
		aborts:      NewAbortService(coordinator, outbox, nil),
		outbox:      outbox,
		circuits:    circuits,
		vessels:     vessels,
		transport:   transport,
	}
}

func (h *fillHarness) pendingOpen(t *testing.T, sessionID uuid.UUID) model.ValveCommandRecord {
	t.Helper()
	for _, record := range h.reconciler.Pending() {
		if record.Command.FillSessionID == sessionID && record.Command.Action == model.ValveOpen {
			return record
		}
	}
	t.Fatalf("no pending open command for session %s", sessionID)
	return model.ValveCommandRecord{}
}

func TestStartTimeoutRetainsCircuitAndKeepsCommandPending(t *testing.T) {
	h := newFillHarness(t)
	h.transport.setFailOpen(true)
	session, err := h.coordinator.StartAutomatic(context.Background(), "V-01", 2.1)
	if !errors.Is(err, ErrCommandUnconfirmed) {
		t.Fatalf("err=%v", err)
	}
	if session.ID == uuid.Nil {
		t.Fatal("limbo session was not returned")
	}
	reservation, ok := h.circuits.Get("C-A")
	if !ok || !reservation.Faulted {
		t.Fatalf("reservation=%v ok=%v", reservation, ok)
	}
	pending := h.pendingOpen(t, session.ID)
	if pending.Status != model.ValveCommandPending {
		t.Fatalf("status=%s", pending.Status)
	}
	// The circuit stays occupied: a second fill cannot take it.
	if _, err := h.coordinator.StartAutomatic(context.Background(), "V-02", 2.1); !errors.Is(err, circuit.ErrCircuitBusy) && !errors.Is(err, ErrActiveSession) {
		t.Fatalf("second start should be blocked, got %v", err)
	}
	// The same command identity is reused on every send attempt.
	for _, sent := range h.transport.sentCommands() {
		if sent.ID != pending.Command.ID {
			t.Fatalf("attempt used a new command id %s", sent.ID)
		}
	}
}

func TestReconcileOpenNotExecutedRollsBackCleanly(t *testing.T) {
	h := newFillHarness(t)
	h.transport.setFailOpen(true)
	session, err := h.coordinator.StartAutomatic(context.Background(), "V-01", 2.1)
	if !errors.Is(err, ErrCommandUnconfirmed) {
		t.Fatalf("err=%v", err)
	}
	pending := h.pendingOpen(t, session.ID)
	if _, err := h.reconciler.Reconcile(pending.Command.ID, false, "operator", "valve verified closed"); err != nil {
		t.Fatal(err)
	}
	if _, occupied := h.circuits.Get("C-A"); occupied {
		t.Fatal("circuit stayed reserved after not-executed reconciliation")
	}
	stored, ok := h.coordinator.Get(session.ID)
	if !ok || stored.State != model.FillFailed {
		t.Fatalf("session=%v ok=%v", stored, ok)
	}
	if item, _ := h.vessels.Registry().Get("V-01"); item.Status != model.VesselReady {
		t.Fatalf("vessel status=%s", item.Status)
	}
	if pending := h.reconciler.Pending(); len(pending) != 0 {
		t.Fatalf("pending=%v", pending)
	}
}

func TestReconcileOpenExecutedKeepsCircuitUntilCloseConfirmed(t *testing.T) {
	h := newFillHarness(t)
	h.transport.setFailOpen(true)
	session, err := h.coordinator.StartAutomatic(context.Background(), "V-01", 2.1)
	if !errors.Is(err, ErrCommandUnconfirmed) {
		t.Fatalf("err=%v", err)
	}
	pending := h.pendingOpen(t, session.ID)
	if _, err := h.reconciler.Reconcile(pending.Command.ID, true, "operator", "valve found open"); err != nil {
		t.Fatal(err)
	}
	reservation, ok := h.circuits.Get("C-A")
	if !ok || !reservation.Faulted {
		t.Fatalf("reservation=%v ok=%v", reservation, ok)
	}
	// Recovery is refused while the valve is confirmed open with no close.
	if err := h.aborts.RecoverIsolation(session.ID); err == nil {
		t.Fatal("recovery should be refused while the valve is open")
	}
	// Abort dispatches the close command with the derived identity.
	if _, err := h.aborts.Abort(context.Background(), session.ID, "manual abort"); err == nil {
		t.Fatal("abort on a faulted circuit should report the fault")
	}
	closeCommand, ok := h.outbox.CommandFor(session.ID, model.ValveClose)
	if !ok {
		t.Fatal("close command was not recorded")
	}
	if closeCommand.ID != model.ValveCommandIDFor(session.ID, model.ValveClose) {
		t.Fatalf("close command id=%s", closeCommand.ID)
	}
	closeRecord, ok := h.outbox.Get(closeCommand.ID)
	if !ok || closeRecord.Status != model.ValveCommandAcknowledged {
		t.Fatalf("close record=%v ok=%v", closeRecord, ok)
	}
	// With open and close both confirmed, isolation recovery is safe.
	if err := h.aborts.RecoverIsolation(session.ID); err != nil {
		t.Fatal(err)
	}
	if _, occupied := h.circuits.Get("C-A"); occupied {
		t.Fatal("circuit stayed reserved after recovery")
	}
}

func TestCloseTimeoutIsReconciledToConsistentState(t *testing.T) {
	h := newFillHarness(t)
	session, err := h.coordinator.StartAutomatic(context.Background(), "V-01", 2.1)
	if err != nil {
		t.Fatal(err)
	}
	h.transport.setFailClose(true)
	if _, err := h.aborts.Abort(context.Background(), session.ID, "emergency"); err == nil {
		t.Fatal("abort should report the close failure")
	}
	reservation, ok := h.circuits.Get("C-A")
	if !ok || !reservation.Faulted {
		t.Fatalf("reservation=%v ok=%v", reservation, ok)
	}
	closeCommand, ok := h.outbox.CommandFor(session.ID, model.ValveClose)
	if !ok {
		t.Fatal("close command was not recorded")
	}
	closeRecord, _ := h.outbox.Get(closeCommand.ID)
	if closeRecord.Status != model.ValveCommandPending {
		t.Fatalf("close status=%s", closeRecord.Status)
	}
	// Recovery is refused while the close outcome is unknown.
	if err := h.aborts.RecoverIsolation(session.ID); err == nil {
		t.Fatal("recovery should be refused while close is unconfirmed")
	}
	if _, err := h.reconciler.Reconcile(closeCommand.ID, true, "operator", "valve confirmed closed"); err != nil {
		t.Fatal(err)
	}
	if _, occupied := h.circuits.Get("C-A"); occupied {
		t.Fatal("circuit stayed reserved after close reconciliation")
	}
	if item, _ := h.vessels.Registry().Get("V-01"); item.Status != model.VesselReady {
		t.Fatalf("vessel status=%s", item.Status)
	}
}

func TestRecoverIsolationRefusedWhileOpenIsPending(t *testing.T) {
	h := newFillHarness(t)
	h.transport.setFailOpen(true)
	session, err := h.coordinator.StartAutomatic(context.Background(), "V-01", 2.1)
	if !errors.Is(err, ErrCommandUnconfirmed) {
		t.Fatalf("err=%v", err)
	}
	if err := h.aborts.RecoverIsolation(session.ID); !errors.Is(err, ErrCommandUnconfirmed) {
		t.Fatalf("expected unconfirmed error, got %v", err)
	}
	if _, occupied := h.circuits.Get("C-A"); !occupied {
		t.Fatal("circuit was freed while the open command is pending")
	}
}

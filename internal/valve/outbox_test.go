package valve

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

type scriptedTransport struct {
	mu       sync.Mutex
	failures int
	refuse   bool
	sent     []model.ValveCommand
}

func (t *scriptedTransport) Execute(_ context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sent = append(t.sent, command)
	if t.failures > 0 {
		t.failures--
		return model.ValveReceipt{}, errors.New("transport timeout")
	}
	if t.refuse {
		return model.ValveReceipt{
			CommandID:  command.ID,
			Executed:   false,
			Message:    "interlock active",
			ReceivedAt: time.Now().UTC(),
		}, nil
	}
	return model.ValveReceipt{
		CommandID:  command.ID,
		Executed:   true,
		Message:    "accepted",
		ReceivedAt: time.Now().UTC(),
	}, nil
}

func (t *scriptedTransport) sentCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.sent)
}

func newTestCommand(t *testing.T, sessionID uuid.UUID, action model.ValveAction) model.ValveCommand {
	t.Helper()
	command, err := model.NewValveCommand(
		model.ValveCommandIDFor(sessionID, action),
		sessionID,
		1,
		"VALVE-A",
		action,
		time.Now(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return command
}

func TestOutboxEnqueueIsIdempotent(t *testing.T) {
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := NewOutbox(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	sessionID := uuid.New()
	command := newTestCommand(t, sessionID, model.ValveOpen)
	if _, err := outbox.Enqueue(command); err != nil {
		t.Fatal(err)
	}
	// A retry restamps IssuedAt; identity must still match.
	command.IssuedAt = command.IssuedAt.Add(time.Second)
	again, err := outbox.Enqueue(command)
	if err != nil {
		t.Fatal(err)
	}
	if again.Command.ID != command.ID {
		t.Fatalf("command id changed across enqueue: %s", again.Command.ID)
	}
	if records := outbox.List(); len(records) != 1 {
		t.Fatalf("records=%d", len(records))
	}
	// Same ID with different content is an identity conflict.
	conflicting := command
	conflicting.ValveID = "VALVE-B"
	if _, err := outbox.Enqueue(conflicting); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("expected identity conflict, got %v", err)
	}
	// Same (session, action) under a different ID is rejected too.
	other := newTestCommand(t, sessionID, model.ValveOpen)
	other.ID = uuid.New()
	if _, err := outbox.Enqueue(other); !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("expected session/action conflict, got %v", err)
	}
}

func TestCommandServiceRetriesWithSameCommandID(t *testing.T) {
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := NewOutbox(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &scriptedTransport{failures: 1}
	controller, err := NewController(transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewCommandService(controller, outbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	command := newTestCommand(t, uuid.New(), model.ValveOpen)
	if _, err := service.Execute(context.Background(), command); err == nil {
		t.Fatal("first dispatch should fail")
	}
	record, ok := outbox.Get(command.ID)
	if !ok {
		t.Fatal("command was not recorded")
	}
	if record.Status != model.ValveCommandPending {
		t.Fatalf("status=%s", record.Status)
	}
	if len(record.Attempts) != 1 || record.Attempts[0].Error == "" {
		t.Fatalf("attempts=%v", record.Attempts)
	}
	receipt, err := service.Execute(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Executed {
		t.Fatal("retry was not executed")
	}
	if transport.sentCount() != 2 {
		t.Fatalf("sent=%d", transport.sentCount())
	}
	for _, sent := range transport.sent {
		if sent.ID != command.ID {
			t.Fatalf("retry used a new command id %s", sent.ID)
		}
	}
	record, _ = outbox.Get(command.ID)
	if record.Status != model.ValveCommandAcknowledged || record.Receipt == nil {
		t.Fatalf("record=%v", record)
	}
}

func TestCommandServiceNeverRedispatchesResolvedCommand(t *testing.T) {
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := NewOutbox(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &scriptedTransport{}
	controller, err := NewController(transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewCommandService(controller, outbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	command := newTestCommand(t, uuid.New(), model.ValveOpen)
	if _, err := service.Execute(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	receipt, err := service.Execute(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Duplicate {
		t.Fatal("resolved command should report a duplicate outcome")
	}
	if transport.sentCount() != 1 {
		t.Fatalf("resolved command was dispatched again: sent=%d", transport.sentCount())
	}
}

func TestOutboxRestoreRecoversPendingCommand(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := NewOutbox(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &scriptedTransport{failures: 1}
	controller, err := NewController(transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewCommandService(controller, outbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	command := newTestCommand(t, uuid.New(), model.ValveOpen)
	if _, err := service.Execute(context.Background(), command); err == nil {
		t.Fatal("dispatch should fail")
	}

	// Simulate a restart: reopen the journal and rebuild the outbox.
	reopened, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewOutbox(reopened, nil)
	if err != nil {
		t.Fatal(err)
	}
	events, err := reopened.Events(OutboxPartition, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(events); err != nil {
		t.Fatal(err)
	}
	pending := restored.Pending()
	if len(pending) != 1 || pending[0].Command.ID != command.ID {
		t.Fatalf("pending=%v", pending)
	}

	// Retrying after the restart must reuse the original command identity.
	recoveredTransport := &scriptedTransport{}
	recoveredController, err := NewController(recoveredTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	recoveredService, err := NewCommandService(recoveredController, restored, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recoveredService.Execute(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if recoveredTransport.sentCount() != 1 || recoveredTransport.sent[0].ID != command.ID {
		t.Fatalf("sent=%v", recoveredTransport.sent)
	}
	if pending := restored.Pending(); len(pending) != 0 {
		t.Fatalf("pending=%v", pending)
	}
}

func TestOutboxReconcileResolvesPendingCommand(t *testing.T) {
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := NewOutbox(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	command := newTestCommand(t, uuid.New(), model.ValveOpen)
	if _, err := outbox.Enqueue(command); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Reconcile(command.ID, true, "", "note"); err == nil {
		t.Fatal("reconciliation without operator should fail")
	}
	record, err := outbox.Reconcile(command.ID, true, "operator-1", "verified on site")
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != model.ValveCommandReconciled {
		t.Fatalf("status=%s", record.Status)
	}
	if record.Reconciliation == nil || !record.Reconciliation.Executed || record.Reconciliation.Operator != "operator-1" {
		t.Fatalf("reconciliation=%v", record.Reconciliation)
	}
	receipt, ok := record.Outcome()
	if !ok || !receipt.Executed {
		t.Fatalf("outcome=%v ok=%v", receipt, ok)
	}
	if _, err := outbox.Reconcile(uuid.New(), true, "operator-1", ""); !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestAcknowledgedCommandCannotBeReconciled(t *testing.T) {
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := NewOutbox(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &scriptedTransport{}
	controller, err := NewController(transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewCommandService(controller, outbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	command := newTestCommand(t, uuid.New(), model.ValveOpen)
	if _, err := service.Execute(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.Reconcile(command.ID, false, "operator-1", ""); !errors.Is(err, ErrCommandAlreadyResolved) {
		t.Fatalf("expected already resolved, got %v", err)
	}
}

func TestRefusedReceiptStaysPendingAndCanRedispatch(t *testing.T) {
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := NewOutbox(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport := &scriptedTransport{refuse: true}
	controller, err := NewController(transport, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewCommandService(controller, outbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	command := newTestCommand(t, uuid.New(), model.ValveClose)
	if _, err := service.Execute(context.Background(), command); !errors.Is(err, ErrCommandRejected) {
		t.Fatalf("expected rejection, got %v", err)
	}
	record, _ := outbox.Get(command.ID)
	if record.Status != model.ValveCommandPending {
		t.Fatalf("status=%s", record.Status)
	}
	if record.Receipt == nil || record.Receipt.Executed {
		t.Fatalf("receipt=%v", record.Receipt)
	}
	// Once the interlock clears, the same command identity is dispatched again.
	transport.refuse = false
	receipt, err := service.Execute(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Executed {
		t.Fatal("redispatch was not executed")
	}
	if transport.sentCount() != 2 || transport.sent[1].ID != command.ID {
		t.Fatalf("sent=%v", transport.sent)
	}
}

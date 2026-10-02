package circuit

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/journal"
)

func TestReservationJournalRestoresStateAcrossRestart(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager("C-A")
	manager.AttachJournal(store, time.Now)
	sessionID := uuid.New()
	reservation, err := manager.Reserve("C-A", "V-01", sessionID, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.RetainFault("C-A", sessionID); err != nil {
		t.Fatal(err)
	}

	// Simulate a restart: a fresh manager replays the journaled events.
	reopened, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	events, err := reopened.Events(ReservationPartition, 0)
	if err != nil {
		t.Fatal(err)
	}
	restored := NewManager("C-A")
	restored.AttachJournal(reopened, time.Now)
	if err := restored.Restore(events); err != nil {
		t.Fatal(err)
	}
	current, ok := restored.Get("C-A")
	if !ok {
		t.Fatal("reservation was lost across restart")
	}
	if current.SessionID != sessionID || !current.Faulted {
		t.Fatalf("reservation=%v", current)
	}
	// The circuit stays occupied for other sessions.
	if _, err := restored.Reserve("C-A", "V-02", uuid.New(), time.Now()); err != ErrCircuitBusy {
		t.Fatalf("expected busy circuit, got %v", err)
	}
	// Generations keep increasing instead of restarting at one.
	if err := restored.Recover("C-A", sessionID); err != nil {
		t.Fatal(err)
	}
	next, err := restored.Reserve("C-A", "V-02", uuid.New(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if next.Generation != reservation.Generation+1 {
		t.Fatalf("generation=%d want %d", next.Generation, reservation.Generation+1)
	}
}

func TestReservationReleaseIsJournaled(t *testing.T) {
	root := t.TempDir()
	store, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	manager := NewManager("C-A")
	manager.AttachJournal(store, time.Now)
	sessionID := uuid.New()
	if _, err := manager.Reserve("C-A", "V-01", sessionID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Release("C-A", sessionID); err != nil {
		t.Fatal(err)
	}
	reopened, err := journal.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	events, err := reopened.Events(ReservationPartition, 0)
	if err != nil {
		t.Fatal(err)
	}
	restored := NewManager("C-A")
	if err := restored.Restore(events); err != nil {
		t.Fatal(err)
	}
	if _, occupied := restored.Get("C-A"); occupied {
		t.Fatal("released reservation survived restart")
	}
}

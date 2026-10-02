package fill

import (
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
)

// ReconcileService is the manual reconciliation entry point for valve
// commands. It resolves pending commands in the outbox and then drives the
// fill session, circuit reservation, and vessel state to match the
// confirmed physical outcome, so the refill circuit is never freed while
// its valve may be open — nor held after the valve is confirmed closed.
type ReconcileService struct {
	coordinator *Coordinator
	outbox      *valve.Outbox
	now         func() time.Time
}

func NewReconcileService(coordinator *Coordinator, outbox *valve.Outbox, now func() time.Time) *ReconcileService {
	if now == nil {
		now = time.Now
	}
	return &ReconcileService{coordinator: coordinator, outbox: outbox, now: now}
}

// Pending lists commands with no confirmed outcome yet.
func (s *ReconcileService) Pending() []model.ValveCommandRecord {
	return s.outbox.Pending()
}

func (s *ReconcileService) List() []model.ValveCommandRecord {
	return s.outbox.List()
}

func (s *ReconcileService) Get(commandID uuid.UUID) (model.ValveCommandRecord, bool) {
	return s.outbox.Get(commandID)
}

// Reconcile records the operator-confirmed outcome of a command and
// reconciles the circuit reservation and fill session with it.
func (s *ReconcileService) Reconcile(commandID uuid.UUID, executed bool, operator, note string) (model.ValveCommandRecord, error) {
	record, err := s.outbox.Reconcile(commandID, executed, operator, note)
	if err != nil {
		return model.ValveCommandRecord{}, err
	}
	s.applyConsistency(record)
	return record, nil
}

func (s *ReconcileService) applyConsistency(record model.ValveCommandRecord) {
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	sessionID := record.Command.FillSessionID
	session, sessionKnown := s.coordinator.sessions[sessionID]
	reservation, reserved := s.coordinator.circuits.GetBySession(sessionID)
	executed := record.Reconciliation != nil && record.Reconciliation.Executed
	switch record.Command.Action {
	case model.ValveOpen:
		if executed {
			// The valve is physically open: the circuit must stay occupied
			// until a close is confirmed.
			if reserved && !reservation.Faulted {
				_ = s.coordinator.circuits.RetainFault(reservation.CircuitID, sessionID)
			}
			return
		}
		// The valve never opened: the limbo start can be rolled back.
		if reserved {
			_ = s.coordinator.circuits.Recover(reservation.CircuitID, sessionID)
		}
		s.coordinator.routes.Untrack(sessionID)
		if sessionKnown && session.Active() {
			failed := session.Failed("valve open reconciled: not executed", s.now())
			s.coordinator.finishState(failed)
			_, _ = s.coordinator.vessels.FinishFill(session.VesselID)
		}
	case model.ValveClose:
		if executed {
			// The valve is confirmed closed: the circuit is physically free.
			if reserved {
				_ = s.coordinator.circuits.Recover(reservation.CircuitID, sessionID)
			}
			s.coordinator.routes.Untrack(sessionID)
			if sessionKnown && !session.Active() {
				delete(s.coordinator.active, session.VesselID)
				_, _ = s.coordinator.vessels.FinishFill(session.VesselID)
			}
			return
		}
		// The valve may still be open: keep the circuit occupied.
		if reserved && !reservation.Faulted {
			_ = s.coordinator.circuits.RetainFault(reservation.CircuitID, sessionID)
		}
	}
}

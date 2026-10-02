package fill

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
)

type AbortService struct {
	coordinator *Coordinator
	outbox      *valve.Outbox
	now         func() time.Time
}

func NewAbortService(coordinator *Coordinator, outbox *valve.Outbox, now func() time.Time) *AbortService {
	if now == nil {
		now = time.Now
	}
	return &AbortService{coordinator: coordinator, outbox: outbox, now: now}
}

func (s *AbortService) Abort(ctx context.Context, sessionID uuid.UUID, reason string) (model.FillSession, error) {
	if reason == "" {
		return model.FillSession{}, errors.New("abort reason is required")
	}
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
	}
	if !session.Active() {
		return session, nil
	}
	if err := s.coordinator.routes.Close(ctx, session.ID); err != nil {
		_ = s.coordinator.circuits.RetainFault(session.CircuitID, session.ID)
		failed := session.Failed(reason+": "+err.Error(), s.now())
		s.coordinator.sessions[session.ID] = failed
		return failed, err
	}
	if err := s.coordinator.circuits.Release(session.CircuitID, session.ID); err != nil {
		return model.FillSession{}, err
	}
	failed := session.Failed(reason, s.now())
	if _, err := s.coordinator.vessels.FinishFill(session.VesselID); err != nil {
		return model.FillSession{}, err
	}
	s.coordinator.finishState(failed)
	return failed, nil
}

// RecoverIsolation frees a faulted circuit reservation, but only when the
// outbox proves the valve cannot still be open: a pending open command, or
// a confirmed-open valve without a confirmed close, blocks recovery until
// the commands are reconciled.
func (s *AbortService) RecoverIsolation(sessionID uuid.UUID) error {
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		return errors.New("fill session not found")
	}
	if err := s.valveStateSafe(sessionID); err != nil {
		return err
	}
	if err := s.coordinator.circuits.Recover(session.CircuitID, session.ID); err != nil {
		return err
	}
	delete(s.coordinator.active, session.VesselID)
	_, err := s.coordinator.vessels.FinishFill(session.VesselID)
	return err
}

func (s *AbortService) valveStateSafe(sessionID uuid.UUID) error {
	if s.outbox == nil {
		return nil
	}
	openCommand, ok := s.outbox.CommandFor(sessionID, model.ValveOpen)
	if !ok {
		return nil
	}
	openRecord, _ := s.outbox.Get(openCommand.ID)
	openReceipt, openKnown := openRecord.Outcome()
	if !openKnown {
		return fmt.Errorf("%w: open command %s is still pending", ErrCommandUnconfirmed, openCommand.ID)
	}
	if !openReceipt.Executed {
		return nil
	}
	closeCommand, ok := s.outbox.CommandFor(sessionID, model.ValveClose)
	if !ok {
		return errors.New("valve is confirmed open and no close command exists; reconcile before recovery")
	}
	closeRecord, _ := s.outbox.Get(closeCommand.ID)
	closeReceipt, closeKnown := closeRecord.Outcome()
	if !closeKnown || !closeReceipt.Executed {
		return fmt.Errorf("%w: valve is confirmed open but close is unconfirmed", ErrCommandUnconfirmed)
	}
	return nil
}

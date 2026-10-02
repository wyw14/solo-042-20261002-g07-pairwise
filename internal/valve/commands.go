package valve

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

// ErrCommandRejected means the valve controller definitively refused the
// command (receipt with Executed=false). Unlike a timeout this is a known
// outcome: the command did not act.
var ErrCommandRejected = errors.New("valve command was rejected by the controller")

type CommandService struct {
	controller *Controller
	outbox     *Outbox
	now        func() time.Time
}

func NewCommandService(controller *Controller, outbox *Outbox, now func() time.Time) (*CommandService, error) {
	if controller == nil || outbox == nil {
		return nil, errors.New("valve controller and command outbox are required")
	}
	if now == nil {
		now = time.Now
	}
	return &CommandService{controller: controller, outbox: outbox, now: now}, nil
}

// Execute runs a command through the persistent outbox: the command is
// journaled before dispatch, every attempt is recorded, and the receipt is
// stored when it arrives. A command whose goal is already confirmed
// executed is never dispatched again — its stored outcome is returned
// instead. A dispatch error leaves the command pending; the caller must
// not treat it as failed.
func (s *CommandService) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	record, err := s.outbox.Enqueue(command)
	if err != nil {
		return model.ValveReceipt{}, err
	}
	if receipt, ok := record.Outcome(); ok && receipt.Executed {
		receipt.Duplicate = true
		return receipt, nil
	}
	receipt, err := s.controller.Execute(ctx, command)
	if recordErr := s.outbox.RecordAttempt(command.ID, err); recordErr != nil {
		return model.ValveReceipt{}, recordErr
	}
	if err != nil {
		return model.ValveReceipt{}, err
	}
	if err := s.outbox.RecordReceipt(command.ID, receipt); err != nil {
		return model.ValveReceipt{}, err
	}
	if !receipt.Executed {
		return receipt, fmt.Errorf("%w: %s", ErrCommandRejected, receipt.Message)
	}
	return receipt, nil
}

func (s *CommandService) Send(ctx context.Context, commandID, sessionID uuid.UUID, circuitGeneration uint64, valveID string, action model.ValveAction) (model.ValveReceipt, error) {
	command, err := model.NewValveCommand(
		commandID,
		sessionID,
		circuitGeneration,
		valveID,
		action,
		s.now(),
	)
	if err != nil {
		return model.ValveReceipt{}, err
	}
	return s.Execute(ctx, command)
}

func (s *CommandService) Open(ctx context.Context, commandID, sessionID uuid.UUID, circuitGeneration uint64, valveID string) (model.ValveReceipt, error) {
	return s.Send(ctx, commandID, sessionID, circuitGeneration, valveID, model.ValveOpen)
}

func (s *CommandService) Close(ctx context.Context, commandID, sessionID uuid.UUID, circuitGeneration uint64, valveID string) (model.ValveReceipt, error) {
	return s.Send(ctx, commandID, sessionID, circuitGeneration, valveID, model.ValveClose)
}

// CommandFor exposes the persisted identity of one logical operation so
// callers can rebuild routes after a restart instead of minting new IDs.
func (s *CommandService) CommandFor(sessionID uuid.UUID, action model.ValveAction) (model.ValveCommand, bool) {
	return s.outbox.CommandFor(sessionID, action)
}

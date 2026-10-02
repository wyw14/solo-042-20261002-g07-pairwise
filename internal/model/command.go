package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type ValveAction string

const (
	ValveOpen  ValveAction = "open"
	ValveClose ValveAction = "close"
)

// valveCommandNamespace scopes deterministic valve command IDs to this
// deployment domain. It must never change once commands have been issued.
var valveCommandNamespace = uuid.MustParse("c07e1f5a-8b3d-4e2a-9f16-5d7a3b9c1e04")

// ValveCommandIDFor derives the stable command ID for one logical valve
// operation. The same (session, action) pair always yields the same ID, so
// retries and post-restart recovery reuse the original identity instead of
// minting a fresh one that could actuate the valve twice.
func ValveCommandIDFor(sessionID uuid.UUID, action ValveAction) uuid.UUID {
	return uuid.NewSHA1(valveCommandNamespace, []byte(sessionID.String()+":"+string(action)))
}

type ValveCommand struct {
	ID                uuid.UUID   `json:"id"`
	FillSessionID     uuid.UUID   `json:"fill_session_id"`
	CircuitGeneration uint64      `json:"circuit_generation"`
	ValveID           string      `json:"valve_id"`
	Action            ValveAction `json:"action"`
	IssuedAt          time.Time   `json:"issued_at"`
}

func NewValveCommand(id, sessionID uuid.UUID, generation uint64, valveID string, action ValveAction, now time.Time) (ValveCommand, error) {
	if id == uuid.Nil || sessionID == uuid.Nil || generation == 0 || valveID == "" {
		return ValveCommand{}, errors.New("invalid valve command identity")
	}
	if action != ValveOpen && action != ValveClose {
		return ValveCommand{}, errors.New("invalid valve action")
	}
	return ValveCommand{
		ID:                id,
		FillSessionID:     sessionID,
		CircuitGeneration: generation,
		ValveID:           valveID,
		Action:            action,
		IssuedAt:          now.UTC(),
	}, nil
}

type ValveReceipt struct {
	CommandID  uuid.UUID `json:"command_id"`
	Executed   bool      `json:"executed"`
	Duplicate  bool      `json:"duplicate"`
	Message    string    `json:"message"`
	ReceivedAt time.Time `json:"received_at"`
}

func (r ValveReceipt) Validate(command ValveCommand) error {
	if r.CommandID != command.ID {
		return errors.New("receipt command identity mismatch")
	}
	if !r.Executed && r.Message == "" {
		return errors.New("failed receipt requires a message")
	}
	return nil
}

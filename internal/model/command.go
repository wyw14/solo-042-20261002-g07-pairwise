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

type ValveCommand struct {
	ID                uuid.UUID   `json:"id"`
	FillSessionID     uuid.UUID   `json:"fill_session_id"`
	CircuitGeneration uint64      `json:"circuit_generation"`
	ValveID           string      `json:"valve_id"`
	Action            ValveAction `json:"action"`
	IssuedAt          time.Time   `json:"issued_at"`
}

// ValidateIdentity 校验命令身份字段，不依赖构造时间。
func (c ValveCommand) ValidateIdentity() error {
	if c.ID == uuid.Nil || c.FillSessionID == uuid.Nil || c.CircuitGeneration == 0 || c.ValveID == "" {
		return errors.New("invalid valve command identity")
	}
	if c.Action != ValveOpen && c.Action != ValveClose {
		return errors.New("invalid valve action")
	}
	return nil
}

func NewValveCommand(id, sessionID uuid.UUID, generation uint64, valveID string, action ValveAction, now time.Time) (ValveCommand, error) {
	command := ValveCommand{
		ID:                id,
		FillSessionID:     sessionID,
		CircuitGeneration: generation,
		ValveID:           valveID,
		Action:            action,
		IssuedAt:          now.UTC(),
	}
	if err := command.ValidateIdentity(); err != nil {
		return ValveCommand{}, err
	}
	return command, nil
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

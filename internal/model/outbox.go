package model

import (
	"time"
)

// ValveCommandStatus describes how far a persisted valve command has
// progressed toward a confirmed outcome.
//
//	pending      — recorded in the outbox; the valve's response is unknown.
//	acknowledged — a valid receipt was captured from the transport.
//	reconciled   — an operator confirmed the physical outcome manually.
type ValveCommandStatus string

const (
	ValveCommandPending      ValveCommandStatus = "pending"
	ValveCommandAcknowledged ValveCommandStatus = "acknowledged"
	ValveCommandReconciled   ValveCommandStatus = "reconciled"
)

// ValveCommandAttempt records one dispatch of a command to the transport.
// Error is empty when the dispatch produced a receipt; a non-empty Error
// means the outcome is unknown (timeout, transport failure) and must not be
// read as "the valve did not act".
type ValveCommandAttempt struct {
	At    time.Time `json:"at"`
	Error string    `json:"error,omitempty"`
}

// ValveCommandReconciliation captures a manual, operator-confirmed outcome
// for a command whose receipt never arrived.
type ValveCommandReconciliation struct {
	Executed bool      `json:"executed"`
	Operator string    `json:"operator"`
	Note     string    `json:"note,omitempty"`
	At       time.Time `json:"at"`
}

// ValveCommandRecord is the recoverable outbox state for one valve command:
// the request itself, every send attempt, and the receipt or manual
// reconciliation that resolved it.
type ValveCommandRecord struct {
	Command        ValveCommand                `json:"command"`
	Status         ValveCommandStatus          `json:"status"`
	Attempts       []ValveCommandAttempt       `json:"attempts,omitempty"`
	Receipt        *ValveReceipt               `json:"receipt,omitempty"`
	Reconciliation *ValveCommandReconciliation `json:"reconciliation,omitempty"`
	CreatedAt      time.Time                   `json:"created_at"`
	UpdatedAt      time.Time                   `json:"updated_at"`
}

// Resolved reports whether the command has a confirmed outcome, either from a
// transport receipt or from manual reconciliation.
func (r ValveCommandRecord) Resolved() bool {
	return r.Status == ValveCommandAcknowledged || r.Status == ValveCommandReconciled
}

// Outcome returns the effective receipt for a resolved command. Reconciled
// commands synthesize a receipt from the operator-confirmed outcome so
// callers can treat both resolution paths uniformly.
func (r ValveCommandRecord) Outcome() (ValveReceipt, bool) {
	switch r.Status {
	case ValveCommandAcknowledged:
		if r.Receipt == nil {
			return ValveReceipt{}, false
		}
		return *r.Receipt, true
	case ValveCommandReconciled:
		if r.Reconciliation == nil {
			return ValveReceipt{}, false
		}
		return ValveReceipt{
			CommandID:  r.Command.ID,
			Executed:   r.Reconciliation.Executed,
			Message:    "reconciled by " + r.Reconciliation.Operator,
			ReceivedAt: r.Reconciliation.At,
		}, true
	default:
		return ValveReceipt{}, false
	}
}

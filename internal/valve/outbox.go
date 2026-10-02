package valve

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

// OutboxPartition is the journal partition holding every valve command
// lifecycle event.
const OutboxPartition = "valve_commands"

const (
	eventCommandEnqueued   = "valve_command.enqueued"
	eventAttemptRecorded   = "valve_command.attempt_recorded"
	eventReceiptRecorded   = "valve_command.receipt_recorded"
	eventCommandReconciled = "valve_command.reconciled"
)

var (
	ErrCommandNotFound        = errors.New("valve command not found")
	ErrCommandConflict        = errors.New("valve command identity conflicts with an existing record")
	ErrCommandAlreadyResolved = errors.New("valve command already has a confirmed outcome")
)

// EventAppender is the slice of the journal store the outbox needs.
type EventAppender interface {
	Append(model.Event) (model.Event, error)
}

type enqueuePayload struct {
	Record model.ValveCommandRecord `json:"record"`
}

type attemptPayload struct {
	CommandID uuid.UUID                 `json:"command_id"`
	Attempt   model.ValveCommandAttempt `json:"attempt"`
}

type receiptPayload struct {
	CommandID uuid.UUID          `json:"command_id"`
	Receipt   model.ValveReceipt `json:"receipt"`
}

type reconcilePayload struct {
	CommandID      uuid.UUID                        `json:"command_id"`
	Reconciliation model.ValveCommandReconciliation `json:"reconciliation"`
}

// Outbox keeps every valve command, its send attempts, and its receipt (or
// manual reconciliation) as recoverable state. Mutations are journaled
// before they are applied in memory, so a restart replays to the same
// state; a command whose dispatch outcome is unknown stays pending instead
// of being assumed failed.
type Outbox struct {
	mu      sync.Mutex
	records map[uuid.UUID]model.ValveCommandRecord
	journal EventAppender
	now     func() time.Time
}

func NewOutbox(journal EventAppender, now func() time.Time) (*Outbox, error) {
	if journal == nil {
		return nil, errors.New("valve command journal is required")
	}
	if now == nil {
		now = time.Now
	}
	return &Outbox{
		records: make(map[uuid.UUID]model.ValveCommandRecord),
		journal: journal,
		now:     now,
	}, nil
}

// Enqueue records a command for dispatch. It is idempotent: re-enqueueing
// the same command identity returns the existing record, and a command ID
// or (session, action) pair that clashes with different content is rejected
// so one logical operation can never fan out into two physical commands.
func (o *Outbox) Enqueue(command model.ValveCommand) (model.ValveCommandRecord, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if existing, ok := o.records[command.ID]; ok {
		if !sameCommandIdentity(existing.Command, command) {
			return model.ValveCommandRecord{}, ErrCommandConflict
		}
		return existing, nil
	}
	for _, existing := range o.records {
		if existing.Command.FillSessionID == command.FillSessionID && existing.Command.Action == command.Action {
			return model.ValveCommandRecord{}, ErrCommandConflict
		}
	}
	now := o.now().UTC()
	record := model.ValveCommandRecord{
		Command:   command,
		Status:    model.ValveCommandPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := o.append(eventCommandEnqueued, command.ID.String(), enqueuePayload{Record: record}); err != nil {
		return model.ValveCommandRecord{}, err
	}
	o.records[command.ID] = record
	return record, nil
}

// RecordAttempt notes one dispatch of an unresolved command. A non-nil
// attemptErr means the outcome is unknown; the command stays pending.
func (o *Outbox) RecordAttempt(commandID uuid.UUID, attemptErr error) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	record, ok := o.records[commandID]
	if !ok {
		return ErrCommandNotFound
	}
	if record.Status == model.ValveCommandAcknowledged {
		return ErrCommandAlreadyResolved
	}
	attempt := model.ValveCommandAttempt{At: o.now().UTC()}
	if attemptErr != nil {
		attempt.Error = attemptErr.Error()
	}
	if err := o.append(eventAttemptRecorded, commandID.String(), attemptPayload{CommandID: commandID, Attempt: attempt}); err != nil {
		return err
	}
	record.Attempts = append(record.Attempts, attempt)
	record.UpdatedAt = attempt.At
	o.records[commandID] = record
	return nil
}

// RecordReceipt stores the transport receipt. A receipt reporting
// Executed=true acknowledges the command terminally; a refusal
// (Executed=false) is kept as the last known response while the command
// stays pending, so it can be re-dispatched with the same identity or
// reconciled manually.
func (o *Outbox) RecordReceipt(commandID uuid.UUID, receipt model.ValveReceipt) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	record, ok := o.records[commandID]
	if !ok {
		return ErrCommandNotFound
	}
	if record.Status == model.ValveCommandAcknowledged {
		return ErrCommandAlreadyResolved
	}
	if err := receipt.Validate(record.Command); err != nil {
		return err
	}
	if err := o.append(eventReceiptRecorded, commandID.String(), receiptPayload{CommandID: commandID, Receipt: receipt}); err != nil {
		return err
	}
	record.Receipt = &receipt
	if receipt.Executed {
		record.Status = model.ValveCommandAcknowledged
	}
	record.UpdatedAt = o.now().UTC()
	o.records[commandID] = record
	return nil
}

// Reconcile lets an operator confirm the physical outcome of a command
// whose receipt never arrived or reported a refusal. Reconciliation may be
// corrected by reconciling again; only an acknowledged (executed) receipt
// is terminal ground truth that cannot be overridden.
func (o *Outbox) Reconcile(commandID uuid.UUID, executed bool, operator, note string) (model.ValveCommandRecord, error) {
	if operator == "" {
		return model.ValveCommandRecord{}, errors.New("reconciliation operator is required")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	record, ok := o.records[commandID]
	if !ok {
		return model.ValveCommandRecord{}, ErrCommandNotFound
	}
	if record.Status == model.ValveCommandAcknowledged {
		return model.ValveCommandRecord{}, ErrCommandAlreadyResolved
	}
	reconciliation := model.ValveCommandReconciliation{
		Executed: executed,
		Operator: operator,
		Note:     note,
		At:       o.now().UTC(),
	}
	if err := o.append(eventCommandReconciled, commandID.String(), reconcilePayload{CommandID: commandID, Reconciliation: reconciliation}); err != nil {
		return model.ValveCommandRecord{}, err
	}
	record.Reconciliation = &reconciliation
	record.Status = model.ValveCommandReconciled
	record.UpdatedAt = reconciliation.At
	o.records[commandID] = record
	return record, nil
}

func (o *Outbox) Get(commandID uuid.UUID) (model.ValveCommandRecord, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record, ok := o.records[commandID]
	return record, ok
}

// CommandFor finds the command recorded for one logical operation,
// regardless of its status.
func (o *Outbox) CommandFor(sessionID uuid.UUID, action model.ValveAction) (model.ValveCommand, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, record := range o.records {
		if record.Command.FillSessionID == sessionID && record.Command.Action == action {
			return record.Command, true
		}
	}
	return model.ValveCommand{}, false
}

func (o *Outbox) List() []model.ValveCommandRecord {
	o.mu.Lock()
	defer o.mu.Unlock()
	return sortedRecords(o.records, nil)
}

// Pending lists commands with no confirmed outcome yet — the set operators
// must reconcile after timeouts or restarts.
func (o *Outbox) Pending() []model.ValveCommandRecord {
	o.mu.Lock()
	defer o.mu.Unlock()
	return sortedRecords(o.records, func(record model.ValveCommandRecord) bool {
		return record.Status == model.ValveCommandPending
	})
}

// Restore replays journaled events back into memory after a restart. It
// applies without journaling, mirroring the live mutation paths.
func (o *Outbox) Restore(events []model.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, event := range events {
		if err := o.apply(event); err != nil {
			return err
		}
	}
	return nil
}

func (o *Outbox) apply(event model.Event) error {
	switch event.Kind {
	case eventCommandEnqueued:
		var payload enqueuePayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		o.records[payload.Record.Command.ID] = payload.Record
	case eventAttemptRecorded:
		var payload attemptPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		record, ok := o.records[payload.CommandID]
		if !ok {
			return ErrCommandNotFound
		}
		record.Attempts = append(record.Attempts, payload.Attempt)
		record.UpdatedAt = payload.Attempt.At
		o.records[payload.CommandID] = record
	case eventReceiptRecorded:
		var payload receiptPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		record, ok := o.records[payload.CommandID]
		if !ok {
			return ErrCommandNotFound
		}
		receipt := payload.Receipt
		record.Receipt = &receipt
		if receipt.Executed {
			record.Status = model.ValveCommandAcknowledged
		}
		record.UpdatedAt = event.OccurredAt
		o.records[payload.CommandID] = record
	case eventCommandReconciled:
		var payload reconcilePayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		record, ok := o.records[payload.CommandID]
		if !ok {
			return ErrCommandNotFound
		}
		reconciliation := payload.Reconciliation
		record.Reconciliation = &reconciliation
		record.Status = model.ValveCommandReconciled
		record.UpdatedAt = reconciliation.At
		o.records[payload.CommandID] = record
	}
	return nil
}

func (o *Outbox) append(kind, aggregateID string, payload any) error {
	event, err := model.NewEvent(OutboxPartition, kind, aggregateID, payload, o.now())
	if err != nil {
		return err
	}
	_, err = o.journal.Append(event)
	return err
}

func sameCommandIdentity(a, b model.ValveCommand) bool {
	// IssuedAt is restamped on every dispatch attempt and is not part of
	// the command's identity.
	return a.ID == b.ID &&
		a.FillSessionID == b.FillSessionID &&
		a.CircuitGeneration == b.CircuitGeneration &&
		a.ValveID == b.ValveID &&
		a.Action == b.Action
}

func sortedRecords(records map[uuid.UUID]model.ValveCommandRecord, keep func(model.ValveCommandRecord) bool) []model.ValveCommandRecord {
	result := make([]model.ValveCommandRecord, 0, len(records))
	for _, record := range records {
		if keep == nil || keep(record) {
			result = append(result, record)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].Command.ID.String() < result[j].Command.ID.String()
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

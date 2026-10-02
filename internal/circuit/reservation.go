package circuit

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

var (
	ErrCircuitBusy         = errors.New("circuit is already reserved")
	ErrReservationMismatch = errors.New("circuit reservation does not match")
)

// ReservationPartition is the journal partition holding reservation
// lifecycle events.
const ReservationPartition = "circuits"

const (
	eventReserved  = "circuit.reserved"
	eventReleased  = "circuit.released"
	eventFaulted   = "circuit.faulted"
	eventRecovered = "circuit.recovered"
)

type Reservation struct {
	CircuitID  string    `json:"circuit_id"`
	SessionID  uuid.UUID `json:"session_id"`
	VesselID   string    `json:"vessel_id"`
	Generation uint64    `json:"generation"`
	ReservedAt time.Time `json:"reserved_at"`
	Faulted    bool      `json:"faulted"`
}

// EventAppender is the slice of the journal store the manager needs.
type EventAppender interface {
	Append(model.Event) (model.Event, error)
}

type reservationRefPayload struct {
	CircuitID string    `json:"circuit_id"`
	SessionID uuid.UUID `json:"session_id"`
}

type Manager struct {
	mu           sync.Mutex
	reservations map[string]Reservation
	generations  map[string]uint64
	journal      EventAppender
	now          func() time.Time
}

func NewManager(circuitIDs ...string) *Manager {
	m := &Manager{
		reservations: make(map[string]Reservation),
		generations:  make(map[string]uint64),
	}
	for _, id := range circuitIDs {
		if id != "" {
			m.generations[id] = 0
		}
	}
	return m
}

// AttachJournal makes reservation mutations durable: every change is
// journaled before it is applied, so a restart replays to the same
// reservation state instead of freeing circuits whose valves may still be
// physically open.
func (m *Manager) AttachJournal(journal EventAppender, now func() time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.journal = journal
	m.now = now
}

func (m *Manager) Reserve(circuitID, vesselID string, sessionID uuid.UUID, now time.Time) (Reservation, error) {
	if circuitID == "" || vesselID == "" || sessionID == uuid.Nil {
		return Reservation{}, errors.New("invalid circuit reservation")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, occupied := m.reservations[circuitID]; occupied {
		return Reservation{}, ErrCircuitBusy
	}
	generation := m.generations[circuitID] + 1
	reservation := Reservation{
		CircuitID:  circuitID,
		SessionID:  sessionID,
		VesselID:   vesselID,
		Generation: generation,
		ReservedAt: now.UTC(),
	}
	if err := m.append(eventReserved, circuitID, reservation); err != nil {
		return Reservation{}, err
	}
	m.generations[circuitID] = generation
	m.reservations[circuitID] = reservation
	return reservation, nil
}

func (m *Manager) Release(circuitID string, sessionID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.reservations[circuitID]
	if !ok {
		return nil
	}
	if current.SessionID != sessionID {
		return ErrReservationMismatch
	}
	if current.Faulted {
		return errors.New("faulted circuit requires explicit recovery")
	}
	if err := m.append(eventReleased, circuitID, reservationRefPayload{CircuitID: circuitID, SessionID: sessionID}); err != nil {
		return err
	}
	delete(m.reservations, circuitID)
	return nil
}

func (m *Manager) RetainFault(circuitID string, sessionID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.reservations[circuitID]
	if !ok || current.SessionID != sessionID {
		return ErrReservationMismatch
	}
	if current.Faulted {
		return nil
	}
	if err := m.append(eventFaulted, circuitID, reservationRefPayload{CircuitID: circuitID, SessionID: sessionID}); err != nil {
		return err
	}
	current.Faulted = true
	m.reservations[circuitID] = current
	return nil
}

func (m *Manager) Recover(circuitID string, sessionID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.reservations[circuitID]
	if !ok || current.SessionID != sessionID {
		return ErrReservationMismatch
	}
	if err := m.append(eventRecovered, circuitID, reservationRefPayload{CircuitID: circuitID, SessionID: sessionID}); err != nil {
		return err
	}
	delete(m.reservations, circuitID)
	return nil
}

func (m *Manager) Get(circuitID string) (Reservation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	reservation, ok := m.reservations[circuitID]
	return reservation, ok
}

// GetBySession finds the reservation held by a fill session, regardless of
// which circuit it occupies.
func (m *Manager) GetBySession(sessionID uuid.UUID) (Reservation, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, reservation := range m.reservations {
		if reservation.SessionID == sessionID {
			return reservation, true
		}
	}
	return Reservation{}, false
}

// Restore replays journaled reservation events back into memory after a
// restart. Generation counters are lifted to the highest generation seen so
// a reused circuit never reissues a stale generation.
func (m *Manager) Restore(events []model.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, event := range events {
		switch event.Kind {
		case eventReserved:
			var reservation Reservation
			if err := json.Unmarshal(event.Payload, &reservation); err != nil {
				return err
			}
			m.reservations[reservation.CircuitID] = reservation
			if reservation.Generation > m.generations[reservation.CircuitID] {
				m.generations[reservation.CircuitID] = reservation.Generation
			}
		case eventReleased, eventRecovered:
			var ref reservationRefPayload
			if err := json.Unmarshal(event.Payload, &ref); err != nil {
				return err
			}
			delete(m.reservations, ref.CircuitID)
		case eventFaulted:
			var ref reservationRefPayload
			if err := json.Unmarshal(event.Payload, &ref); err != nil {
				return err
			}
			if current, ok := m.reservations[ref.CircuitID]; ok && current.SessionID == ref.SessionID {
				current.Faulted = true
				m.reservations[ref.CircuitID] = current
			}
		}
	}
	return nil
}

func (m *Manager) append(kind, aggregateID string, payload any) error {
	if m.journal == nil {
		return nil
	}
	now := m.now
	if now == nil {
		now = time.Now
	}
	event, err := model.NewEvent(ReservationPartition, kind, aggregateID, payload, now())
	if err != nil {
		return err
	}
	_, err = m.journal.Append(event)
	return err
}

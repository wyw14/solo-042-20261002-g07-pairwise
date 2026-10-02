package circuit

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	ErrCircuitBusy         = errors.New("circuit is already reserved")
	ErrReservationMismatch = errors.New("circuit reservation does not match")
)

type Reservation struct {
	CircuitID  string    `json:"circuit_id"`
	SessionID  uuid.UUID `json:"session_id"`
	VesselID   string    `json:"vessel_id"`
	Generation uint64    `json:"generation"`
	ReservedAt time.Time `json:"reserved_at"`
	Faulted    bool      `json:"faulted"`
}

type Manager struct {
	mu           sync.Mutex
	reservations map[string]Reservation
	generations  map[string]uint64
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

func (m *Manager) Reserve(circuitID, vesselID string, sessionID uuid.UUID, now time.Time) (Reservation, error) {
	if circuitID == "" || vesselID == "" || sessionID == uuid.Nil {
		return Reservation{}, errors.New("invalid circuit reservation")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, occupied := m.reservations[circuitID]; occupied {
		return Reservation{}, ErrCircuitBusy
	}
	m.generations[circuitID]++
	reservation := Reservation{
		CircuitID:  circuitID,
		SessionID:  sessionID,
		VesselID:   vesselID,
		Generation: m.generations[circuitID],
		ReservedAt: now.UTC(),
	}
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
	current.Faulted = true
	m.reservations[circuitID] = current
	return nil
}

// ReleaseConfirmed 在关阀命令已被设备确认执行后释放预留，
// 即使预留此前因结果未知被标记为故障。调用方必须持有“阀门已确认关闭”这一前提。
func (m *Manager) ReleaseConfirmed(circuitID string, sessionID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.reservations[circuitID]
	if !ok {
		return nil
	}
	if current.SessionID != sessionID {
		return ErrReservationMismatch
	}
	delete(m.reservations, circuitID)
	return nil
}

func (m *Manager) Recover(circuitID string, sessionID uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.reservations[circuitID]
	if !ok || current.SessionID != sessionID {
		return ErrReservationMismatch
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

// RestoreReservation 在进程重启重放后重建回路预留。
// 同一回路已被占用且归属不同会话时返回错误，避免恢复出互相矛盾的预留。
func (m *Manager) RestoreReservation(reservation Reservation) error {
	if reservation.CircuitID == "" || reservation.SessionID == uuid.Nil || reservation.Generation == 0 {
		return errors.New("invalid circuit reservation")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.reservations[reservation.CircuitID]; ok {
		if existing.SessionID != reservation.SessionID {
			return ErrCircuitBusy
		}
		if existing.Generation > reservation.Generation {
			return errors.New("restored reservation generation moved backward")
		}
	}
	if generation := m.generations[reservation.CircuitID]; reservation.Generation > generation {
		m.generations[reservation.CircuitID] = reservation.Generation
	}
	m.reservations[reservation.CircuitID] = reservation
	return nil
}

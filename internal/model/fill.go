package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type FillState string

const (
	FillCreated     FillState = "created"
	FillPrechecking FillState = "prechecking"
	// FillAwaitingValve 表示开阀命令已持久化发出但结果未知（超时/重启）。
	// 此状态下回路预留与容器 filling 状态必须保留，等待同 ID 补发回执或人工对账。
	FillAwaitingValve FillState = "awaiting_valve"
	FillFilling       FillState = "filling"
	FillSoaking       FillState = "soaking"
	FillVerified      FillState = "verified"
	FillCompleted     FillState = "completed"
	FillCancelled     FillState = "cancelled"
	FillFailed        FillState = "failed"
)

type FillOrigin string

const (
	FillAutomatic FillOrigin = "automatic"
	FillManual    FillOrigin = "manual"
)

type FillSession struct {
	ID            uuid.UUID  `json:"id"`
	VesselID      string     `json:"vessel_id"`
	Origin        FillOrigin `json:"origin"`
	State         FillState  `json:"state"`
	Generation    uint64     `json:"generation"`
	CircuitID     string     `json:"circuit_id"`
	PlannedLiters float64    `json:"planned_liters"`
	Failure       string     `json:"failure,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func NewFillSession(vesselID string, origin FillOrigin, generation uint64, liters float64, now time.Time) (FillSession, error) {
	if vesselID == "" || generation == 0 || liters <= 0 {
		return FillSession{}, errors.New("invalid fill session parameters")
	}
	if origin != FillAutomatic && origin != FillManual {
		return FillSession{}, errors.New("invalid fill origin")
	}
	return FillSession{
		ID:            uuid.New(),
		VesselID:      vesselID,
		Origin:        origin,
		State:         FillCreated,
		Generation:    generation,
		PlannedLiters: liters,
		CreatedAt:     now.UTC(),
		UpdatedAt:     now.UTC(),
	}, nil
}

// NewRecoveredFillSession 在进程重启后依据持久化的阀门通路/命令记录重建会话视图。
// 重启时无法获知命令是否已在设备侧执行，因此恢复出的会话一律处于 awaiting_valve，
// 必须等待同 ID 补发回执或人工对账后才能继续推进。
func NewRecoveredFillSession(id uuid.UUID, vesselID string, origin FillOrigin, generation uint64, liters float64, circuitID string, now time.Time) (FillSession, error) {
	if id == uuid.Nil || vesselID == "" || generation == 0 || liters <= 0 || circuitID == "" {
		return FillSession{}, errors.New("invalid recovered fill session parameters")
	}
	if origin != FillAutomatic && origin != FillManual {
		return FillSession{}, errors.New("invalid fill origin")
	}
	return FillSession{
		ID:            id,
		VesselID:      vesselID,
		Origin:        origin,
		State:         FillAwaitingValve,
		Generation:    generation,
		CircuitID:     circuitID,
		PlannedLiters: liters,
		CreatedAt:     now.UTC(),
		UpdatedAt:     now.UTC(),
	}, nil
}

func (s FillSession) Active() bool {
	switch s.State {
	case FillCreated, FillPrechecking, FillAwaitingValve, FillFilling, FillSoaking, FillVerified:
		return true
	default:
		return false
	}
}

func (s FillSession) Transition(next FillState, now time.Time) (FillSession, error) {
	allowed := map[FillState]map[FillState]bool{
		FillCreated:       {FillPrechecking: true, FillCancelled: true, FillFailed: true},
		FillPrechecking:   {FillFilling: true, FillAwaitingValve: true, FillCancelled: true, FillFailed: true},
		FillAwaitingValve: {FillFilling: true, FillCancelled: true, FillFailed: true},
		FillFilling:       {FillSoaking: true, FillCancelled: true, FillFailed: true},
		FillSoaking:       {FillVerified: true, FillFailed: true},
		FillVerified:      {FillCompleted: true, FillFailed: true},
	}
	if !allowed[s.State][next] {
		return FillSession{}, errors.New("invalid fill state transition")
	}
	s.State = next
	s.UpdatedAt = now.UTC()
	return s, nil
}

func (s FillSession) WithCircuit(id string, now time.Time) FillSession {
	s.CircuitID = id
	s.UpdatedAt = now.UTC()
	return s
}

func (s FillSession) Failed(message string, now time.Time) FillSession {
	s.State = FillFailed
	s.Failure = message
	s.UpdatedAt = now.UTC()
	return s
}

// Conclude 用于命令对账/恢复：根据人工结论或重放的命令结果把会话置为终态。
// 它显式区别于正常工艺流转 Transition，只能在对账场景调用。
func (s FillSession) Conclude(state FillState, reason string, now time.Time) (FillSession, error) {
	switch state {
	case FillCompleted, FillCancelled, FillFailed:
	default:
		return FillSession{}, errors.New("reconciliation target must be a terminal state")
	}
	s.State = state
	if reason != "" {
		s.Failure = reason
	}
	s.UpdatedAt = now.UTC()
	return s, nil
}

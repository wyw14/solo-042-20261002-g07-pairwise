package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/pressure"
)

type CompletionService struct {
	coordinator *Coordinator
	pressure    *pressure.Monitor
	journal     EventAppender
	now         func() time.Time
}

type EventAppender interface {
	Append(model.Event) (model.Event, error)
}

func NewCompletionService(coordinator *Coordinator, pressureMonitor *pressure.Monitor, store EventAppender, now func() time.Time) *CompletionService {
	if now == nil {
		now = time.Now
	}
	return &CompletionService{coordinator: coordinator, pressure: pressureMonitor, journal: store, now: now}
}

func (s *CompletionService) Complete(ctx context.Context, sessionID uuid.UUID) (model.FillSession, error) {
	if err := ctx.Err(); err != nil {
		return model.FillSession{}, err
	}
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
	}
	if session.State != model.FillSoaking {
		return model.FillSession{}, errors.New("fill session is not soaking")
	}
	result, err := s.pressure.Evaluate(session.VesselID)
	if err != nil {
		return model.FillSession{}, err
	}
	if !result.Stable {
		return model.FillSession{}, errors.New("pressure soak window is not stable")
	}
	event, err := model.NewEvent("fills", "fill.soak_verified", session.ID.String(), result, s.now())
	if err != nil {
		return model.FillSession{}, err
	}
	if _, err := s.journal.Append(event); err != nil {
		return model.FillSession{}, err
	}
	verified, err := session.Transition(model.FillVerified, s.now())
	if err != nil {
		return model.FillSession{}, err
	}

	route, ok, err := s.coordinator.routes.Get(session.ID)
	if err != nil {
		return model.FillSession{}, err
	}
	if !ok {
		return model.FillSession{}, errors.New("route is not open")
	}
	if err := s.coordinator.recordCloseIntentLocked(verified, model.FillCompleted, "", route.CloseCommandID); err != nil {
		return model.FillSession{}, err
	}

	closeState, err := s.coordinator.routes.Close(ctx, session.ID)
	if err != nil {
		return model.FillSession{}, err
	}
	switch {
	case closeState.KnownExecuted():
		return s.coordinator.closeOut(verified, model.FillCompleted, "")
	default:
		// 关阀未确认执行（结果未知，或设备明确未执行——阀门可能仍开着）：
		// 一律保留故障预留与容器 filling，会话保持 active，等待同 ID 补发回执，
		// 补发耗尽后由外发箱转 awaiting 人工对账；绝不能把仍可能通液的回路释放。
		_ = s.coordinator.circuits.RetainFault(session.CircuitID, session.ID)
		held := verified
		s.coordinator.sessions[session.ID] = held
		return held, ErrCommandUnconfirmed
	}
}

package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

type AbortService struct {
	coordinator *Coordinator
	now         func() time.Time
}

func NewAbortService(coordinator *Coordinator, now func() time.Time) *AbortService {
	if now == nil {
		now = time.Now
	}
	return &AbortService{coordinator: coordinator, now: now}
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

	// 开阀本身尚未确认（awaiting_valve）：没有证据表明阀门已开，
	// 但也不能凭超时认定没开。记录中止意图并保留故障预留，转人工/恢复收敛。
	if session.State == model.FillAwaitingValve {
		route, ok, err := s.coordinator.routes.Get(session.ID)
		if err != nil {
			return model.FillSession{}, err
		}
		if !ok {
			return model.FillSession{}, errors.New("route is not open")
		}
		// 记录关阀意图：若之后确认开阀其实已执行，恢复流程会据此补发关阀命令。
		if err := s.coordinator.recordCloseIntentLocked(session, model.FillFailed, reason, route.CloseCommandID); err != nil {
			return model.FillSession{}, err
		}
		held := session.Failed("abort held: "+reason+": open valve command unconfirmed", s.now())
		// Failed 是非活动终态，但回路仍保留；标记为故障等待人工/恢复处理。
		_ = s.coordinator.circuits.RetainFault(session.CircuitID, session.ID)
		s.coordinator.sessions[session.ID] = held
		return held, ErrCommandUnconfirmed
	}

	route, ok, err := s.coordinator.routes.Get(session.ID)
	if err != nil {
		return model.FillSession{}, err
	}
	if !ok {
		return model.FillSession{}, errors.New("route is not open")
	}
	if err := s.coordinator.recordCloseIntentLocked(session, model.FillFailed, reason, route.CloseCommandID); err != nil {
		return model.FillSession{}, err
	}

	closeState, err := s.coordinator.routes.Close(ctx, session.ID)
	if err != nil {
		return model.FillSession{}, err
	}
	if !closeState.KnownExecuted() {
		// 关阀结果未知或被拒：阀门可能仍开着，保留故障预留与容器 filling，
		// 等待同 ID 补发回执，补发耗尽后转 awaiting 人工对账。
		_ = s.coordinator.circuits.RetainFault(session.CircuitID, session.ID)
		failed := session.Failed(reason+": close valve command unconfirmed", s.now())
		s.coordinator.sessions[session.ID] = failed
		return failed, ErrCommandUnconfirmed
	}

	return s.coordinator.closeOut(session, model.FillFailed, reason)
}

// RecoverIsolation 在人工现场处理、确认安全后释放被保留的故障回路。
func (s *AbortService) RecoverIsolation(sessionID uuid.UUID) error {
	s.coordinator.mu.Lock()
	defer s.coordinator.mu.Unlock()
	session, ok := s.coordinator.sessions[sessionID]
	if !ok {
		return errors.New("fill session not found")
	}
	if err := s.coordinator.circuits.Recover(session.CircuitID, session.ID); err != nil {
		return err
	}
	if _, err := s.coordinator.vessels.FinishFill(session.VesselID); err != nil {
		return err
	}
	_ = s.coordinator.routes.MarkClosed(session.ID)
	delete(s.coordinator.active, session.VesselID)
	return nil
}

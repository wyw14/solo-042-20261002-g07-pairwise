package fill

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
)

// restoreOpenRoute 在重放后重建一条开通路对应的回路预留、容器状态与会话视图。
// 不假设命令结果：会话一律进入 awaiting_valve。调用方无需持锁（方法内部加锁）。
func (c *Coordinator) restoreOpenRoute(route valve.Route, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, faulted := c.closeIntents[route.SessionID]
	reservation := circuit.Reservation{
		CircuitID:  route.CircuitID,
		SessionID:  route.SessionID,
		VesselID:   route.VesselID,
		Generation: route.CircuitGeneration,
		ReservedAt: now.UTC(),
		Faulted:    faulted,
	}
	if err := c.circuits.RestoreReservation(reservation); err != nil {
		return err
	}
	// 容器可能已经是 filling（同次进程内）；BeginFill 对 filling 幂等。
	if _, err := c.vessels.BeginFill(route.VesselID); err != nil {
		return err
	}
	if _, exists := c.sessions[route.SessionID]; !exists {
		session, err := model.NewRecoveredFillSession(
			route.SessionID,
			route.VesselID,
			route.Origin,
			1,
			route.PlannedLiters,
			route.CircuitID,
			now,
		)
		if err != nil {
			return err
		}
		c.sessions[route.SessionID] = session
		c.active[route.VesselID] = route.SessionID
	}
	return nil
}

func (c *Coordinator) setCloseIntents(intents map[uuid.UUID]closeIntent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeIntents = intents
}

// reconcileSession 根据外发箱中开/关阀命令的确定结论收敛会话与回路预留。
// 没有明确结论时保持现状（awaiting），绝不臆测成功或失败。调用方无需持锁。
func (c *Coordinator) reconcileSession(ctx context.Context, sessionID uuid.UUID) (model.FillSession, bool, error) {
	_ = ctx
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[sessionID]
	if !ok {
		return model.FillSession{}, false, nil
	}
	route, routeOpen, err := c.routes.Get(sessionID)
	if err != nil {
		return model.FillSession{}, false, err
	}
	openState, err := c.routes.Outbox().Get(route.OpenCommandID)
	if err != nil {
		return model.FillSession{}, false, err
	}

	// 开阀确认未驱动阀门：释放预留，会话失败，通路关闭。
	if openState.KnownNotExecuted() {
		final, err := session.Conclude(model.FillFailed, "open valve confirmed not executed: "+openFailureDetail(openState), c.now())
		if err != nil {
			return model.FillSession{}, false, err
		}
		if err := c.circuits.ReleaseConfirmed(session.CircuitID, session.ID); err != nil {
			return model.FillSession{}, false, err
		}
		if _, err := c.vessels.FinishFill(session.VesselID); err != nil {
			return model.FillSession{}, false, err
		}
		_ = c.routes.MarkClosed(session.ID)
		delete(c.closeIntents, session.ID)
		c.finishState(final)
		return final, true, nil
	}

	// 开阀确认已执行：awaiting_valve 的会话先回到 filling。
	if openState.KnownExecuted() && session.State == model.FillAwaitingValve {
		session, err = session.Transition(model.FillFilling, c.now())
		if err != nil {
			return model.FillSession{}, false, err
		}
		c.sessions[sessionID] = session
	}

	// 关阀命令：以明确的“已执行”回执为释放回路的唯一依据。
	intent, intended := c.closeIntents[session.ID]
	closeState, closeErr := c.routes.Outbox().Get(route.CloseCommandID)
	if closeErr != nil {
		// 关阀命令尚未登记。
		if !intended || !openState.KnownExecuted() {
			// 既无关阀意图、开阀又未确认：无需关阀，保持 awaiting。
			return session, false, nil
		}
		// 存在关阀意图且开阀已确认（例如崩溃于补发开阀之后）：用固定关阀 ID 补发。
		dispatched, err := c.routes.Close(ctx, session.ID)
		if err != nil {
			return model.FillSession{}, false, err
		}
		closeState = dispatched
	}

	if closeState.KnownExecuted() && routeOpen {
		terminal := model.FillCompleted
		reason := ""
		if intended {
			terminal = intent.Terminal
			reason = intent.Reason
		} else if session.State != model.FillVerified {
			// 没有完成意图但关阀已执行（例如崩溃于中止流程）：按失败安全收敛。
			terminal = model.FillFailed
			reason = "valve closed during recovery without completion"
		}
		final, err := c.closeOut(session, terminal, reason)
		if err != nil {
			return model.FillSession{}, false, err
		}
		delete(c.closeIntents, session.ID)
		return final, true, nil
	}

	// 关阀意图存在但关阀未确认：保持故障预留，等待补发回执或人工对账。
	if intended && !closeState.KnownExecuted() {
		_ = c.circuits.RetainFault(session.CircuitID, session.ID)
	}
	return session, false, nil
}

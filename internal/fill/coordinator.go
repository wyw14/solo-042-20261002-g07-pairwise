package fill

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

var (
	ErrActiveSession = errors.New("vessel already has an active fill session")
	// ErrCommandUnconfirmed 复用外发箱的未确认哨兵：命令结果未知，
	// 既不能宣告成功也不能宣告失败，回路预留必须保留。
	ErrCommandUnconfirmed = valve.ErrAwaitingConfirmation
)

// Coordinator 协调补液会话、回路预留、容器状态与持久阀门命令外发箱。
//
// 关键不变量：只有拿到阀门明确回执，或人工对账给出结论后，才推进/回滚
// 回路预留与容器状态。开阀结果未知时，预留与容器 filling 状态一律保留，
// 会话进入 awaiting_valve，等待恢复或对账。
type Coordinator struct {
	mu          sync.RWMutex
	sessions    map[uuid.UUID]model.FillSession
	active      map[string]uuid.UUID
	circuits    *circuit.Manager
	circuitList *circuit.Registry
	routes      *valve.RouteService
	vessels     *vessel.StateService
	journal     EventAppender
	// closeIntents 记录已进入完成/中止流程、等待关阀确认的会话目标终态。
	// 与事件 fill.close_intended 一致，供重启后按原意图收敛。
	closeIntents map[uuid.UUID]closeIntent
	now          func() time.Time
}

type closeIntent struct {
	Terminal model.FillState
	Reason   string
}

func NewCoordinator(circuits *circuit.Manager, circuitList *circuit.Registry, routes *valve.RouteService, vessels *vessel.StateService, journal EventAppender, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{
		sessions:     make(map[uuid.UUID]model.FillSession),
		active:       make(map[string]uuid.UUID),
		circuits:     circuits,
		circuitList:  circuitList,
		routes:       routes,
		vessels:      vessels,
		journal:      journal,
		closeIntents: make(map[uuid.UUID]closeIntent),
		now:          now,
	}
}

type closeIntentPayload struct {
	Terminal       model.FillState `json:"terminal"`
	Reason         string          `json:"reason,omitempty"`
	CloseCommandID uuid.UUID       `json:"close_command_id"`
}

// recordCloseIntentLocked 在下发关阀命令前持久化目标终态，
// 这样即使进程在关阀确认前崩溃，重启后仍能按同一意图收敛。调用方持有 c.mu。
func (c *Coordinator) recordCloseIntentLocked(session model.FillSession, terminal model.FillState, reason string, closeCommandID uuid.UUID) error {
	if _, exists := c.closeIntents[session.ID]; exists {
		return nil
	}
	intent := closeIntent{Terminal: terminal, Reason: reason}
	if c.journal != nil {
		event, err := model.NewEvent("fills", "fill.close_intended", session.ID.String(),
			closeIntentPayload{Terminal: terminal, Reason: reason, CloseCommandID: closeCommandID}, c.now())
		if err != nil {
			return err
		}
		if _, err := c.journal.Append(event); err != nil {
			return err
		}
	}
	c.closeIntents[session.ID] = intent
	return nil
}

// StartResult 描述一次启动补液的结果。当开阀结论未知时 Awaiting 为 true，
// Session 处于 awaiting_valve，调用方应返回 202 而非视为失败。
type StartResult struct {
	Session  model.FillSession
	Awaiting bool
}

func (c *Coordinator) Start(ctx context.Context, vesselID string, origin model.FillOrigin, liters float64) (StartResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.active[vesselID]; exists {
		return StartResult{}, ErrActiveSession
	}
	selected, err := c.selectCircuit()
	if err != nil {
		return StartResult{}, err
	}
	session, err := model.NewFillSession(vesselID, origin, 1, liters, c.now())
	if err != nil {
		return StartResult{}, err
	}
	reservation, err := c.circuits.Reserve(selected.ID, vesselID, session.ID, c.now())
	if err != nil {
		return StartResult{}, err
	}
	session = session.WithCircuit(selected.ID, c.now())
	session, err = session.Transition(model.FillPrechecking, c.now())
	if err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return StartResult{}, err
	}
	if _, err := c.vessels.BeginFill(vesselID); err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return StartResult{}, err
	}

	route := valve.Route{
		SessionID:         session.ID,
		VesselID:          vesselID,
		Origin:            origin,
		PlannedLiters:     liters,
		CircuitID:         selected.ID,
		CircuitGeneration: reservation.Generation,
		ValveID:           selected.ValveID,
		OpenCommandID:     uuid.New(),
		CloseCommandID:    uuid.New(),
	}

	// 开阀命令经由持久外发箱发出。同 ID 重试、跨重启补发都由外发箱保证幂等。
	openState, err := c.routes.Open(ctx, route)
	if err != nil {
		// 只有“登记/下发前”的确定性错误才回滚预留；此时命令未进入外发箱。
		_, _ = c.vessels.FinishFill(vesselID)
		_ = c.circuits.Release(selected.ID, session.ID)
		return StartResult{}, err
	}

	switch {
	case openState.KnownExecuted():
		session, err = session.Transition(model.FillFilling, c.now())
		if err != nil {
			return StartResult{}, err
		}
		c.sessions[session.ID] = session
		c.active[vesselID] = session.ID
		return StartResult{Session: session}, nil
	case openState.KnownNotExecuted():
		// 设备明确拒绝/未执行：这是唯一可以安全释放预留的失败情形。
		_, _ = c.vessels.FinishFill(vesselID)
		_ = c.circuits.Release(selected.ID, session.ID)
		_ = c.routes.MarkClosed(session.ID)
		failed := session.Failed("valve open rejected: "+openFailureDetail(openState), c.now())
		c.sessions[session.ID] = failed
		return StartResult{}, errors.New(failed.Failure)
	default:
		// 结果未知（超时/重启/无回执）：绝不认定失败、绝不换 ID 重开。
		// 命令保留 inflight，由恢复循环用同一 ID 自动补发；
		// 同时保留回路预留与容器 filling，会话挂起等待确认。
		awaiting, transitionErr := session.Transition(model.FillAwaitingValve, c.now())
		if transitionErr != nil {
			return StartResult{}, transitionErr
		}
		c.sessions[session.ID] = awaiting
		c.active[vesselID] = session.ID
		return StartResult{Session: awaiting, Awaiting: true}, nil
	}
}

func (c *Coordinator) StartAutomatic(ctx context.Context, vesselID string, liters float64) (StartResult, error) {
	return c.Start(ctx, vesselID, model.FillAutomatic, liters)
}

func (c *Coordinator) StartManual(ctx context.Context, vesselID string, liters float64) (StartResult, error) {
	return c.Start(ctx, vesselID, model.FillManual, liters)
}

func (c *Coordinator) BeginSoak(sessionID uuid.UUID) (model.FillSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
	}
	if session.State != model.FillFilling {
		return model.FillSession{}, errors.New("fill session is not filling")
	}
	next, err := session.Transition(model.FillSoaking, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	c.sessions[sessionID] = next
	return next, nil
}

func (c *Coordinator) Get(id uuid.UUID) (model.FillSession, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	session, ok := c.sessions[id]
	return session, ok
}

func (c *Coordinator) List() []model.FillSession {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]model.FillSession, 0, len(c.sessions))
	for _, session := range c.sessions {
		result = append(result, session)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result
}

func (c *Coordinator) selectCircuit() (circuit.Circuit, error) {
	for _, candidate := range c.circuitList.List() {
		if !candidate.Enabled {
			continue
		}
		if _, occupied := c.circuits.Get(candidate.ID); !occupied {
			return candidate, nil
		}
	}
	return circuit.Circuit{}, circuit.ErrCircuitBusy
}

func (c *Coordinator) finishState(session model.FillSession) {
	c.sessions[session.ID] = session
	delete(c.active, session.VesselID)
}

// closeOut 在关阀已确认执行后释放回路与容器状态，并把会话收敛到给定终态。
// 调用方必须持有 c.mu。
func (c *Coordinator) closeOut(session model.FillSession, terminal model.FillState, reason string) (model.FillSession, error) {
	final, err := session.Conclude(terminal, reason, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	// 进入 closeOut 的唯一前提是关阀已确认执行，因此即使预留此前被标记故障也可释放。
	if err := c.circuits.ReleaseConfirmed(session.CircuitID, session.ID); err != nil {
		return model.FillSession{}, err
	}
	if _, err := c.vessels.FinishFill(session.VesselID); err != nil {
		return model.FillSession{}, err
	}
	if err := c.routes.MarkClosed(session.ID); err != nil {
		return model.FillSession{}, err
	}
	c.finishState(final)
	return final, nil
}

func openFailureDetail(state valve.CommandState) string {
	if state.Receipt != nil && state.Receipt.Message != "" {
		return state.Receipt.Message
	}
	if state.Note != "" {
		return state.Note
	}
	return "device reported not executed"
}

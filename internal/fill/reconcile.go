package fill

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
)

// EventReader 提供对账所需的事件读取能力。
type EventReader interface {
	Events(partition string, after uint64) ([]model.Event, error)
}

// ReconcileService 负责进程重启后的状态恢复、未决命令的同 ID 补发，
// 以及人工对账结论向回路预留/容器/会话状态的收敛。
type ReconcileService struct {
	coordinator *Coordinator
	routes      *valve.RouteService
	outbox      *valve.Outbox
	store       EventReader
	now         func() time.Time

	mu sync.Mutex
}

func NewReconcileService(coordinator *Coordinator, routes *valve.RouteService, outbox *valve.Outbox, store EventReader, now func() time.Time) *ReconcileService {
	if now == nil {
		now = time.Now
	}
	return &ReconcileService{
		coordinator: coordinator,
		routes:      routes,
		outbox:      outbox,
		store:       store,
		now:         now,
	}
}

const fillsPartition = "fills"

// Restore 在启动时重建内存状态：
//   - 重放关阀意图；
//   - 对每条崩溃时仍开通的阀门通路，恢复回路预留、容器 filling 与 awaiting 会话。
//
// 恢复不假设任何命令结果：所有恢复会话都停留在 awaiting_valve，
// 待 Recover 同 ID 补发取得回执或人工对账后再推进。
func (r *ReconcileService) Restore() error {
	intents, err := r.loadCloseIntents()
	if err != nil {
		return err
	}
	r.coordinator.setCloseIntents(intents)

	openRoutes, err := r.routes.OpenRoutes()
	if err != nil {
		return err
	}
	for _, route := range openRoutes {
		if err := r.coordinator.restoreOpenRoute(route, r.now()); err != nil {
			return err
		}
	}
	return nil
}

// Recover 重发所有未决命令（同一 ID，设备幂等去重），然后逐条收敛会话。
// 已 awaiting 的命令不自动重发，只做状态收敛，等待人工对账。
func (r *ReconcileService) Recover(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.outbox.Recover(ctx); err != nil {
		return err
	}
	openRoutes, err := r.routes.OpenRoutes()
	if err != nil {
		return err
	}
	for _, route := range openRoutes {
		if _, _, err := r.coordinator.reconcileSession(ctx, route.SessionID); err != nil {
			return err
		}
	}
	return nil
}

// PendingCommands 返回仍待确认的阀门命令，供人工查询。
func (r *ReconcileService) PendingCommands() []valve.CommandState {
	return r.outbox.Pending()
}

// GetCommand 查询单条阀门命令状态。
func (r *ReconcileService) GetCommand(id uuid.UUID) (valve.CommandState, error) {
	return r.outbox.Get(id)
}

// ListCommands 按状态过滤查询阀门命令。
func (r *ReconcileService) ListCommands(statuses ...string) []valve.CommandState {
	return r.outbox.List(statuses...)
}

// ResolveCommand 是人工对账入口：登记人工结论，并把关联会话/回路预留收敛到一致状态。
func (r *ReconcileService) ResolveCommand(ctx context.Context, commandID uuid.UUID, outcome, operator, note string) (valve.CommandState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.outbox.Resolve(commandID, outcome, operator, note)
	if err != nil {
		return valve.CommandState{}, err
	}
	openRoutes, err := r.routes.OpenRoutes()
	if err != nil {
		return valve.CommandState{}, err
	}
	for _, route := range openRoutes {
		if route.OpenCommandID == commandID || route.CloseCommandID == commandID {
			if _, _, err := r.coordinator.reconcileSession(ctx, route.SessionID); err != nil {
				return valve.CommandState{}, err
			}
			break
		}
	}
	return state, nil
}

func (r *ReconcileService) loadCloseIntents() (map[uuid.UUID]closeIntent, error) {
	result := make(map[uuid.UUID]closeIntent)
	if r.store == nil {
		return result, nil
	}
	events, err := r.store.Events(fillsPartition, 0)
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		if event.Kind != "fill.close_intended" {
			continue
		}
		var payload closeIntentPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return nil, err
		}
		sessionID, err := uuid.Parse(event.AggregateID)
		if err != nil {
			return nil, err
		}
		result[sessionID] = closeIntent{Terminal: payload.Terminal, Reason: payload.Reason}
	}
	return result, nil
}

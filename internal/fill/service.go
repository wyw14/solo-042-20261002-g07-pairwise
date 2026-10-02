package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
)

type Service struct {
	coordinator   *Coordinator
	completion    *CompletionService
	abort         *AbortService
	reconcile     *ReconcileService
	defaultLiters float64
}

func NewService(coordinator *Coordinator, completion *CompletionService, abort *AbortService, reconcile *ReconcileService, defaultLiters float64) (*Service, error) {
	if defaultLiters <= 0 {
		return nil, errors.New("default fill volume must be positive")
	}
	return &Service{
		coordinator:   coordinator,
		completion:    completion,
		abort:         abort,
		reconcile:     reconcile,
		defaultLiters: defaultLiters,
	}, nil
}

// StartOutcome 是启动补液的对外结果。Awaiting 为 true 时开阀命令结果未知，
// 会话保留回路预留并等待确认，调用方应返回 202 Accepted。
type StartOutcome struct {
	Session  model.FillSession `json:"session"`
	Awaiting bool              `json:"awaiting"`
}

func (s *Service) OnLowLevel(ctx context.Context, vesselID string, observedAt time.Time) (StartOutcome, error) {
	_ = observedAt
	result, err := s.coordinator.StartAutomatic(ctx, vesselID, s.defaultLiters)
	if err != nil {
		return StartOutcome{}, err
	}
	return StartOutcome{Session: result.Session, Awaiting: result.Awaiting}, nil
}

func (s *Service) StartManual(ctx context.Context, vesselID string, liters float64) (StartOutcome, error) {
	if liters <= 0 {
		liters = s.defaultLiters
	}
	result, err := s.coordinator.StartManual(ctx, vesselID, liters)
	if err != nil {
		return StartOutcome{}, err
	}
	return StartOutcome{Session: result.Session, Awaiting: result.Awaiting}, nil
}

func (s *Service) BeginSoak(id uuid.UUID) (model.FillSession, error) {
	return s.coordinator.BeginSoak(id)
}

// Complete 在关阀结果未知时返回 ErrCommandUnconfirmed，此时会话仍 active、
// 回路被保留为故障，调用方应提示人工对账而非视为工艺完成。
func (s *Service) Complete(ctx context.Context, id uuid.UUID) (model.FillSession, error) {
	return s.completion.Complete(ctx, id)
}

func (s *Service) Abort(ctx context.Context, id uuid.UUID, reason string) (model.FillSession, error) {
	return s.abort.Abort(ctx, id, reason)
}

func (s *Service) RecoverIsolation(id uuid.UUID) error {
	return s.abort.RecoverIsolation(id)
}

func (s *Service) Get(id uuid.UUID) (model.FillSession, bool) {
	return s.coordinator.Get(id)
}

func (s *Service) List() []model.FillSession {
	return s.coordinator.List()
}

// PendingCommands 返回所有待确认（inflight/awaiting）的阀门命令。
func (s *Service) PendingCommands() []valve.CommandState {
	return s.reconcile.PendingCommands()
}

// ListCommands 按状态过滤返回阀门命令；不过滤返回全部。
func (s *Service) ListCommands(statuses ...string) []valve.CommandState {
	return s.reconcile.ListCommands(statuses...)
}

func (s *Service) GetCommand(id uuid.UUID) (valve.CommandState, error) {
	return s.reconcile.GetCommand(id)
}

// ResolveCommand 提交人工对账结论并收敛关联会话与回路预留。
func (s *Service) ResolveCommand(ctx context.Context, id uuid.UUID, outcome, operator, note string) (valve.CommandState, error) {
	return s.reconcile.ResolveCommand(ctx, id, outcome, operator, note)
}

// Recover 重新下发未决命令并收敛会话，供启动后/定时调用。
func (s *Service) Recover(ctx context.Context) error {
	return s.reconcile.Recover(ctx)
}

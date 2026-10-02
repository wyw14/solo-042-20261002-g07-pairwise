package valve

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

// 命令生命周期状态。关键约束：只有拿到设备明确回执才能进入 confirmed 终态；
// 传输超时、进程重启、上下文取消都不能凭空认定执行失败。
const (
	StatusInflight  = "inflight"  // 已记录或已发出，尚无明确回执
	StatusExecuted  = "executed"  // 设备回执确认已执行
	StatusFailed    = "failed"    // 设备明确拒绝/执行失败（回执 Executed=false）
	StatusAwaiting  = "awaiting"  // 自动尝试用尽，等待人工对账
	StatusResolved  = "resolved"  // 人工核对现场后给出的最终结论（executed/failed）
	StatusCancelled = "cancelled" // 人工确认命令在到达设备前作废（仅未执行命令）
)

// 可对人工开放的最终结论。
const (
	ResolutionExecuted = "executed"
	ResolutionFailed   = "failed"
	ResolutionCancel   = "cancel"
)

// ErrAwaitingConfirmation 表示命令没有明确失败，只是结果未知，
// 调用方必须保留相关预留/状态，等待对账，不得按失败回滚。
var ErrAwaitingConfirmation = errors.New("valve command outcome is unconfirmed")

// defaultAttemptLimit 是自动同 ID 补发的尝试上限；达到后转 awaiting 请人工对账。
// 转 awaiting 不是失败结论，只是停止自动补发。
const defaultAttemptLimit = 5

const valveCommandPartition = "valve-commands"

// Attempt 记录一次发送尝试。超时/取消的尝试 Error 有值但不改变命令结论。
type Attempt struct {
	Number     int        `json:"number"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error      string     `json:"error,omitempty"`
}

// CommandState 是一条阀门命令在外发箱中的可恢复状态。
type CommandState struct {
	Command    model.ValveCommand  `json:"command"`
	Status     string              `json:"status"`
	Attempts   []Attempt           `json:"attempts"`
	Receipt    *model.ValveReceipt `json:"receipt,omitempty"`
	Resolution string              `json:"resolution,omitempty"`
	ResolvedBy string              `json:"resolved_by,omitempty"`
	ResolvedAt *time.Time          `json:"resolved_at,omitempty"`
	Note       string              `json:"note,omitempty"`
	CreatedAt  time.Time           `json:"created_at"`
	UpdatedAt  time.Time           `json:"updated_at"`

	// inflight 尝试在进程内持有；重放后重置为 nil，表示该尝试已随进程死亡而结果未知。
	inflightAttempt int
}

// Settled 报告命令是否已有明确终态。
func (s CommandState) Settled() bool {
	switch s.Status {
	case StatusExecuted, StatusFailed, StatusResolved, StatusCancelled:
		return true
	default:
		return false
	}
}

// KnownExecuted 报告是否能确认阀门已按命令动作。对账结论为 executed 也算。
func (s CommandState) KnownExecuted() bool {
	return s.Status == StatusExecuted ||
		(s.Status == StatusResolved && s.Resolution == ResolutionExecuted)
}

// KnownNotExecuted 报告是否能确认命令未驱动阀门（明确失败或作废）。
func (s CommandState) KnownNotExecuted() bool {
	return s.Status == StatusFailed ||
		s.Status == StatusCancelled ||
		(s.Status == StatusResolved && s.Resolution == ResolutionFailed) ||
		(s.Status == StatusResolved && s.Resolution == ResolutionCancel)
}

type commandRequestedPayload struct {
	Command model.ValveCommand `json:"command"`
}

type attemptStartedPayload struct {
	Number    int       `json:"number"`
	StartedAt time.Time `json:"started_at"`
}

type attemptFinishedPayload struct {
	Number     int       `json:"number"`
	FinishedAt time.Time `json:"finished_at"`
	Error      string    `json:"error,omitempty"`
}

type receiptRecordedPayload struct {
	Status  string             `json:"status"`
	Receipt model.ValveReceipt `json:"receipt"`
}

type commandMarkedAwaitingPayload struct {
	Reason string `json:"reason"`
}

type commandResolvedPayload struct {
	Outcome    string    `json:"outcome"`
	ResolvedBy string    `json:"resolved_by"`
	ResolvedAt time.Time `json:"resolved_at"`
	Note       string    `json:"note,omitempty"`
}

// EventStore 是外发箱需要的最小事件存储能力。
type EventStore interface {
	Append(model.Event) (model.Event, error)
	Events(partition string, after uint64) ([]model.Event, error)
}

// Outbox 把开关阀请求、发送尝试、回执全部持久化为可恢复状态。
// 同一条命令（同一 ID）无论重试多少次、进程是否重启，都只对应一条记录，
// 设备可依赖命令 ID 做幂等去重。
type Outbox struct {
	store     EventStore
	transport Transport
	now       func() time.Time
	// attemptLimit 为自动补发尝试上限；<=0 时取 defaultAttemptLimit。
	attemptLimit int

	mu     sync.Mutex
	loaded bool
	// states 以命令 ID 为键；index 以补液会话为键记录其开/关阀命令 ID。
	states map[uuid.UUID]*CommandState
	index  map[uuid.UUID][]uuid.UUID
}

// NewOutbox 创建外发箱。transport 为实际下发通道（可为空以支持仅恢复场景）。
func NewOutbox(store EventStore, transport Transport, now func() time.Time) (*Outbox, error) {
	return NewOutboxWithLimit(store, transport, defaultAttemptLimit, now)
}

// NewOutboxWithLimit 创建外发箱并指定自动补发尝试上限。
func NewOutboxWithLimit(store EventStore, transport Transport, attemptLimit int, now func() time.Time) (*Outbox, error) {
	if store == nil {
		return nil, errors.New("outbox event store is required")
	}
	if now == nil {
		now = time.Now
	}
	if attemptLimit <= 0 {
		attemptLimit = defaultAttemptLimit
	}
	return &Outbox{
		store:        store,
		transport:    transport,
		now:          now,
		attemptLimit: attemptLimit,
		states:       make(map[uuid.UUID]*CommandState),
		index:        make(map[uuid.UUID][]uuid.UUID),
	}, nil
}

// SetTransport 在启动期注入/替换下发通道。
func (b *Outbox) SetTransport(transport Transport) {
	b.mu.Lock()
	b.transport = transport
	b.mu.Unlock()
}

func (b *Outbox) appendLocked(kind string, aggregateID string, payload any) error {
	event, err := model.NewEvent(valveCommandPartition, kind, aggregateID, payload, b.now())
	if err != nil {
		return err
	}
	recorded, err := b.store.Append(event)
	if err != nil {
		return err
	}
	_ = recorded
	return nil
}

// Dispatch 登记（若尚未登记）并尝试下发一条命令。
//
// 语义：
//   - 同一 command.ID 的重复调用永远复用既有记录，绝不生成新命令 ID；
//   - 收到设备明确回执 -> 返回终态（executed/failed）；
//   - 超时/上下文取消/传输错误 -> 命令保持 inflight（达到补发上限后转 awaiting），
//     返回未决的 CommandState 且 error 为 nil，调用方必须按状态而非 error 判定，
//     绝不能把未决当作失败；
//   - 命令已有终态时直接返回该终态（重复回执的 Duplicate 标志由设备给出）。
func (b *Outbox) Dispatch(ctx context.Context, command model.ValveCommand) (CommandState, error) {
	if err := b.ensureLoaded(); err != nil {
		return CommandState{}, err
	}
	if err := command.ValidateIdentity(); err != nil {
		return CommandState{}, err
	}

	b.mu.Lock()
	state, exists := b.states[command.ID]
	if !exists {
		state = &CommandState{
			Command:   command,
			Status:    StatusInflight,
			CreatedAt: b.now().UTC(),
			UpdatedAt: b.now().UTC(),
		}
		if err := b.appendLocked("valve.command_requested", command.ID.String(),
			commandRequestedPayload{Command: command}); err != nil {
			b.mu.Unlock()
			return CommandState{}, err
		}
		b.states[command.ID] = state
		b.index[command.FillSessionID] = append(b.index[command.FillSessionID], command.ID)
	} else if err := ensureSameCommand(state.Command, command); err != nil {
		b.mu.Unlock()
		return CommandState{}, err
	}
	transport := b.transport
	b.mu.Unlock()

	if state.Settled() {
		return snapshot(state), nil
	}

	// 进程内已有一个尚未返回的尝试：本次调用不并发重发，直接返回当前未知状态。
	// 跨进程/重启后的孤儿尝试会在 Recover 中重新用同一 ID 补发。
	b.mu.Lock()
	if state.inflightAttempt != 0 {
		current := snapshot(state)
		b.mu.Unlock()
		return current, nil
	}
	attemptNumber := len(state.Attempts) + 1
	startedAt := b.now().UTC()
	if err := b.appendLocked("valve.attempt_started", command.ID.String(),
		attemptStartedPayload{Number: attemptNumber, StartedAt: startedAt}); err != nil {
		b.mu.Unlock()
		return CommandState{}, err
	}
	state.Attempts = append(state.Attempts, Attempt{Number: attemptNumber, StartedAt: startedAt})
	state.inflightAttempt = attemptNumber
	state.UpdatedAt = startedAt
	b.mu.Unlock()

	if transport == nil {
		// 没有下发通道时，尝试保持打开；命令停留 inflight 等待 Recover/对账。
		b.finishAttempt(command.ID, attemptNumber, errors.New("valve transport is not configured"))
		return b.Get(command.ID)
	}

	receipt, execErr := transport.Execute(ctx, command)
	finishedAt := b.now().UTC()
	if execErr != nil {
		// 传输层错误（含超时、连接中断、ctx 取消）：设备侧结果未知。
		// 不做失败结论，只闭合本次尝试。
		b.finishAttempt(command.ID, attemptNumber, execErr)
		return b.Get(command.ID)
	}
	if err := receipt.Validate(command); err != nil {
		b.finishAttempt(command.ID, attemptNumber, err)
		return b.Get(command.ID)
	}
	b.recordReceipt(command.ID, attemptNumber, receipt, finishedAt)
	return b.Get(command.ID)
}

func (b *Outbox) finishAttempt(commandID uuid.UUID, number int, cause error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.states[commandID]
	if !ok {
		return
	}
	finishedAt := b.now().UTC()
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	if err := b.appendLocked("valve.attempt_finished", commandID.String(),
		attemptFinishedPayload{Number: number, FinishedAt: finishedAt, Error: message}); err != nil {
		// 事件落盘失败属于严重问题：状态无法再被信任地恢复，直接 panic 由进程边界处理。
		panic(err)
	}
	for i := range state.Attempts {
		if state.Attempts[i].Number == number {
			ts := finishedAt
			state.Attempts[i].FinishedAt = &ts
			state.Attempts[i].Error = message
			break
		}
	}
	state.inflightAttempt = 0
	state.UpdatedAt = finishedAt

	// 达到自动补发上限仍无明确结论：转 awaiting 请人工对账。
	// 这不是失败判定，只是停止自动重试；命令结果仍属未知。
	failedAttempts := 0
	for _, attempt := range state.Attempts {
		if attempt.Error != "" {
			failedAttempts++
		}
	}
	if state.Status == StatusInflight && failedAttempts >= b.attemptLimit {
		if err := b.appendLocked("valve.command_marked_awaiting", commandID.String(),
			commandMarkedAwaitingPayload{Reason: "automatic send attempts exhausted"}); err != nil {
			panic(err)
		}
		state.Status = StatusAwaiting
		state.Note = "automatic send attempts exhausted"
		state.UpdatedAt = finishedAt
	}
}

func (b *Outbox) recordReceipt(commandID uuid.UUID, attemptNumber int, receipt model.ValveReceipt, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.states[commandID]
	if !ok {
		return
	}
	status := StatusExecuted
	if !receipt.Executed {
		status = StatusFailed
	}
	// 先闭合尝试（成功尝试无错误），再记录回执，顺序即事件顺序。
	if err := b.appendLocked("valve.attempt_finished", commandID.String(),
		attemptFinishedPayload{Number: attemptNumber, FinishedAt: at}); err != nil {
		panic(err)
	}
	for i := range state.Attempts {
		if state.Attempts[i].Number == attemptNumber && state.Attempts[i].FinishedAt == nil {
			ts := at
			state.Attempts[i].FinishedAt = &ts
			break
		}
	}
	receiptCopy := receipt
	if err := b.appendLocked("valve.receipt_recorded", commandID.String(),
		receiptRecordedPayload{Status: status, Receipt: receipt}); err != nil {
		panic(err)
	}
	state.Receipt = &receiptCopy
	state.Status = status
	state.inflightAttempt = 0
	state.UpdatedAt = at
}

// MarkAwaiting 把一条未知命令显式转入“待人工确认”。幂等。
func (b *Outbox) MarkAwaiting(commandID uuid.UUID, reason string) error {
	if err := b.ensureLoaded(); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.states[commandID]
	if !ok {
		return errors.New("valve command not found")
	}
	if state.Settled() {
		return nil
	}
	if state.Status == StatusAwaiting {
		return nil
	}
	if err := b.appendLocked("valve.command_marked_awaiting", commandID.String(),
		commandMarkedAwaitingPayload{Reason: reason}); err != nil {
		return err
	}
	state.Status = StatusAwaiting
	state.Note = reason
	state.UpdatedAt = b.now().UTC()
	return nil
}

// Resolve 是人工对账入口：现场确认阀门实际状态后给出结论。
// outcome 为 executed / failed / cancel。
func (b *Outbox) Resolve(commandID uuid.UUID, outcome, operator, note string) (CommandState, error) {
	if err := b.ensureLoaded(); err != nil {
		return CommandState{}, err
	}
	if operator == "" {
		return CommandState{}, errors.New("operator identity is required for reconciliation")
	}
	switch outcome {
	case ResolutionExecuted, ResolutionFailed, ResolutionCancel:
	default:
		return CommandState{}, errors.New("invalid reconciliation outcome")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.states[commandID]
	if !ok {
		return CommandState{}, errors.New("valve command not found")
	}
	if state.Settled() {
		// 已有设备/人工终态时不得用对账覆盖，防止把已执行命令改成未执行。
		return CommandState{}, errors.New("valve command already has a confirmed outcome")
	}
	at := b.now().UTC()
	status := StatusResolved
	if outcome == ResolutionCancel {
		status = StatusCancelled
	}
	if err := b.appendLocked("valve.command_resolved", commandID.String(),
		commandResolvedPayload{Outcome: outcome, ResolvedBy: operator, ResolvedAt: at, Note: note}); err != nil {
		return CommandState{}, err
	}
	state.Status = status
	state.Resolution = outcome
	state.ResolvedBy = operator
	state.ResolvedAt = &at
	if note != "" {
		state.Note = note
	}
	state.UpdatedAt = at
	return snapshot(state), nil
}

// Recover 在启动/定时任务中扫描未决命令：
//   - 携带进程内 inflight 标记的命令是重启前发出的孤儿尝试，结果未知，
//     用同一个命令 ID 重新下发（设备按 ID 幂等去重，不会重复开阀）；
//   - 其余 inflight 命令也做一次同 ID 补发；
//   - 已经 awaiting 的命令不再自动重发，等待人工。
//
// 返回仍未能确认的命令快照。该方法只做“同 ID 补发”，绝不新建命令。
func (b *Outbox) Recover(ctx context.Context) ([]CommandState, error) {
	if err := b.ensureLoaded(); err != nil {
		return nil, err
	}
	pending := b.Pending()
	for _, state := range pending {
		if state.Status == StatusAwaiting {
			continue
		}
		// 重放后 inflightAttempt 必为 0，Dispatch 会复用同一 ID 补发。
		if _, err := b.Dispatch(ctx, state.Command); err != nil {
			return nil, err
		}
	}
	return b.Pending(), nil
}

// Get 返回单条命令状态。
func (b *Outbox) Get(id uuid.UUID) (CommandState, error) {
	if err := b.ensureLoaded(); err != nil {
		return CommandState{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.states[id]
	if !ok {
		return CommandState{}, errors.New("valve command not found")
	}
	return snapshot(state), nil
}

// Pending 返回所有尚未确认（inflight/awaiting）的命令，按创建时间排序。
func (b *Outbox) Pending() []CommandState {
	return b.List(StatusInflight, StatusAwaiting)
}

// List 按给定状态过滤返回命令；不过滤传 nil 则返回全部，按创建时间排序。
func (b *Outbox) List(statuses ...string) []CommandState {
	if err := b.ensureLoaded(); err != nil {
		return nil
	}
	filter := make(map[string]bool, len(statuses))
	for _, status := range statuses {
		filter[status] = true
	}
	b.mu.Lock()
	result := make([]CommandState, 0, len(b.states))
	for _, state := range b.states {
		if len(filter) == 0 || filter[state.Status] {
			result = append(result, snapshot(state))
		}
	}
	b.mu.Unlock()
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result
}

// BySession 返回某补液会话关联的全部命令（开阀、关阀），按创建时间排序。
func (b *Outbox) BySession(sessionID uuid.UUID) []CommandState {
	if err := b.ensureLoaded(); err != nil {
		return nil
	}
	b.mu.Lock()
	ids := append([]uuid.UUID(nil), b.index[sessionID]...)
	states := make([]CommandState, 0, len(ids))
	for _, id := range ids {
		if state, ok := b.states[id]; ok {
			states = append(states, snapshot(state))
		}
	}
	b.mu.Unlock()
	sort.Slice(states, func(i, j int) bool {
		return states[i].CreatedAt.Before(states[j].CreatedAt)
	})
	return states
}

func (b *Outbox) ensureLoaded() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.loaded {
		return nil
	}
	events, err := b.store.Events(valveCommandPartition, 0)
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := b.applyEvent(event); err != nil {
			return err
		}
	}
	b.loaded = true
	return nil
}

// applyEvent 重放单个事件重建状态。重放后 inflightAttempt 一律为 0，
// 因为发出那次尝试的进程已经不在了——其结果只能靠同 ID 补发或人工对账确定。
func (b *Outbox) applyEvent(event model.Event) error {
	id, err := uuid.Parse(event.AggregateID)
	if err != nil {
		return err
	}
	switch event.Kind {
	case "valve.command_requested":
		var payload commandRequestedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		if _, exists := b.states[id]; exists {
			return nil
		}
		b.states[id] = &CommandState{
			Command:   payload.Command,
			Status:    StatusInflight,
			CreatedAt: event.OccurredAt,
			UpdatedAt: event.OccurredAt,
		}
		b.index[payload.Command.FillSessionID] = append(b.index[payload.Command.FillSessionID], id)
	case "valve.attempt_started":
		var payload attemptStartedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		state := b.states[id]
		if state == nil {
			return nil
		}
		// 去重：事件重放不应重复累加尝试。
		if payload.Number > len(state.Attempts) {
			state.Attempts = append(state.Attempts, Attempt{Number: payload.Number, StartedAt: payload.StartedAt})
		}
	case "valve.attempt_finished":
		var payload attemptFinishedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		state := b.states[id]
		if state == nil {
			return nil
		}
		for i := range state.Attempts {
			if state.Attempts[i].Number == payload.Number {
				ts := payload.FinishedAt
				state.Attempts[i].FinishedAt = &ts
				state.Attempts[i].Error = payload.Error
				break
			}
		}
	case "valve.receipt_recorded":
		var payload receiptRecordedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		state := b.states[id]
		if state == nil {
			return nil
		}
		receipt := payload.Receipt
		state.Receipt = &receipt
		state.Status = payload.Status
	case "valve.command_marked_awaiting":
		var payload commandMarkedAwaitingPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		state := b.states[id]
		if state == nil {
			return nil
		}
		if !state.Settled() {
			state.Status = StatusAwaiting
			state.Note = payload.Reason
		}
	case "valve.command_resolved":
		var payload commandResolvedPayload
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			return err
		}
		state := b.states[id]
		if state == nil {
			return nil
		}
		if payload.Outcome == ResolutionCancel {
			state.Status = StatusCancelled
		} else {
			state.Status = StatusResolved
		}
		state.Resolution = payload.Outcome
		state.ResolvedBy = payload.ResolvedBy
		resolvedAt := payload.ResolvedAt
		state.ResolvedAt = &resolvedAt
		if payload.Note != "" {
			state.Note = payload.Note
		}
	}
	if state := b.states[id]; state != nil {
		if event.OccurredAt.After(state.UpdatedAt) {
			state.UpdatedAt = event.OccurredAt
		}
	}
	return nil
}

func snapshot(state *CommandState) CommandState {
	copied := *state
	copied.Attempts = append([]Attempt(nil), state.Attempts...)
	copied.inflightAttempt = 0
	return copied
}

func ensureSameCommand(existing, incoming model.ValveCommand) error {
	if existing.FillSessionID != incoming.FillSessionID ||
		existing.CircuitGeneration != incoming.CircuitGeneration ||
		existing.ValveID != incoming.ValveID ||
		existing.Action != incoming.Action {
		return errors.New("command id reused with different parameters")
	}
	return nil
}

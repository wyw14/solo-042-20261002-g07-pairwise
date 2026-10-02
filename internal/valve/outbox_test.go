package valve

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/journal"
	"github.com/wyw14/cry-98/internal/model"
)

// scriptedTransport 按脚本返回结果，并记录每次实际下发的命令 ID。
type scriptedTransport struct {
	mu        sync.Mutex
	script    []error
	calls     []uuid.UUID
	receiptOK bool
	now       func() time.Time
}

func (t *scriptedTransport) Execute(ctx context.Context, command model.ValveCommand) (model.ValveReceipt, error) {
	t.mu.Lock()
	t.calls = append(t.calls, command.ID)
	var err error
	if len(t.script) > 0 {
		err = t.script[0]
		t.script = t.script[1:]
	}
	now := t.now
	t.mu.Unlock()
	if err != nil {
		return model.ValveReceipt{}, err
	}
	return model.ValveReceipt{
		CommandID:  command.ID,
		Executed:   t.receiptOK,
		Message:    "accepted",
		ReceivedAt: now().UTC(),
	}, nil
}

func (t *scriptedTransport) callCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.calls)
}

func (t *scriptedTransport) uniqueIDs() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	seen := make(map[uuid.UUID]bool)
	for _, id := range t.calls {
		seen[id] = true
	}
	return len(seen)
}

func newTestStore(t *testing.T) *journal.Store {
	t.Helper()
	store, err := journal.NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func testCommand() model.ValveCommand {
	command, err := model.NewValveCommand(uuid.New(), uuid.New(), 1, "VALVE-A", model.ValveOpen, time.Unix(100, 0).UTC())
	if err != nil {
		panic(err)
	}
	return command
}

var errTransportTimeout = errors.New("device timeout")

// 超时不判失败：多次超时后命令仍为 inflight，且全程只用一个命令 ID。
func TestOutboxTimeoutDoesNotImplyFailure(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	now := func() time.Time { return clock }
	store := newTestStore(t)
	transport := &scriptedTransport{
		script: []error{errTransportTimeout, context.DeadlineExceeded},
		now:    now,
	}
	box, err := NewOutbox(store, transport, now)
	if err != nil {
		t.Fatal(err)
	}
	command := testCommand()

	first, err := box.Dispatch(context.Background(), command)
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if first.Settled() {
		t.Fatalf("timed out command must not be settled, got %s", first.Status)
	}
	if first.Status != StatusInflight {
		t.Fatalf("expected inflight after timeout, got %s", first.Status)
	}
	if len(first.Attempts) != 1 || first.Attempts[0].Error == "" {
		t.Fatalf("attempt should record the timeout error: %+v", first.Attempts)
	}

	// 同 ID 再发：仍是同一条命令、同一个 ID，绝不新开命令。
	second, err := box.Dispatch(context.Background(), command)
	if err != nil {
		t.Fatalf("second dispatch: %v", err)
	}
	if second.Settled() {
		t.Fatalf("still unconfirmed, got %s", second.Status)
	}
	if transport.uniqueIDs() != 1 {
		t.Fatalf("retries must reuse the same command id, saw %d distinct ids (%v)", transport.uniqueIDs(), transport.calls)
	}
	if transport.callCount() != 2 {
		t.Fatalf("expected 2 send attempts, got %d", transport.callCount())
	}
	if len(box.Pending()) != 1 {
		t.Fatalf("expected one pending command, got %d", len(box.Pending()))
	}
}

// 收到明确回执后命令进入终态；重复 Dispatch 不再下发，直接返回终态。
func TestOutboxReceiptSettlesAndIsIdempotent(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	now := func() time.Time { return clock }
	store := newTestStore(t)
	transport := &scriptedTransport{receiptOK: true, now: now}
	box, err := NewOutbox(store, transport, now)
	if err != nil {
		t.Fatal(err)
	}
	command := testCommand()

	state, err := box.Dispatch(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if !state.KnownExecuted() || state.Receipt == nil {
		t.Fatalf("expected executed receipt, got %+v", state)
	}

	again, err := box.Dispatch(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if !again.KnownExecuted() {
		t.Fatalf("settled command should stay executed, got %s", again.Status)
	}
	if transport.callCount() != 1 {
		t.Fatalf("settled command must not be re-sent, calls=%d", transport.callCount())
	}
	if len(box.Pending()) != 0 {
		t.Fatalf("settled command must not be pending")
	}
}

// 设备明确返回未执行 -> failed（这是唯一可安全回滚的情形）。
func TestOutboxExplicitRejectionIsFailure(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	now := func() time.Time { return clock }
	store := newTestStore(t)
	transport := &scriptedTransport{receiptOK: false, now: now}
	box, err := NewOutbox(store, transport, now)
	if err != nil {
		t.Fatal(err)
	}
	state, err := box.Dispatch(context.Background(), testCommand())
	if err != nil {
		t.Fatal(err)
	}
	if !state.KnownNotExecuted() || state.Status != StatusFailed {
		t.Fatalf("expected failed for explicit non-execution receipt, got %s", state.Status)
	}
}

// 重启：先发命令后超时，再用同一持久存储构造全新外发箱实例，
// Recover 必须用同一个命令 ID 补发并在拿到回执后确认执行——全程没有新 ID。
func TestOutboxRecoveryReusesCommandIDAcrossRestart(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	now := func() time.Time { return clock }
	store := newTestStore(t)
	command := testCommand()

	before := &scriptedTransport{script: []error{errTransportTimeout}, now: now}
	box1, err := NewOutbox(store, before, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := box1.Dispatch(context.Background(), command); err != nil {
		t.Fatal(err)
	}

	// 模拟进程重启：新建外发箱与传输，恢复出的命令仍是 inflight。
	after := &scriptedTransport{receiptOK: true, now: now}
	box2, err := NewOutbox(store, after, now)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := box2.Get(command.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Settled() {
		t.Fatalf("recovered command must be unconfirmed, got %s", recovered.Status)
	}
	// 旧进程的在途尝试不应被当作存活尝试。
	if recovered.inflightAttempt != 0 {
		t.Fatalf("restored state must clear in-process inflight marker")
	}

	if _, err := box2.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := box2.Get(command.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !state.KnownExecuted() {
		t.Fatalf("expected executed after same-id recovery resend, got %s", state.Status)
	}
	allIDs := append(append([]uuid.UUID{}, before.calls...), after.calls...)
	seen := make(map[uuid.UUID]bool)
	for _, id := range allIDs {
		if id != command.ID {
			t.Fatalf("recovery sent a different command id: %s vs %s", id, command.ID)
		}
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("expected exactly one command id across restart, got %d", len(seen))
	}
}

// 人工对账：awaiting 命令可被人工置为 executed/failed，且已有终态不可覆盖。
func TestOutboxManualResolve(t *testing.T) {
	clock := time.Unix(1000, 0).UTC()
	now := func() time.Time { return clock }
	store := newTestStore(t)
	transport := &scriptedTransport{script: []error{errTransportTimeout}, now: now}
	box, err := NewOutbox(store, transport, now)
	if err != nil {
		t.Fatal(err)
	}
	command := testCommand()
	state, err := box.Dispatch(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if err := box.MarkAwaiting(command.ID, "needs site check"); err != nil {
		t.Fatal(err)
	}
	resolved, err := box.Resolve(command.ID, ResolutionExecuted, "operator-7", "valve physically open")
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.KnownExecuted() || resolved.ResolvedBy != "operator-7" {
		t.Fatalf("unexpected resolved state: %+v", resolved)
	}
	// 已终态不得再被对账覆盖。
	if _, err := box.Resolve(command.ID, ResolutionFailed, "operator-9", ""); err == nil {
		t.Fatal("overriding a confirmed outcome must be rejected")
	}
	_ = state
}

// 同一命令 ID 不得携带不同参数复用。
func TestOutboxRejectsConflictingCommandIdentity(t *testing.T) {
	now := func() time.Time { return time.Unix(1000, 0).UTC() }
	store := newTestStore(t)
	transport := &scriptedTransport{receiptOK: true, now: now}
	box, err := NewOutbox(store, transport, now)
	if err != nil {
		t.Fatal(err)
	}
	first := testCommand()
	if _, err := box.Dispatch(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	conflict := first
	conflict.ValveID = "VALVE-B"
	if _, err := box.Dispatch(context.Background(), conflict); err == nil {
		t.Fatal("expected identity conflict error")
	}
}

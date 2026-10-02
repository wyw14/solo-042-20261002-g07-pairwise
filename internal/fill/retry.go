package fill

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
)

type RetryPolicy struct {
	Attempts int
	Delay    time.Duration
}

type RetryService struct {
	commands *valve.CommandService
	policy   RetryPolicy
}

func NewRetryService(commands *valve.CommandService, policy RetryPolicy) (*RetryService, error) {
	if commands == nil || policy.Attempts < 1 || policy.Delay < 0 {
		return nil, errors.New("invalid retry service configuration")
	}
	return &RetryService{commands: commands, policy: policy}, nil
}

// Open dispatches the open command with retries. The command identity comes
// from the caller (derived from the fill session) and is reused across every
// attempt, so a retry after a timeout can never actuate the valve twice
// under different IDs.
func (s *RetryService) Open(ctx context.Context, commandID, sessionID uuid.UUID, generation uint64, valveID string) (model.ValveReceipt, error) {
	var lastErr error
	for attempt := 0; attempt < s.policy.Attempts; attempt++ {
		receipt, err := s.commands.Open(ctx, commandID, sessionID, generation, valveID)
		if err == nil {
			return receipt, nil
		}
		if errors.Is(err, valve.ErrCommandRejected) {
			// Definitive refusal — retrying cannot change the outcome.
			return model.ValveReceipt{}, err
		}
		lastErr = err
		if attempt+1 < s.policy.Attempts {
			timer := time.NewTimer(s.policy.Delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return model.ValveReceipt{}, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return model.ValveReceipt{}, lastErr
}

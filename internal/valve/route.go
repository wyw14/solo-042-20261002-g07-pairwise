package valve

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

type Route struct {
	SessionID         uuid.UUID
	CircuitID         string
	CircuitGeneration uint64
	ValveID           string
	OpenCommandID     uuid.UUID
	CloseCommandID    uuid.UUID
}

type RouteService struct {
	commands *CommandService
	mu       sync.Mutex
	open     map[uuid.UUID]Route
}

func NewRouteService(commands *CommandService) *RouteService {
	return &RouteService{commands: commands, open: make(map[uuid.UUID]Route)}
}

// Open dispatches the route's open command. Tracking the route is the
// caller's job (TrackOpen) and must happen before dispatch, so the close
// command stays reachable even when the open outcome is uncertain.
func (s *RouteService) Open(ctx context.Context, route Route) error {
	if route.SessionID == uuid.Nil || route.OpenCommandID == uuid.Nil {
		return errors.New("route command identity is incomplete")
	}
	_, err := s.commands.Open(
		ctx,
		route.OpenCommandID,
		route.SessionID,
		route.CircuitGeneration,
		route.ValveID,
	)
	return err
}

func (s *RouteService) TrackOpen(route Route) error {
	if route.SessionID == uuid.Nil || route.CloseCommandID == uuid.Nil {
		return errors.New("route tracking identity is incomplete")
	}
	s.mu.Lock()
	s.open[route.SessionID] = route
	s.mu.Unlock()
	return nil
}

// Untrack drops the tracked route without dispatching anything, used when
// a command is reconciled in a way that makes the route moot.
func (s *RouteService) Untrack(sessionID uuid.UUID) {
	s.mu.Lock()
	delete(s.open, sessionID)
	s.mu.Unlock()
}

// Close dispatches the route's close command. When the route is no longer
// tracked in memory (e.g. after a restart) it is rebuilt from the persisted
// outbox: the open command carries the circuit generation and valve, and
// the close command ID is derived deterministically, so the close reuses
// the original command identity instead of a fresh one.
func (s *RouteService) Close(ctx context.Context, sessionID uuid.UUID) error {
	s.mu.Lock()
	route, ok := s.open[sessionID]
	s.mu.Unlock()
	if !ok {
		openCommand, found := s.commands.CommandFor(sessionID, model.ValveOpen)
		if !found {
			return errors.New("route is not open")
		}
		route = Route{
			SessionID:         sessionID,
			CircuitGeneration: openCommand.CircuitGeneration,
			ValveID:           openCommand.ValveID,
			OpenCommandID:     openCommand.ID,
			CloseCommandID:    model.ValveCommandIDFor(sessionID, model.ValveClose),
		}
	}
	if _, err := s.commands.Close(
		ctx,
		route.CloseCommandID,
		route.SessionID,
		route.CircuitGeneration,
		route.ValveID,
	); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.open, sessionID)
	s.mu.Unlock()
	return nil
}

package fill

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
	"github.com/wyw14/cry-98/internal/vessel"
)

var ErrActiveSession = errors.New("vessel already has an active fill session")

// ErrCommandUnconfirmed means a valve command was dispatched but its
// outcome is unknown (timeout or transport error). The valve may physically
// have acted, so the circuit reservation is retained as faulted and the
// session stays active until an operator reconciles the pending command.
var ErrCommandUnconfirmed = errors.New("valve command outcome is unconfirmed")

type Coordinator struct {
	mu          sync.RWMutex
	sessions    map[uuid.UUID]model.FillSession
	active      map[string]uuid.UUID
	circuits    *circuit.Manager
	circuitList *circuit.Registry
	routes      *valve.RouteService
	retry       *RetryService
	vessels     *vessel.StateService
	now         func() time.Time
}

func (c *Coordinator) SetRetryService(retry *RetryService) {
	c.mu.Lock()
	c.retry = retry
	c.mu.Unlock()
}

func NewCoordinator(circuits *circuit.Manager, circuitList *circuit.Registry, routes *valve.RouteService, vessels *vessel.StateService, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{
		sessions:    make(map[uuid.UUID]model.FillSession),
		active:      make(map[string]uuid.UUID),
		circuits:    circuits,
		circuitList: circuitList,
		routes:      routes,
		vessels:     vessels,
		now:         now,
	}
}

func (c *Coordinator) Start(ctx context.Context, vesselID string, origin model.FillOrigin, liters float64) (model.FillSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.active[vesselID]; exists {
		return model.FillSession{}, ErrActiveSession
	}
	selected, err := c.selectCircuit()
	if err != nil {
		return model.FillSession{}, err
	}
	session, err := model.NewFillSession(vesselID, origin, 1, liters, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	reservation, err := c.circuits.Reserve(selected.ID, vesselID, session.ID, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	session = session.WithCircuit(selected.ID, c.now())
	session, err = session.Transition(model.FillPrechecking, c.now())
	if err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, err
	}
	if _, err := c.vessels.BeginFill(vesselID); err != nil {
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, err
	}
	route := valve.Route{
		SessionID:         session.ID,
		CircuitID:         selected.ID,
		CircuitGeneration: reservation.Generation,
		ValveID:           selected.ValveID,
		OpenCommandID:     model.ValveCommandIDFor(session.ID, model.ValveOpen),
		CloseCommandID:    model.ValveCommandIDFor(session.ID, model.ValveClose),
	}
	// Track the route before dispatching so the close command stays
	// reachable even when the open outcome is uncertain.
	if err := c.routes.TrackOpen(route); err != nil {
		_, _ = c.vessels.FinishFill(vesselID)
		_ = c.circuits.Release(selected.ID, session.ID)
		return model.FillSession{}, err
	}
	var openErr error
	if c.retry != nil {
		_, openErr = c.retry.Open(ctx, route.OpenCommandID, session.ID, reservation.Generation, selected.ValveID)
	} else {
		openErr = c.routes.Open(ctx, route)
	}
	if openErr != nil {
		if errors.Is(openErr, valve.ErrCommandRejected) {
			// The valve definitively refused to open: nothing was actuated,
			// so the reservation can be rolled back safely.
			c.routes.Untrack(session.ID)
			_, _ = c.vessels.FinishFill(vesselID)
			_ = c.circuits.Release(selected.ID, session.ID)
			return model.FillSession{}, openErr
		}
		// Timeout or transport error: the open outcome is UNKNOWN and the
		// valve may physically be open. Do not assume failure — keep the
		// circuit reserved (faulted) and the session active until the
		// pending command is reconciled.
		_ = c.circuits.RetainFault(selected.ID, session.ID)
		c.sessions[session.ID] = session
		c.active[vesselID] = session.ID
		return session, fmt.Errorf("%w: %w", ErrCommandUnconfirmed, openErr)
	}
	session, err = session.Transition(model.FillFilling, c.now())
	if err != nil {
		return model.FillSession{}, err
	}
	c.sessions[session.ID] = session
	c.active[vesselID] = session.ID
	return session, nil
}

func (c *Coordinator) StartAutomatic(ctx context.Context, vesselID string, liters float64) (model.FillSession, error) {
	return c.Start(ctx, vesselID, model.FillAutomatic, liters)
}

func (c *Coordinator) StartManual(ctx context.Context, vesselID string, liters float64) (model.FillSession, error) {
	return c.Start(ctx, vesselID, model.FillManual, liters)
}

func (c *Coordinator) BeginSoak(sessionID uuid.UUID) (model.FillSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[sessionID]
	if !ok {
		return model.FillSession{}, errors.New("fill session not found")
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

package valve

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
)

const valveRoutePartition = "valve-routes"

// Route 描述一个补液会话占用的阀门通路及其开/关阀命令身份。
// 命令 ID 在通路注册时固定，整条通路生命周期内不得更换：
// 重启后重放本记录即可用原 ID 补发，设备按 ID 幂等去重。
type Route struct {
	SessionID         uuid.UUID        `json:"session_id"`
	VesselID          string           `json:"vessel_id"`
	Origin            model.FillOrigin `json:"origin"`
	PlannedLiters     float64          `json:"planned_liters"`
	CircuitID         string           `json:"circuit_id"`
	CircuitGeneration uint64           `json:"circuit_generation"`
	ValveID           string           `json:"valve_id"`
	OpenCommandID     uuid.UUID        `json:"open_command_id"`
	CloseCommandID    uuid.UUID        `json:"close_command_id"`
}

type routeRegisteredPayload struct {
	Route Route `json:"route"`
}

type routeClosedPayload struct {
	SessionID uuid.UUID `json:"session_id"`
	At        time.Time `json:"at"`
}

// RouteEventStore 是路由服务需要的最小事件存储能力。
type RouteEventStore interface {
	Append(model.Event) (model.Event, error)
	Events(partition string, after uint64) ([]model.Event, error)
}

// RouteService 持久化阀门通路的注册与关闭，所有开/关阀命令都经由 Outbox 发出。
type RouteService struct {
	outbox *Outbox
	store  RouteEventStore
	now    func() time.Time

	mu     sync.Mutex
	loaded bool
	open   map[uuid.UUID]Route
}

func NewRouteService(outbox *Outbox, store RouteEventStore, now func() time.Time) (*RouteService, error) {
	if outbox == nil || store == nil {
		return nil, errors.New("route service requires an outbox and an event store")
	}
	if now == nil {
		now = time.Now
	}
	return &RouteService{
		outbox: outbox,
		store:  store,
		now:    now,
		open:   make(map[uuid.UUID]Route),
	}, nil
}

func (s *RouteService) validate(route Route) error {
	if route.SessionID == uuid.Nil || route.VesselID == "" || route.CircuitID == "" ||
		route.CircuitGeneration == 0 || route.ValveID == "" ||
		route.OpenCommandID == uuid.Nil || route.CloseCommandID == uuid.Nil {
		return errors.New("route identity is incomplete")
	}
	if route.OpenCommandID == route.CloseCommandID {
		return errors.New("open and close command ids must differ")
	}
	return nil
}

func (s *RouteService) ensureLoaded() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loaded {
		return nil
	}
	events, err := s.store.Events(valveRoutePartition, 0)
	if err != nil {
		return err
	}
	for _, event := range events {
		switch event.Kind {
		case "valve.route_registered":
			var payload routeRegisteredPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			if err := s.validate(payload.Route); err != nil {
				return err
			}
			// 已关闭后再次注册同一会话：以最新注册为准。
			s.open[payload.Route.SessionID] = payload.Route
		case "valve.route_closed":
			var payload routeClosedPayload
			if err := json.Unmarshal(event.Payload, &payload); err != nil {
				return err
			}
			delete(s.open, payload.SessionID)
		}
	}
	s.loaded = true
	return nil
}

// Register 持久化一条开通路。已注册同会话时必须与既有记录一致（幂等）。
func (s *RouteService) Register(route Route) error {
	if err := s.ensureLoaded(); err != nil {
		return err
	}
	if err := s.validate(route); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.open[route.SessionID]; ok {
		if existing != route {
			return errors.New("route already registered with different parameters")
		}
		return nil
	}
	event, err := model.NewEvent(valveRoutePartition, "valve.route_registered",
		route.SessionID.String(), routeRegisteredPayload{Route: route}, s.now())
	if err != nil {
		return err
	}
	if _, err := s.store.Append(event); err != nil {
		return err
	}
	s.open[route.SessionID] = route
	return nil
}

// Open 用通路固定的开阀命令 ID 经外发箱下发开阀命令。
// 返回的命令状态即为可恢复结论：未知时不会返回普通 error，由调用方据状态决策。
func (s *RouteService) Open(ctx context.Context, route Route) (CommandState, error) {
	if err := s.Register(route); err != nil {
		return CommandState{}, err
	}
	command, err := model.NewValveCommand(
		route.OpenCommandID,
		route.SessionID,
		route.CircuitGeneration,
		route.ValveID,
		model.ValveOpen,
		s.now(),
	)
	if err != nil {
		return CommandState{}, err
	}
	return s.outbox.Dispatch(ctx, command)
}

// Close 用通路固定的关阀命令 ID 经外发箱下发关阀命令。
// 通路未在内存中时回退到持久化记录，因此重启后仍能安全关阀。
func (s *RouteService) Close(ctx context.Context, sessionID uuid.UUID) (CommandState, error) {
	if err := s.ensureLoaded(); err != nil {
		return CommandState{}, err
	}
	s.mu.Lock()
	route, ok := s.open[sessionID]
	s.mu.Unlock()
	if !ok {
		return CommandState{}, errors.New("route is not open")
	}
	command, err := model.NewValveCommand(
		route.CloseCommandID,
		route.SessionID,
		route.CircuitGeneration,
		route.ValveID,
		model.ValveClose,
		s.now(),
	)
	if err != nil {
		return CommandState{}, err
	}
	return s.outbox.Dispatch(ctx, command)
}

// MarkClosed 在确认关阀命令已执行、回路释放后调用，记录通路关闭。
func (s *RouteService) MarkClosed(sessionID uuid.UUID) error {
	if err := s.ensureLoaded(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.open[sessionID]; !ok {
		return nil
	}
	event, err := model.NewEvent(valveRoutePartition, "valve.route_closed",
		sessionID.String(), routeClosedPayload{SessionID: sessionID, At: s.now()}, s.now())
	if err != nil {
		return err
	}
	if _, err := s.store.Append(event); err != nil {
		return err
	}
	delete(s.open, sessionID)
	return nil
}

// Get 返回某会话当前仍开通的通路记录。
func (s *RouteService) Get(sessionID uuid.UUID) (Route, bool, error) {
	if err := s.ensureLoaded(); err != nil {
		return Route{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	route, ok := s.open[sessionID]
	return route, ok, nil
}

// OpenRoutes 返回重启后仍处于开通状态、需要对账/补发的全部通路。
func (s *RouteService) OpenRoutes() ([]Route, error) {
	if err := s.ensureLoaded(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	routes := make([]Route, 0, len(s.open))
	for _, route := range s.open {
		routes = append(routes, route)
	}
	return routes, nil
}

// Outbox 返回路由服务使用的持久命令外发箱。
func (s *RouteService) Outbox() *Outbox {
	return s.outbox
}

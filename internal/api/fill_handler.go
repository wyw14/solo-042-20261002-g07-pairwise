package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/circuit"
	"github.com/wyw14/cry-98/internal/fill"
)

type startFillRequest struct {
	VesselID string  `json:"vessel_id"`
	Liters   float64 `json:"liters"`
}

type abortFillRequest struct {
	Reason string `json:"reason"`
}

func (s *Server) listFillSessions(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.deps.Fills.List()})
}

func (s *Server) startFill(writer http.ResponseWriter, request *http.Request) {
	var input startFillRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	outcome, err := s.deps.Fills.StartManual(request.Context(), input.VesselID, input.Liters)
	if err != nil {
		status := http.StatusConflict
		if !errors.Is(err, fill.ErrActiveSession) && !errors.Is(err, circuit.ErrCircuitBusy) {
			status = http.StatusBadRequest
		}
		writeError(writer, status, err.Error())
		return
	}
	// 开阀命令已持久化但结果未知：不宣告成功也不宣告失败，返回 202 待确认。
	if outcome.Awaiting {
		writeJSON(writer, http.StatusAccepted, outcome)
		return
	}
	writeJSON(writer, http.StatusCreated, outcome)
}

func (s *Server) beginSoak(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid session id")
		return
	}
	session, err := s.deps.Fills.BeginSoak(id)
	if err != nil {
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, session)
}

func (s *Server) completeFill(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid session id")
		return
	}
	session, err := s.deps.Fills.Complete(request.Context(), id)
	if err != nil {
		// 关阀结果未知：回路已保留为故障，返回 202 并附上挂起会话，引导人工对账。
		if errors.Is(err, fill.ErrCommandUnconfirmed) {
			writeJSON(writer, http.StatusAccepted, map[string]any{
				"session":  session,
				"awaiting": true,
				"message":  err.Error(),
			})
			return
		}
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, session)
}

func (s *Server) abortFill(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid session id")
		return
	}
	var input abortFillRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	session, err := s.deps.Fills.Abort(request.Context(), id, input.Reason)
	if err != nil {
		if errors.Is(err, fill.ErrCommandUnconfirmed) {
			writeJSON(writer, http.StatusAccepted, map[string]any{
				"session":  session,
				"awaiting": true,
				"message":  err.Error(),
			})
			return
		}
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, session)
}

func (s *Server) recoverFillIsolation(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid session id")
		return
	}
	if err := s.deps.Fills.RecoverIsolation(id); err != nil {
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "recovered"})
}

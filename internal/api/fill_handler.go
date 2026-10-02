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
	session, err := s.deps.Fills.StartManual(request.Context(), input.VesselID, input.Liters)
	if err != nil {
		status := http.StatusConflict
		if !errors.Is(err, fill.ErrActiveSession) && !errors.Is(err, circuit.ErrCircuitBusy) && !errors.Is(err, fill.ErrCommandUnconfirmed) {
			status = http.StatusBadRequest
		}
		writeError(writer, status, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, session)
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

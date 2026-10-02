package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-98/internal/model"
	"github.com/wyw14/cry-98/internal/valve"
)

type reconcileCommandRequest struct {
	Executed bool   `json:"executed"`
	Operator string `json:"operator"`
	Note     string `json:"note"`
}

// listValveCommands is the pending-command query entry: without a filter it
// returns every recorded command; ?status=pending|acknowledged|reconciled
// narrows it to one state.
func (s *Server) listValveCommands(writer http.ResponseWriter, request *http.Request) {
	status := request.URL.Query().Get("status")
	var items []model.ValveCommandRecord
	switch model.ValveCommandStatus(status) {
	case "":
		items = s.deps.ValveCommands.List()
	case model.ValveCommandPending:
		items = s.deps.ValveCommands.Pending()
	case model.ValveCommandAcknowledged, model.ValveCommandReconciled:
		for _, record := range s.deps.ValveCommands.List() {
			if record.Status == model.ValveCommandStatus(status) {
				items = append(items, record)
			}
		}
	default:
		writeError(writer, http.StatusBadRequest, "invalid status filter")
		return
	}
	if items == nil {
		items = []model.ValveCommandRecord{}
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) getValveCommand(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid command id")
		return
	}
	record, ok := s.deps.ValveCommands.Get(id)
	if !ok {
		writeError(writer, http.StatusNotFound, "valve command not found")
		return
	}
	writeJSON(writer, http.StatusOK, record)
}

// reconcileValveCommand is the manual reconciliation entry: an operator
// confirms the physical outcome of a command whose receipt never arrived,
// and the circuit reservation is reconciled with it.
func (s *Server) reconcileValveCommand(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid command id")
		return
	}
	var input reconcileCommandRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if input.Operator == "" {
		writeError(writer, http.StatusBadRequest, "reconciliation operator is required")
		return
	}
	record, err := s.deps.ValveCommands.Reconcile(id, input.Executed, input.Operator, input.Note)
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, valve.ErrCommandNotFound) {
			status = http.StatusNotFound
		}
		writeError(writer, status, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, record)
}

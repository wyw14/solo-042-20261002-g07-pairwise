package api

import (
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type resolveCommandRequest struct {
	// Outcome 为人工现场核对结论：executed（阀门确已按命令动作）、
	// failed（确认未驱动阀门）、cancel（确认命令到达设备前作废）。
	Outcome  string `json:"outcome"`
	Operator string `json:"operator"`
	Note     string `json:"note"`
}

// listValveCommands 返回阀门命令，可用 ?status=inflight,awaiting 过滤，
// 默认只返回待确认命令，突出需要人工介入的项目。
func (s *Server) listValveCommands(writer http.ResponseWriter, request *http.Request) {
	filter := request.URL.Query().Get("status")
	var statuses []string
	if filter != "" {
		for _, value := range strings.Split(filter, ",") {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				statuses = append(statuses, trimmed)
			}
		}
	} else {
		statuses = []string{"inflight", "awaiting"}
	}
	items := s.deps.Fills.ListCommands(statuses...)
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) getValveCommand(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid command id")
		return
	}
	state, err := s.deps.Fills.GetCommand(id)
	if err != nil {
		writeError(writer, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, state)
}

// resolveValveCommand 是人工对账入口：操作员现场确认阀门实际位置后提交结论，
// 系统据此把关联补液会话与回路预留收敛到一致状态。
func (s *Server) resolveValveCommand(writer http.ResponseWriter, request *http.Request) {
	id, err := uuid.Parse(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, "invalid command id")
		return
	}
	var input resolveCommandRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	state, err := s.deps.Fills.ResolveCommand(request.Context(), id,
		strings.TrimSpace(input.Outcome), strings.TrimSpace(input.Operator), strings.TrimSpace(input.Note))
	if err != nil {
		writeError(writer, http.StatusConflict, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, state)
}

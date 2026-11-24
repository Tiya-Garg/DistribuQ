package dashboard

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/Tiya-Garg/DistribuQ/internal/workflow"
)

// HandlePipelinesHTTP handles GET /api/pipelines and POST /api/pipelines.
func (s *Server) handlePipelines(w http.ResponseWriter, r *http.Request) {
	wb := s.sched.WorkflowBridge()
	if wb == nil {
		http.Error(w, `{"error":"workflow engine not configured"}`, http.StatusNotImplemented)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if r.Method == http.MethodGet {
		path := strings.TrimPrefix(r.URL.Path, "/api/pipelines")
		path = strings.TrimPrefix(path, "/")

		if path != "" {
			// Query specific pipeline details
			pipeline, err := wb.Store().Get(path)
			if err != nil {
				http.Error(w, `{"error":"pipeline not found"}`, http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(pipeline)
			return
		}

		// List all pipelines
		list := wb.Store().List()
		_ = json.NewEncoder(w).Encode(list)
		return
	}

	if r.Method == http.MethodPost {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
			return
		}
		def, err := workflow.ParsePipelineJSON(body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		pipeline, err := wb.StartPipeline(r.Context(), def)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(pipeline)
		return
	}

	http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
}

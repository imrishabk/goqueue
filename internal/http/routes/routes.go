package routes

import (
	"net/http"

	"github.com/imrishabk/goqueue/internal/http/handler"
)

// NewRouter wires HTTP routes at the HTTP seam.
// Uses Go 1.22+ pattern syntax with PathValue to avoid manual HasSuffix parsing.
func NewRouter(h *handler.Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", h.Health)

	// jobs
	mux.HandleFunc("GET /jobs", h.ListJobs)
	mux.HandleFunc("POST /jobs", h.CreateJob)
	mux.HandleFunc("GET /jobs/{id}", h.GetJob)

	// workers
	mux.HandleFunc("POST /workers/register", h.RegisterWorker)
	mux.HandleFunc("GET /workers", h.ListWorkers)
	mux.HandleFunc("POST /workers/{id}/heartbeat", h.Heartbeat)

	return mux
}

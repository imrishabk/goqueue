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
	mux.HandleFunc("GET /stats", h.Stats)

	// jobs
	mux.HandleFunc("GET /jobs", h.ListJobs)
	mux.HandleFunc("POST /jobs", h.CreateJob)
	mux.HandleFunc("GET /jobs/{id}", h.GetJob)
	mux.HandleFunc("POST /jobs/{id}/complete", h.CompleteJob)
	mux.HandleFunc("POST /jobs/{id}/fail", h.FailJob)
	mux.HandleFunc("GET /jobs/{id}/attempts", h.ListAttempts)
	mux.HandleFunc("DELETE /jobs/{id}", h.DeleteJob)
	mux.HandleFunc("PATCH /jobs/{id}", h.PatchJob)

	// workers
	mux.HandleFunc("POST /workers/register", h.RegisterWorker)
	mux.HandleFunc("GET /workers", h.ListWorkers)
	mux.HandleFunc("POST /workers/{id}/heartbeat", h.Heartbeat)
	mux.HandleFunc("POST /workers/{id}/poll", h.Poll)

	return mux
}

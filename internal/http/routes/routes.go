package routes

import (
	"net/http"

	"github.com/imrishabk/goqueue/internal/http/handler"
)

// NewRouter wires HTTP routes at the HTTP seam.
func NewRouter(h *handler.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.Health)
	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		h.ListJobs(w, r)
	})
	return mux
}

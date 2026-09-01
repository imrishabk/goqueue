package handler

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

// Handler exposes HTTP handlers at the HTTP seam.
type Handler struct {
	store store.Store
}

// NewHandler creates a Handler backed by the given Store.
func NewHandler(s store.Store) *Handler {
	return &Handler{store: s}
}

// Health returns 200 with {"status":"ok"}.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ListJobs handles GET /jobs?limit=&offset=&status=&type=
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// defaults
	limit := 20
	offset := 0

	if s := q.Get("limit"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 0 {
			http.Error(w, `{"error":"invalid limit"}`, http.StatusBadRequest)
			return
		}
		if v > 100 {
			v = 100
		}
		limit = v
	}
	if s := q.Get("offset"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 0 {
			http.Error(w, `{"error":"invalid offset"}`, http.StatusBadRequest)
			return
		}
		offset = v
	}

	page := store.Pagination{
		Limit:  &limit,
		OffSet: &offset,
	}

	// For 01, filter is empty (no status/type filtering yet)
	filter := store.JobFilter{}

	jobs, err := h.store.ListJobs(r.Context(), filter, page)
	if err != nil {
		http.Error(w, `{"error":"failed to list jobs"}`, http.StatusInternalServerError)
		return
	}
	if jobs == nil {
		jobs = []model.Job{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(jobs)
}

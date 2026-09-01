package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
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

// --- helpers to address review judgements ---

// parsePagination extracts limit/offset with defaults 20/0, caps limit at 100.
func parsePagination(r *http.Request) (store.Pagination, error) {
	q := r.URL.Query()
	limit := 20
	offset := 0
	if s := q.Get("limit"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 0 {
			return store.Pagination{}, err
		}
		if v > 100 {
			v = 100
		}
		limit = v
	}
	if s := q.Get("offset"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 0 {
			return store.Pagination{}, err
		}
		offset = v
	}
	return store.Pagination{Limit: &limit, OffSet: &offset}, nil
}

// parseScheduledAt converts optional RFC3339 string to time, defaulting to now().
func parseScheduledAt(s *string) (time.Time, error) {
	if s == nil || *s == "" {
		return time.Now().UTC(), nil
	}
	t, err := time.Parse(time.RFC3339, *s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, *s)
		if err != nil {
			return time.Time{}, err
		}
	}
	return t, nil
}

// parseJobID extracts UUID from path /jobs/<id>
func parseJobID(path string) (uuid.UUID, error) {
	idStr := strings.TrimPrefix(path, "/jobs/")
	idStr = strings.TrimSuffix(idStr, "/")
	if idStr == "" || strings.Contains(idStr, "/") {
		return uuid.Nil, errInvalidID
	}
	return uuid.Parse(idStr)
}

var errInvalidID = errString("invalid id")

type errString string

func (e errString) Error() string { return string(e) }

// parseWorkerHeartbeatID extracts worker ID from /workers/<id>/heartbeat
func parseWorkerHeartbeatID(path string) (string, error) {
	trimmed := strings.TrimPrefix(path, "/workers/")
	if !strings.HasSuffix(trimmed, "/heartbeat") {
		return "", errInvalidID
	}
	id := strings.TrimSuffix(trimmed, "/heartbeat")
	id = strings.TrimSuffix(id, "/")
	if id == "" || strings.Contains(id, "/") {
		return "", errInvalidID
	}
	return id, nil
}

// createJobRequest mirrors POST /jobs body — scheduled_at remains string to keep JSON flexible,
// but parsing is delegated to parseScheduledAt to avoid Primitive Obsession in handler.
type createJobRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    *int16          `json:"priority"`
	MaxAttempts *int16          `json:"max_attempts"`
	ScheduledAt *string         `json:"scheduled_at"`
}

// CreateJob handles POST /jobs
func (h *Handler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Type) == "" {
		http.Error(w, `{"error":"type is required"}`, http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		http.Error(w, `{"error":"payload is required"}`, http.StatusBadRequest)
		return
	}
	var js json.RawMessage
	if err := json.Unmarshal(req.Payload, &js); err != nil {
		http.Error(w, `{"error":"payload must be valid json"}`, http.StatusBadRequest)
		return
	}

	priority := int16(0)
	if req.Priority != nil {
		priority = *req.Priority
	}
	maxAttempts := int16(3)
	if req.MaxAttempts != nil {
		if *req.MaxAttempts <= 0 {
			http.Error(w, `{"error":"max_attempts must be > 0"}`, http.StatusBadRequest)
			return
		}
		maxAttempts = *req.MaxAttempts
	}
	scheduledAt, err := parseScheduledAt(req.ScheduledAt)
	if err != nil {
		http.Error(w, `{"error":"scheduled_at must be RFC3339"}`, http.StatusBadRequest)
		return
	}

	job := &model.Job{
		Type:         req.Type,
		Payload:      req.Payload,
		Status:       model.JobStatusPending,
		Priority:     priority,
		MaxAttempts:  maxAttempts,
		AttemptCount: 0,
		ScheduledAt:  scheduledAt,
	}

	created, err := h.store.CreateJob(r.Context(), job)
	if err != nil {
		http.Error(w, `{"error":"failed to create job"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(created)
}

// GetJob handles GET /jobs/:id
func (h *Handler) GetJob(w http.ResponseWriter, r *http.Request) {
	// prefer PathValue when router uses pattern /jobs/{id}, fallback to manual parse for direct handler tests
	idStr := r.PathValue("id")
	var id uuid.UUID
	var err error
	if idStr != "" {
		id, err = uuid.Parse(idStr)
		if err != nil {
			http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
			return
		}
	} else {
		id, err = parseJobID(r.URL.Path)
		if err != nil {
			http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
			return
		}
	}
	job, err := h.store.GetJob(r.Context(), id)
	if err != nil {
		http.Error(w, `{"error":"failed to get job"}`, http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(job)
}

// registerWorkerRequest mirrors POST /workers/register
type registerWorkerRequest struct {
	ID           string   `json:"id"`
	Hostname     string   `json:"hostname"`
	Capabilities []string `json:"capabilities"`
}

// RegisterWorker handles POST /workers/register
func (h *Handler) RegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req registerWorkerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.ID) == "" {
		http.Error(w, `{"error":"id is required"}`, http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Hostname) == "" {
		http.Error(w, `{"error":"hostname is required"}`, http.StatusBadRequest)
		return
	}
	if req.Capabilities == nil {
		req.Capabilities = []string{}
	}
	now := time.Now().UTC()
	worker := &model.Worker{
		ID:            req.ID,
		Hostname:      req.Hostname,
		Status:        model.WorkerStatusAlive,
		Capabilities:  req.Capabilities,
		LastHeartbeat: now,
		RegisterdAt:   now,
	}
	created, err := h.store.CreateWorker(r.Context(), worker)
	if err != nil {
		http.Error(w, `{"error":"failed to create worker"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(created)
}

// Heartbeat handles POST /workers/:id/heartbeat
func (h *Handler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		var err error
		id, err = parseWorkerHeartbeatID(r.URL.Path)
		if err != nil {
			http.Error(w, `{"error":"invalid worker id"}`, http.StatusBadRequest)
			return
		}
	}
	existing, err := h.store.GetWorker(r.Context(), id)
	if err != nil {
		http.Error(w, `{"error":"failed to get worker"}`, http.StatusInternalServerError)
		return
	}
	if existing == nil {
		http.Error(w, `{"error":"worker not found"}`, http.StatusNotFound)
		return
	}
	now := time.Now().UTC()
	status := model.WorkerStatusAlive
	updated, err := h.store.UpdateWorker(r.Context(), id, store.WorkerUpdate{
		Status:        &status,
		LastHeartbeat: &now,
	})
	if err != nil {
		http.Error(w, `{"error":"failed to update worker"}`, http.StatusInternalServerError)
		return
	}
	if updated == nil {
		existing.LastHeartbeat = now
		existing.Status = status
		updated = existing
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(updated)
}

// ListWorkers handles GET /workers?limit=&offset=
func (h *Handler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	page, err := parsePagination(r)
	if err != nil {
		http.Error(w, `{"error":"invalid pagination"}`, http.StatusBadRequest)
		return
	}
	filter := store.WorkerFilter{}
	if s := r.URL.Query().Get("status"); s != "" {
		ws := model.WorkerStatus(s)
		filter.Status = &ws
	}
	workers, err := h.store.ListWorkers(r.Context(), filter, page)
	if err != nil {
		http.Error(w, `{"error":"failed to list workers"}`, http.StatusInternalServerError)
		return
	}
	if workers == nil {
		workers = []model.Worker{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(workers)
}

// ListJobs handles GET /jobs?limit=&offset=&status=&type=
func (h *Handler) ListJobs(w http.ResponseWriter, r *http.Request) {
	page, err := parsePagination(r)
	if err != nil {
		http.Error(w, `{"error":"invalid pagination"}`, http.StatusBadRequest)
		return
	}

	filter := store.JobFilter{}
	if s := r.URL.Query().Get("status"); s != "" {
		parts := strings.Split(s, ",")
		var statuses []model.JobStatus
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				statuses = append(statuses, model.JobStatus(p))
			}
		}
		if len(statuses) > 0 {
			filter.Status = statuses
		}
	}
	if t := r.URL.Query().Get("type"); t != "" {
		filter.Type = &t
	}

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

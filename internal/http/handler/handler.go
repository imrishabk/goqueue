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

// parsePollID extracts worker ID from /workers/<id>/poll
func parsePollID(path string) (string, error) {
	trimmed := strings.TrimPrefix(path, "/workers/")
	if !strings.HasSuffix(trimmed, "/poll") {
		return "", errInvalidID
	}
	id := strings.TrimSuffix(trimmed, "/poll")
	id = strings.TrimSuffix(id, "/")
	if id == "" || strings.Contains(id, "/") {
		return "", errInvalidID
	}
	return id, nil
}

// parseJobActionID extracts job ID from /jobs/<id>/complete, /fail, /attempts
func parseJobActionID(path, suffix string) (uuid.UUID, error) {
	if !strings.HasSuffix(path, suffix) {
		return uuid.Nil, errInvalidID
	}
	trimmed := strings.TrimSuffix(path, suffix)
	idStr := strings.TrimPrefix(trimmed, "/jobs/")
	idStr = strings.TrimSuffix(idStr, "/")
	if idStr == "" || strings.Contains(idStr, "/") {
		return uuid.Nil, errInvalidID
	}
	return uuid.Parse(idStr)
}

// jobIDFromRequest extracts job ID from request, handling both router PathValue and direct handler tests.
// Handles /jobs/{id}, /jobs/{id}/complete, /jobs/{id}/fail, /jobs/{id}/attempts.
func jobIDFromRequest(r *http.Request) (uuid.UUID, error) {
	if idStr := r.PathValue("id"); idStr != "" {
		return uuid.Parse(idStr)
	}
	// fallback for direct handler tests without router
	path := strings.Trim(r.URL.Path, "/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 || parts[0] != "jobs" || parts[1] == "" {
		return uuid.Nil, errInvalidID
	}
	return uuid.Parse(parts[1])
}

// requireWorkerID decodes {"worker_id": "..."} and validates it.
func requireWorkerID(r *http.Request) (string, error) {
	var req struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		return "", err
	}
	if strings.TrimSpace(req.WorkerID) == "" {
		return "", errInvalidID
	}
	return req.WorkerID, nil
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
	id, err := jobIDFromRequest(r)
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
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
		RegisteredAt:  now,
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

// Poll handles POST /workers/:id/poll
// Capabilities are fetched via GetWorker and passed explicitly to ClaimNextJob
// to keep Store focused on jobs table (no worker JOIN) and to make the
// capability set observable at the HTTP seam for tests.
func (h *Handler) Poll(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		var err error
		id, err = parsePollID(r.URL.Path)
		if err != nil {
			http.Error(w, `{"error":"invalid worker id"}`, http.StatusBadRequest)
			return
		}
	}
	worker, err := h.store.GetWorker(r.Context(), id)
	if err != nil {
		http.Error(w, `{"error":"failed to get worker"}`, http.StatusInternalServerError)
		return
	}
	if worker == nil {
		http.Error(w, `{"error":"worker not found"}`, http.StatusNotFound)
		return
	}
	caps := worker.Capabilities
	if caps == nil {
		caps = []string{}
	}
	job, err := h.store.ClaimNextJob(r.Context(), id, caps)
	if err != nil {
		http.Error(w, `{"error":"failed to claim job"}`, http.StatusInternalServerError)
		return
	}
	if job == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(job)
}

// CompleteJob handles POST /jobs/:id/complete {worker_id}
func (h *Handler) CompleteJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := jobIDFromRequest(r)
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	workerID, err := requireWorkerID(r)
	if err != nil {
		if err == errInvalidID {
			http.Error(w, `{"error":"worker_id is required"}`, http.StatusBadRequest)
		} else {
			http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		}
		return
	}
	job, err := h.store.CompleteJob(r.Context(), jobID, workerID)
	if err != nil {
		http.Error(w, `{"error":"failed to complete job"}`, http.StatusInternalServerError)
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

// FailJob handles POST /jobs/:id/fail {worker_id, error}
func (h *Handler) FailJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := jobIDFromRequest(r)
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	var req struct {
		WorkerID string `json:"worker_id"`
		Error    string `json:"error"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.WorkerID) == "" {
		http.Error(w, `{"error":"worker_id is required"}`, http.StatusBadRequest)
		return
	}
	job, err := h.store.FailJob(r.Context(), jobID, req.WorkerID, req.Error)
	if err != nil {
		http.Error(w, `{"error":"failed to fail job"}`, http.StatusInternalServerError)
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

// ListAttempts handles GET /jobs/:id/attempts
func (h *Handler) ListAttempts(w http.ResponseWriter, r *http.Request) {
	jobID, err := jobIDFromRequest(r)
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	page, err := parsePagination(r)
	if err != nil {
		http.Error(w, `{"error":"invalid pagination"}`, http.StatusBadRequest)
		return
	}
	filter := store.JobAttemptFilter{JobID: &jobID}
	attempts, err := h.store.ListJobAttempts(r.Context(), filter, page)
	if err != nil {
		http.Error(w, `{"error":"failed to list attempts"}`, http.StatusInternalServerError)
		return
	}
	if attempts == nil {
		attempts = []model.JobAttempt{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(attempts)
}

// DeleteJob handles DELETE /jobs/:id.
// Non-running jobs are removed (204); running jobs conflict (409) so an
// owner can't pull work out from under a worker. Missing jobs are 404.
func (h *Handler) DeleteJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := jobIDFromRequest(r)
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	job, err := h.store.GetJob(r.Context(), jobID)
	if err != nil {
		http.Error(w, `{"error":"failed to get job"}`, http.StatusInternalServerError)
		return
	}
	if job == nil {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}
	if job.Status == model.JobStatusRunning {
		http.Error(w, `{"error":"job is running"}`, http.StatusConflict)
		return
	}
	if err := h.store.DeleteJob(r.Context(), jobID); err != nil {
		http.Error(w, `{"error":"failed to delete job"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// patchJobRequest mirrors PATCH /jobs body; at least one field required.
type patchJobRequest struct {
	Priority    *int16  `json:"priority"`
	MaxAttempts *int16  `json:"max_attempts"`
	ScheduledAt *string `json:"scheduled_at"`
}

// PatchJob handles PATCH /jobs/:id for priority/max_attempts/scheduled_at.
func (h *Handler) PatchJob(w http.ResponseWriter, r *http.Request) {
	jobID, err := jobIDFromRequest(r)
	if err != nil {
		http.Error(w, `{"error":"invalid id"}`, http.StatusBadRequest)
		return
	}
	var req patchJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if req.Priority == nil && req.MaxAttempts == nil && req.ScheduledAt == nil {
		http.Error(w, `{"error":"nothing to update"}`, http.StatusBadRequest)
		return
	}
	if req.MaxAttempts != nil && *req.MaxAttempts <= 0 {
		http.Error(w, `{"error":"max_attempts must be > 0"}`, http.StatusBadRequest)
		return
	}
	upd := store.JobUpdate{Priority: req.Priority, MaxAttempts: req.MaxAttempts}
	if req.ScheduledAt != nil {
		t, err := parseScheduledAt(req.ScheduledAt)
		if err != nil {
			http.Error(w, `{"error":"scheduled_at must be RFC3339"}`, http.StatusBadRequest)
			return
		}
		upd.ScheduledAt = &t
	}
	existing, err := h.store.GetJob(r.Context(), jobID)
	if err != nil {
		http.Error(w, `{"error":"failed to get job"}`, http.StatusInternalServerError)
		return
	}
	if existing == nil {
		http.Error(w, `{"error":"job not found"}`, http.StatusNotFound)
		return
	}
	updated, err := h.store.UpdateJob(r.Context(), jobID, upd)
	if err != nil {
		http.Error(w, `{"error":"failed to update job"}`, http.StatusInternalServerError)
		return
	}
	if updated == nil {
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

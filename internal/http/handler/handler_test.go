package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

// fakeStore is a minimal in-memory stub for Handler tests at the HTTP seam.
type fakeStore struct {
	jobs     []model.Job
	workers  []model.Worker
	attempts []model.JobAttempt
	listErr  error
}

func (f *fakeStore) CreateJob(_ context.Context, job *model.Job) (*model.Job, error) {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	// store a copy for GetJob tests
	f.jobs = append(f.jobs, *job)
	// return copy
	cp := *job
	return &cp, nil
}
func (f *fakeStore) GetJob(_ context.Context, id uuid.UUID) (*model.Job, error) {
	for _, j := range f.jobs {
		if j.ID == id {
			cp := j
			return &cp, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) ListJobs(_ context.Context, _ store.JobFilter, page store.Pagination) ([]model.Job, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	// honor pagination trivially for tests
	start := 0
	if page.OffSet != nil {
		start = *page.OffSet
		if start > len(f.jobs) {
			start = len(f.jobs)
		}
	}
	end := len(f.jobs)
	if page.Limit != nil {
		if start+*page.Limit < end {
			end = start + *page.Limit
		}
	}
	return f.jobs[start:end], nil
}
func (f *fakeStore) UpdateJob(_ context.Context, _ uuid.UUID, _ store.JobUpdate) (*model.Job, error) {
	return nil, nil
}
func (f *fakeStore) DeleteJob(_ context.Context, _ uuid.UUID) error { return nil }

// timePtr helper for nullable timestamp fields in tests.
func timePtr(t time.Time) *time.Time { return &t }

func (f *fakeStore) CompleteJob(_ context.Context, jobID uuid.UUID, workerID string) (*model.Job, error) {
	for i, j := range f.jobs {
		if j.ID == jobID {
			now := time.Now().UTC()
			f.jobs[i].Status = model.JobStatusSucceeded
			f.jobs[i].CompletedAt = timePtr(now)
			// close open attempt for this worker/job
			for k, a := range f.attempts {
				if a.JobID == jobID && a.WorkerID == workerID && a.FinishedAt == nil {
					f.attempts[k].FinishedAt = timePtr(now)
					f.attempts[k].Success = true
					f.attempts[k].WorkerID = workerID
					break
				}
			}
			// if no open attempt, create one as success
			found := false
			for _, a := range f.attempts {
				if a.JobID == jobID && a.WorkerID == workerID && a.Success {
					found = true
					break
				}
			}
			if !found {
				// create closed attempt if none existed
				f.attempts = append(f.attempts, model.JobAttempt{ID: uuid.New(), JobID: jobID, WorkerID: workerID, StartedAt: now.Add(-time.Second), FinishedAt: timePtr(now), Success: true})
			}
			cp := f.jobs[i]
			return &cp, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) FailJob(_ context.Context, jobID uuid.UUID, workerID string, errMsg string) (*model.Job, error) {
	for i, j := range f.jobs {
		if j.ID == jobID {
			now := time.Now().UTC()
			f.jobs[i].AttemptCount++
			// record attempt
			f.attempts = append(f.attempts, model.JobAttempt{
				ID: uuid.New(), JobID: jobID, WorkerID: workerID, StartedAt: now.Add(-time.Second), FinishedAt: timePtr(now), Success: false, Error: errMsg,
			})
			if f.jobs[i].AttemptCount >= f.jobs[i].MaxAttempts {
				f.jobs[i].Status = model.JobStatusDead
				f.jobs[i].DeadAt = timePtr(now)
			} else {
				f.jobs[i].Status = model.JobStatusPending
			}
			cp := f.jobs[i]
			return &cp, nil
		}
	}
	return nil, nil
}

func (f *fakeStore) ClaimNextJob(_ context.Context, _ string, capabilities []string) (*model.Job, error) {
	now := time.Now().UTC()
	// find candidates
	var candidates []int
	for i, j := range f.jobs {
		if j.Status != model.JobStatusPending {
			continue
		}
		if j.ScheduledAt.After(now) {
			continue
		}
		if len(capabilities) > 0 {
			match := false
			for _, c := range capabilities {
				if c == j.Type {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		candidates = append(candidates, i)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	// order by priority DESC, scheduled_at ASC, created_at ASC — use sort.Slice
	sort.Slice(candidates, func(i, j int) bool {
		a := f.jobs[candidates[i]]
		b := f.jobs[candidates[j]]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		if !a.ScheduledAt.Equal(b.ScheduledAt) {
			return a.ScheduledAt.Before(b.ScheduledAt)
		}
		return a.CreatedAt.Before(b.CreatedAt)
	})
	idx := candidates[0]
	f.jobs[idx].Status = model.JobStatusRunning
	cp := f.jobs[idx]
	return &cp, nil
}

func (f *fakeStore) CreateJobAttempt(_ context.Context, ja *model.JobAttempt) (*model.JobAttempt, error) {
	return ja, nil
}
func (f *fakeStore) GetJobAttempt(_ context.Context, _ uuid.UUID) (*model.JobAttempt, error) {
	return nil, nil
}
func (f *fakeStore) ListJobAttempts(_ context.Context, filter store.JobAttemptFilter, _ store.Pagination) ([]model.JobAttempt, error) {
	if filter.JobID != nil {
		var out []model.JobAttempt
		for _, a := range f.attempts {
			if a.JobID == *filter.JobID {
				out = append(out, a)
			}
		}
		return out, nil
	}
	return f.attempts, nil
}
func (f *fakeStore) UpdateJobAttempt(_ context.Context, _ uuid.UUID, _ store.JobAttemptUpdate) (*model.JobAttempt, error) {
	return nil, nil
}
func (f *fakeStore) DeleteJobAttempt(_ context.Context, _ uuid.UUID) error { return nil }

func (f *fakeStore) CreateWorker(_ context.Context, w *model.Worker) (*model.Worker, error) {
	if w.Status == "" {
		w.Status = model.WorkerStatusAlive
	}
	f.workers = append(f.workers, *w)
	cp := *w
	return &cp, nil
}
func (f *fakeStore) GetWorker(_ context.Context, id string) (*model.Worker, error) {
	for _, w := range f.workers {
		if w.ID == id {
			cp := w
			return &cp, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) ListWorkers(_ context.Context, _ store.WorkerFilter, page store.Pagination) ([]model.Worker, error) {
	start := 0
	if page.OffSet != nil {
		start = *page.OffSet
		if start > len(f.workers) {
			start = len(f.workers)
		}
	}
	end := len(f.workers)
	if page.Limit != nil {
		if start+*page.Limit < end {
			end = start + *page.Limit
		}
	}
	return f.workers[start:end], nil
}
func (f *fakeStore) UpdateWorker(_ context.Context, id string, upd store.WorkerUpdate) (*model.Worker, error) {
	for i, w := range f.workers {
		if w.ID == id {
			if upd.Status != nil {
				f.workers[i].Status = *upd.Status
			}
			if upd.LastHeartbeat != nil {
				f.workers[i].LastHeartbeat = *upd.LastHeartbeat
			}
			if upd.Hostname != nil {
				f.workers[i].Hostname = *upd.Hostname
			}
			cp := f.workers[i]
			return &cp, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) DeleteWorker(_ context.Context, _ string) error { return nil }

func (f *fakeStore) SweepDeadWorkers(_ context.Context, deadBefore time.Time) (int, int, error) {
	var deadIDs []string
	for i, w := range f.workers {
		if w.Status == model.WorkerStatusAlive && w.LastHeartbeat.Before(deadBefore) {
			f.workers[i].Status = model.WorkerStatusDead
			deadIDs = append(deadIDs, w.ID)
		}
	}
	if len(deadIDs) == 0 {
		return 0, 0, nil
	}
	deadSet := make(map[string]bool, len(deadIDs))
	for _, id := range deadIDs {
		deadSet[id] = true
	}
	requeued := 0
	// requeue running jobs with open attempts from dead workers
	for i, j := range f.jobs {
		if j.Status != model.JobStatusRunning {
			continue
		}
		for _, a := range f.attempts {
			if a.JobID == j.ID && deadSet[a.WorkerID] && a.FinishedAt == nil {
				f.jobs[i].Status = model.JobStatusPending
				requeued++
				break
			}
		}
	}
	// close open attempts
	for i, a := range f.attempts {
		if deadSet[a.WorkerID] && a.FinishedAt == nil {
			now := time.Now().UTC()
			f.attempts[i].FinishedAt = timePtr(now)
			f.attempts[i].Success = false
			f.attempts[i].Error = "worker died"
		}
	}
	return len(deadIDs), requeued, nil
}

var _ store.Store = (*fakeStore)(nil)

func TestHealth(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	h.Health(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %v", body)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json, got %s", ct)
	}
}

func TestListJobs_Empty(t *testing.T) {
	h := NewHandler(&fakeStore{jobs: []model.Job{}})
	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w := httptest.NewRecorder()
	h.ListJobs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", w.Code, w.Body.String())
	}
	var jobs []model.Job
	if err := json.NewDecoder(w.Body).Decode(&jobs); err != nil {
		t.Fatalf("decode: %v body %s", err, w.Body.String())
	}
	if len(jobs) != 0 {
		t.Fatalf("expected 0 jobs, got %d", len(jobs))
	}
}

func TestListJobs_Pagination(t *testing.T) {
	jobs := make([]model.Job, 5)
	for i := range jobs {
		jobs[i].ID = uuid.New()
	}
	h := NewHandler(&fakeStore{jobs: jobs})
	// limit=2 offset=1 should return 2 jobs (indices 1,2)
	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=2&offset=1", nil)
	w := httptest.NewRecorder()
	h.ListJobs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var got []model.Job
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(got))
	}
}

func TestListJobs_LimitCapped(t *testing.T) {
	h := NewHandler(&fakeStore{jobs: make([]model.Job, 5)})
	req := httptest.NewRequest(http.MethodGet, "/jobs?limit=500", nil)
	w := httptest.NewRecorder()
	h.ListJobs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	// should be capped to 100, so still returns 5 (all jobs) not error
	var got []model.Job
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5, got %d", len(got))
	}
}

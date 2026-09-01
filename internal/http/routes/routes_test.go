package routes

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/http/handler"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

type fakeStore struct {
	jobs    []model.Job
	workers []model.Worker
}

func (f *fakeStore) CreateJob(_ context.Context, job *model.Job) (*model.Job, error) {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	f.jobs = append(f.jobs, *job)
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
func (f *fakeStore) ListJobs(_ context.Context, _ store.JobFilter, _ store.Pagination) ([]model.Job, error) {
	return f.jobs, nil
}
func (f *fakeStore) UpdateJob(_ context.Context, _ uuid.UUID, _ store.JobUpdate) (*model.Job, error) {
	return nil, nil
}
func (f *fakeStore) DeleteJob(_ context.Context, _ uuid.UUID) error { return nil }
func (f *fakeStore) CompleteJob(_ context.Context, _ uuid.UUID, _ string) (*model.Job, error) {
	return nil, nil
}
func (f *fakeStore) FailJob(_ context.Context, _ uuid.UUID, _ string, _ string) (*model.Job, error) {
	return nil, nil
}
func (f *fakeStore) ClaimNextJob(_ context.Context, _ string, caps []string) (*model.Job, error) {
	now := time.Now().UTC()
	var candidates []int
	for i, j := range f.jobs {
		if j.Status != model.JobStatusPending {
			continue
		}
		if j.ScheduledAt.After(now) {
			continue
		}
		if len(caps) > 0 {
			match := false
			for _, c := range caps {
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
func (f *fakeStore) ListJobAttempts(_ context.Context, _ store.JobAttemptFilter, _ store.Pagination) ([]model.JobAttempt, error) {
	return nil, nil
}
func (f *fakeStore) UpdateJobAttempt(_ context.Context, _ uuid.UUID, _ store.JobAttemptUpdate) (*model.JobAttempt, error) {
	return nil, nil
}
func (f *fakeStore) DeleteJobAttempt(_ context.Context, _ uuid.UUID) error { return nil }
func (f *fakeStore) CreateWorker(_ context.Context, w *model.Worker) (*model.Worker, error) {
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
func (f *fakeStore) ListWorkers(_ context.Context, _ store.WorkerFilter, _ store.Pagination) ([]model.Worker, error) {
	return f.workers, nil
}
func (f *fakeStore) UpdateWorker(_ context.Context, id string, upd store.WorkerUpdate) (*model.Worker, error) {
	for i, w := range f.workers {
		if w.ID == id {
			if upd.LastHeartbeat != nil {
				f.workers[i].LastHeartbeat = *upd.LastHeartbeat
			}
			cp := f.workers[i]
			return &cp, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) DeleteWorker(_ context.Context, _ string) error { return nil }

var _ store.Store = (*fakeStore)(nil)

func TestRouter_HeartbeatViaPattern(t *testing.T) {
	fs := &fakeStore{workers: []model.Worker{{ID: "w1", Hostname: "h1"}}}
	h := handler.NewHandler(fs)
	r := NewRouter(h)
	req := httptest.NewRequest(http.MethodPost, "/workers/w1/heartbeat", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", w.Code, w.Body.String())
	}
}

func TestRouter_GetJobViaPattern(t *testing.T) {
	id := uuid.New()
	fs := &fakeStore{jobs: []model.Job{{ID: id, Type: "email"}}}
	h := handler.NewHandler(fs)
	r := NewRouter(h)
	req := httptest.NewRequest(http.MethodGet, "/jobs/"+id.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", w.Code, w.Body.String())
	}
}

func TestRouter_Health(t *testing.T) {
	h := handler.NewHandler(&fakeStore{})
	r := NewRouter(h)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestRouter_PollViaPattern(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	fs := &fakeStore{
		jobs:    []model.Job{{ID: uuid.New(), Type: "email", Status: model.JobStatusPending, Priority: 5, ScheduledAt: now, CreatedAt: now}},
		workers: []model.Worker{{ID: "w1", Hostname: "h1", Capabilities: []string{"email"}}},
	}
	h := handler.NewHandler(fs)
	r := NewRouter(h)
	req := httptest.NewRequest(http.MethodPost, "/workers/w1/poll", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", w.Code, w.Body.String())
	}
}

package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
)

func TestPoll_GenericWorkerClaimsAny(t *testing.T) {
	fs := &fakeStore{
		jobs: []model.Job{
			{ID: uuid.New(), Type: "email", Status: model.JobStatusPending, Priority: 0, ScheduledAt: time.Now().Add(-time.Minute), CreatedAt: time.Now()},
		},
		workers: []model.Worker{{ID: "w1", Hostname: "h1", Capabilities: []string{}}},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPost, "/workers/w1/poll", nil)
	w := httptest.NewRecorder()
	h.Poll(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", w.Code, w.Body.String())
	}
	var job model.Job
	if err := json.NewDecoder(w.Body).Decode(&job); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if job.Type != "email" {
		t.Fatalf("expected email, got %s", job.Type)
	}
	if job.Status != model.JobStatusRunning {
		t.Fatalf("expected running, got %s", job.Status)
	}
}

func TestPoll_TypedWorkerOnlyMatching(t *testing.T) {
	fs := &fakeStore{
		jobs: []model.Job{
			{ID: uuid.New(), Type: "resize", Status: model.JobStatusPending, ScheduledAt: time.Now().Add(-time.Minute)},
		},
		workers: []model.Worker{{ID: "w1", Hostname: "h1", Capabilities: []string{"email"}}},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPost, "/workers/w1/poll", nil)
	w := httptest.NewRecorder()
	h.Poll(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d body %s", w.Code, w.Body.String())
	}
}

func TestPoll_PriorityOrdering(t *testing.T) {
	now := time.Now().Add(-time.Minute)
	fs := &fakeStore{
		jobs: []model.Job{
			{ID: uuid.New(), Type: "email", Status: model.JobStatusPending, Priority: 0, ScheduledAt: now, CreatedAt: now},
			{ID: uuid.New(), Type: "email", Status: model.JobStatusPending, Priority: 10, ScheduledAt: now, CreatedAt: now.Add(time.Second)},
		},
		workers: []model.Worker{{ID: "w1", Capabilities: []string{"email"}}},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPost, "/workers/w1/poll", nil)
	w := httptest.NewRecorder()
	h.Poll(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var job model.Job
	_ = json.NewDecoder(w.Body).Decode(&job)
	if job.Priority != 10 {
		t.Fatalf("expected priority 10, got %d", job.Priority)
	}
}

func TestPoll_ScheduledDelay(t *testing.T) {
	fs := &fakeStore{
		jobs: []model.Job{
			{ID: uuid.New(), Type: "email", Status: model.JobStatusPending, ScheduledAt: time.Now().Add(time.Hour)},
		},
		workers: []model.Worker{{ID: "w1", Capabilities: []string{}}},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPost, "/workers/w1/poll", nil)
	w := httptest.NewRecorder()
	h.Poll(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for future job, got %d", w.Code)
	}
}

func TestPoll_NoJobReturns204(t *testing.T) {
	fs := &fakeStore{
		jobs:    []model.Job{},
		workers: []model.Worker{{ID: "w1"}},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPost, "/workers/w1/poll", nil)
	w := httptest.NewRecorder()
	h.Poll(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}

func TestPoll_WorkerNotFound(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodPost, "/workers/notfound/poll", nil)
	w := httptest.NewRecorder()
	h.Poll(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestPoll_TypedWorkerMatches(t *testing.T) {
	fs := &fakeStore{
		jobs: []model.Job{
			{ID: uuid.New(), Type: "email", Status: model.JobStatusPending, ScheduledAt: time.Now().Add(-time.Minute)},
		},
		workers: []model.Worker{{ID: "w1", Capabilities: []string{"email"}}},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPost, "/workers/w1/poll", nil)
	w := httptest.NewRecorder()
	h.Poll(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

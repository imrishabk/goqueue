package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
)

func TestComplete_Success(t *testing.T) {
	jobID := uuid.New()
	fs := &fakeStore{
		jobs:    []model.Job{{ID: jobID, Type: "email", Status: model.JobStatusRunning, MaxAttempts: 3, AttemptCount: 0}},
		workers: []model.Worker{{ID: "w1"}},
	}
	// seed attempt as if claimed
	fs.attempts = []model.JobAttempt{{ID: uuid.New(), JobID: jobID, WorkerID: "w1", StartedAt: time.Now().Add(-time.Second)}}
	h := NewHandler(fs)
	body := `{"worker_id":"w1"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs/"+jobID.String()+"/complete", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.CompleteJob(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", w.Code, w.Body.String())
	}
	var job model.Job
	if err := json.NewDecoder(w.Body).Decode(&job); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if job.Status != model.JobStatusSucceeded {
		t.Fatalf("expected succeeded, got %s", job.Status)
	}
	if job.CompletedAt == nil {
		t.Fatalf("expected completed_at set")
	}
	// attempt should be closed
	if len(fs.attempts) == 0 || !fs.attempts[0].Success || fs.attempts[0].FinishedAt == nil {
		t.Fatalf("expected attempt success true and finished_at set, got %+v", fs.attempts[0])
	}
}

func TestComplete_NotFound(t *testing.T) {
	h := NewHandler(&fakeStore{})
	body := `{"worker_id":"w1"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs/"+uuid.New().String()+"/complete", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.CompleteJob(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestFail_RetryThenDead(t *testing.T) {
	jobID := uuid.New()
	fs := &fakeStore{
		jobs:    []model.Job{{ID: jobID, Type: "email", Status: model.JobStatusRunning, MaxAttempts: 3, AttemptCount: 0}},
		workers: []model.Worker{{ID: "w1"}},
	}
	h := NewHandler(fs)
	// first fail -> pending, attempt 1
	body := `{"worker_id":"w1","error":"boom"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs/"+jobID.String()+"/fail", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.FailJob(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("fail 1 expected 200, got %d", w.Code)
	}
	var job model.Job
	_ = json.NewDecoder(w.Body).Decode(&job)
	if job.Status != model.JobStatusPending {
		t.Fatalf("expected pending after 1st fail, got %s", job.Status)
	}
	if job.AttemptCount != 1 {
		t.Fatalf("expected attempt 1, got %d", job.AttemptCount)
	}
	if !job.ScheduledAt.After(time.Now().Add(-time.Second)) {
		t.Fatalf("expected backoff-rescheduled scheduled_at in future, got %s", job.ScheduledAt)
	}
	// need to set job back to running for next fail (simulate poll)
	fs.jobs[0].Status = model.JobStatusRunning
	// second fail -> pending, attempt 2
	req2 := httptest.NewRequest(http.MethodPost, "/jobs/"+jobID.String()+"/fail", bytes.NewBufferString(body))
	w2 := httptest.NewRecorder()
	h.FailJob(w2, req2)
	var job2 model.Job
	_ = json.NewDecoder(w2.Body).Decode(&job2)
	if job2.AttemptCount != 2 || job2.Status != model.JobStatusPending {
		t.Fatalf("expected pending 2, got %+v", job2)
	}
	fs.jobs[0].Status = model.JobStatusRunning
	// third fail -> dead
	req3 := httptest.NewRequest(http.MethodPost, "/jobs/"+jobID.String()+"/fail", bytes.NewBufferString(body))
	w3 := httptest.NewRecorder()
	h.FailJob(w3, req3)
	var job3 model.Job
	_ = json.NewDecoder(w3.Body).Decode(&job3)
	if job3.Status != model.JobStatusDead {
		t.Fatalf("expected dead after 3rd, got %s", job3.Status)
	}
	if job3.DeadAt == nil {
		t.Fatalf("expected dead_at set")
	}
	if len(fs.attempts) != 3 {
		t.Fatalf("expected 3 attempts, got %d", len(fs.attempts))
	}
}

func TestFail_BackoffHoldback(t *testing.T) {
	// A failed job must NOT be immediately reclaimable: poll returns 204 until backoff elapses.
	now := time.Now()
	fs := &fakeStore{
		jobs: []model.Job{
			{ID: uuid.New(), Type: "email", Status: model.JobStatusPending, MaxAttempts: 3, ScheduledAt: now.Add(-time.Minute)},
		},
		workers: []model.Worker{{ID: "w1", Capabilities: []string{"email"}}},
	}
	h := NewHandler(fs)

	// claim
	req := httptest.NewRequest(http.MethodPost, "/workers/w1/poll", nil)
	w := httptest.NewRecorder()
	h.Poll(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected claim 200, got %d", w.Code)
	}
	var claimed model.Job
	_ = json.NewDecoder(w.Body).Decode(&claimed)

	// fail -> pending with future scheduled_at
	before := time.Now()
	body := `{"worker_id":"w1","error":"boom"}`
	freq := httptest.NewRequest(http.MethodPost, "/jobs/"+claimed.ID.String()+"/fail", bytes.NewBufferString(body))
	fw := httptest.NewRecorder()
	h.FailJob(fw, freq)
	if fw.Code != http.StatusOK {
		t.Fatalf("expected fail 200, got %d", fw.Code)
	}
	var failed model.Job
	_ = json.NewDecoder(fw.Body).Decode(&failed)
	if !failed.ScheduledAt.After(before) {
		t.Fatalf("expected scheduled_at after fail time, got %s", failed.ScheduledAt)
	}

	// immediate repoll must find nothing (backoff)
	req2 := httptest.NewRequest(http.MethodPost, "/workers/w1/poll", nil)
	w2 := httptest.NewRecorder()
	h.Poll(w2, req2)
	if w2.Code != http.StatusNoContent {
		t.Fatalf("expected 204 during backoff, got %d body %s", w2.Code, w2.Body.String())
	}
}

func TestFail_ClosesOpenAttempt(t *testing.T) {
	// Fail must close the poll-opened attempt row, not insert a second one:
	// one fail cycle = exactly one attempt row.
	jobID := uuid.New()
	openID := uuid.New()
	fs := &fakeStore{
		jobs: []model.Job{{ID: jobID, Type: "email", Status: model.JobStatusRunning, MaxAttempts: 3}},
		attempts: []model.JobAttempt{
			{ID: openID, JobID: jobID, WorkerID: "w1", StartedAt: time.Now().Add(-time.Second)},
		},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPost, "/jobs/"+jobID.String()+"/fail", bytes.NewBufferString(`{"worker_id":"w1","error":"boom"}`))
	w := httptest.NewRecorder()
	h.FailJob(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if len(fs.attempts) != 1 {
		t.Fatalf("expected 1 attempt row (closed in place), got %d", len(fs.attempts))
	}
	got := fs.attempts[0]
	if got.ID != openID || got.FinishedAt == nil || got.Success || got.Error != "boom" {
		t.Fatalf("open attempt not closed properly: %+v", got)
	}
}

func TestListAttempts(t *testing.T) {
	jobID := uuid.New()
	fs := &fakeStore{
		jobs: []model.Job{{ID: jobID}},
		attempts: []model.JobAttempt{
			{ID: uuid.New(), JobID: jobID, WorkerID: "w1", StartedAt: time.Now().Add(-2 * time.Second), FinishedAt: timePtr(time.Now().Add(-time.Second)), Success: false},
			{ID: uuid.New(), JobID: jobID, WorkerID: "w1", StartedAt: time.Now(), Success: true},
		},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodGet, "/jobs/"+jobID.String()+"/attempts", nil)
	w := httptest.NewRecorder()
	h.ListAttempts(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var attempts []model.JobAttempt
	if err := json.NewDecoder(w.Body).Decode(&attempts); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("expected 2, got %d", len(attempts))
	}
}

func TestComplete_InvalidID(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodPost, "/jobs/not-uuid/complete", bytes.NewBufferString(`{"worker_id":"w1"}`))
	w := httptest.NewRecorder()
	h.CompleteJob(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

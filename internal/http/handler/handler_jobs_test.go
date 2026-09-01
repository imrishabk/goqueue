package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
)

func TestCreateJob_Valid(t *testing.T) {
	h := NewHandler(&fakeStore{})
	body := `{"type":"email","payload":{"to":"a@b.com"},"priority":5,"max_attempts":3}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateJob(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body %s", w.Code, w.Body.String())
	}
	var job model.Job
	if err := json.NewDecoder(w.Body).Decode(&job); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if job.Type != "email" {
		t.Fatalf("expected type email, got %s", job.Type)
	}
	if job.Priority != 5 {
		t.Fatalf("expected priority 5, got %d", job.Priority)
	}
	if job.Status != model.JobStatusPending {
		t.Fatalf("expected pending, got %s", job.Status)
	}
}

func TestCreateJob_MissingType(t *testing.T) {
	h := NewHandler(&fakeStore{})
	body := `{"payload":{"to":"a@b.com"}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.CreateJob(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestCreateJob_MissingPayload(t *testing.T) {
	h := NewHandler(&fakeStore{})
	body := `{"type":"email"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.CreateJob(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing payload, got %d", w.Code)
	}
}

func TestCreateJob_Defaults(t *testing.T) {
	h := NewHandler(&fakeStore{})
	body := `{"type":"email","payload":{"x":1}}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.CreateJob(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	var job model.Job
	_ = json.NewDecoder(w.Body).Decode(&job)
	if job.MaxAttempts != 3 {
		t.Fatalf("expected default max_attempts 3, got %d", job.MaxAttempts)
	}
	if job.Priority != 0 {
		t.Fatalf("expected default priority 0, got %d", job.Priority)
	}
	if job.ScheduledAt.IsZero() {
		t.Fatalf("expected scheduled_at default now, got zero")
	}
}

func TestGetJob_Found(t *testing.T) {
	id := uuid.New()
	fs := &fakeStore{jobs: []model.Job{{ID: id, Type: "email"}}}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodGet, "/jobs/"+id.String(), nil)
	w := httptest.NewRecorder()
	h.GetJob(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var job model.Job
	_ = json.NewDecoder(w.Body).Decode(&job)
	if job.ID != id {
		t.Fatalf("expected id %s, got %s", id, job.ID)
	}
}

func TestGetJob_NotFound(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodGet, "/jobs/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()
	h.GetJob(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestGetJob_InvalidUUID(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodGet, "/jobs/not-a-uuid", nil)
	w := httptest.NewRecorder()
	h.GetJob(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

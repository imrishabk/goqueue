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

func TestDeleteJob_Pending(t *testing.T) {
	jobID := uuid.New()
	fs := &fakeStore{
		jobs: []model.Job{{ID: jobID, Type: "email", Status: model.JobStatusPending}},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodDelete, "/jobs/"+jobID.String(), nil)
	w := httptest.NewRecorder()
	h.DeleteJob(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d body %s", w.Code, w.Body.String())
	}
	// gone afterwards
	req2 := httptest.NewRequest(http.MethodGet, "/jobs/"+jobID.String(), nil)
	w2 := httptest.NewRecorder()
	h.GetJob(w2, req2)
	if w2.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", w2.Code)
	}
}

func TestDeleteJob_RunningConflict(t *testing.T) {
	jobID := uuid.New()
	fs := &fakeStore{
		jobs: []model.Job{{ID: jobID, Type: "email", Status: model.JobStatusRunning}},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodDelete, "/jobs/"+jobID.String(), nil)
	w := httptest.NewRecorder()
	h.DeleteJob(w, req)
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", w.Code)
	}
	if fs.jobs[0].Status != model.JobStatusRunning {
		t.Fatalf("running job must survive delete, got %s", fs.jobs[0].Status)
	}
}

func TestDeleteJob_NotFound(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodDelete, "/jobs/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()
	h.DeleteJob(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestPatchJob_Priority(t *testing.T) {
	jobID := uuid.New()
	fs := &fakeStore{
		jobs: []model.Job{{ID: jobID, Type: "email", Status: model.JobStatusPending, Priority: 0}},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPatch, "/jobs/"+jobID.String(), bytes.NewBufferString(`{"priority":9}`))
	w := httptest.NewRecorder()
	h.PatchJob(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", w.Code, w.Body.String())
	}
	var job model.Job
	_ = json.NewDecoder(w.Body).Decode(&job)
	if job.Priority != 9 {
		t.Fatalf("expected priority 9, got %d", job.Priority)
	}
}

func TestPatchJob_Empty(t *testing.T) {
	jobID := uuid.New()
	fs := &fakeStore{jobs: []model.Job{{ID: jobID}}}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPatch, "/jobs/"+jobID.String(), bytes.NewBufferString(`{}`))
	w := httptest.NewRecorder()
	h.PatchJob(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestPatchJob_ScheduledAt(t *testing.T) {
	jobID := uuid.New()
	fs := &fakeStore{jobs: []model.Job{{ID: jobID}}}
	h := NewHandler(fs)
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	req := httptest.NewRequest(http.MethodPatch, "/jobs/"+jobID.String(), bytes.NewBufferString(`{"scheduled_at":"`+future+`"}`))
	w := httptest.NewRecorder()
	h.PatchJob(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", w.Code, w.Body.String())
	}
	if !fs.jobs[0].ScheduledAt.After(time.Now()) {
		t.Fatalf("expected future scheduled_at, got %s", fs.jobs[0].ScheduledAt)
	}
}

func TestPatchJob_NotFound(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodPatch, "/jobs/"+uuid.New().String(), bytes.NewBufferString(`{"priority":1}`))
	w := httptest.NewRecorder()
	h.PatchJob(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

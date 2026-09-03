package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/imrishabk/goqueue/internal/model"
)

func TestStats_Counts(t *testing.T) {
	fs := &fakeStore{
		jobs: []model.Job{
			{Status: model.JobStatusPending},
			{Status: model.JobStatusPending},
			{Status: model.JobStatusRunning},
			{Status: model.JobStatusDead},
		},
		workers: []model.Worker{
			{ID: "w1", Status: model.WorkerStatusAlive},
			{ID: "w2", Status: model.WorkerStatusDead},
		},
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()
	h.Stats(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body struct {
		Jobs    map[string]int64 `json:"jobs"`
		Workers map[string]int64 `json:"workers"`
	}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Jobs["pending"] != 2 || body.Jobs["running"] != 1 || body.Jobs["dead"] != 1 {
		t.Fatalf("bad job counts: %v", body.Jobs)
	}
	if body.Jobs["succeeded"] != 0 || body.Jobs["failed"] != 0 {
		t.Fatalf("missing statuses must be zero: %v", body.Jobs)
	}
	if body.Workers["alive"] != 1 || body.Workers["dead"] != 1 {
		t.Fatalf("bad worker counts: %v", body.Workers)
	}
}

func TestStats_Empty(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()
	h.Stats(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body struct {
		Jobs    map[string]int64 `json:"jobs"`
		Workers map[string]int64 `json:"workers"`
	}
	_ = json.NewDecoder(w.Body).Decode(&body)
	if len(body.Jobs) != 5 || len(body.Workers) != 2 {
		t.Fatalf("expected full zero shape, got %+v", body)
	}
}

package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/imrishabk/goqueue/internal/model"
)

func TestRegisterWorker_Valid(t *testing.T) {
	h := NewHandler(&fakeStore{})
	body := `{"id":"worker-1","hostname":"host-1","capabilities":["email","resize"]}`
	req := httptest.NewRequest(http.MethodPost, "/workers/register", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.RegisterWorker(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body %s", w.Code, w.Body.String())
	}
	var wk model.Worker
	if err := json.NewDecoder(w.Body).Decode(&wk); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if wk.ID != "worker-1" {
		t.Fatalf("expected worker-1, got %s", wk.ID)
	}
	if wk.Status != model.WorkerStatusAlive {
		t.Fatalf("expected alive, got %s", wk.Status)
	}
	if len(wk.Capabilities) != 2 {
		t.Fatalf("expected 2 capabilities, got %v", wk.Capabilities)
	}
}

func TestRegisterWorker_MissingID(t *testing.T) {
	h := NewHandler(&fakeStore{})
	body := `{"hostname":"host-1"}`
	req := httptest.NewRequest(http.MethodPost, "/workers/register", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.RegisterWorker(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestRegisterWorker_MissingHostname(t *testing.T) {
	h := NewHandler(&fakeStore{})
	body := `{"id":"worker-1"}`
	req := httptest.NewRequest(http.MethodPost, "/workers/register", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.RegisterWorker(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHeartbeat_Valid(t *testing.T) {
	fs := &fakeStore{}
	h := NewHandler(fs)
	// first register
	body := `{"id":"worker-1","hostname":"host-1"}`
	req := httptest.NewRequest(http.MethodPost, "/workers/register", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	h.RegisterWorker(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("setup register failed %d", w.Code)
	}
	// heartbeat
	req2 := httptest.NewRequest(http.MethodPost, "/workers/worker-1/heartbeat", nil)
	w2 := httptest.NewRecorder()
	h.Heartbeat(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", w2.Code, w2.Body.String())
	}
	var wk model.Worker
	_ = json.NewDecoder(w2.Body).Decode(&wk)
	if wk.ID != "worker-1" {
		t.Fatalf("expected worker-1, got %s", wk.ID)
	}
}

func TestHeartbeat_NotFound(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodPost, "/workers/not-found/heartbeat", nil)
	w := httptest.NewRecorder()
	h.Heartbeat(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestListWorkers_Empty(t *testing.T) {
	h := NewHandler(&fakeStore{})
	req := httptest.NewRequest(http.MethodGet, "/workers", nil)
	w := httptest.NewRecorder()
	h.ListWorkers(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var workers []model.Worker
	if err := json.NewDecoder(w.Body).Decode(&workers); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(workers) != 0 {
		t.Fatalf("expected 0, got %d", len(workers))
	}
}

func TestListWorkers_Pagination(t *testing.T) {
	fs := &fakeStore{}
	h := NewHandler(fs)
	// register 3 workers
	for i := 0; i < 3; i++ {
		body := `{"id":"w` + string(rune('0'+i)) + `","hostname":"h` + string(rune('0'+i)) + `"}`
		req := httptest.NewRequest(http.MethodPost, "/workers/register", bytes.NewBufferString(body))
		w := httptest.NewRecorder()
		h.RegisterWorker(w, req)
	}
	req := httptest.NewRequest(http.MethodGet, "/workers?limit=2&offset=1", nil)
	w := httptest.NewRecorder()
	h.ListWorkers(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var got []model.Worker
	_ = json.NewDecoder(w.Body).Decode(&got)
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

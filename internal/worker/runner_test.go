package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// fakeCoord is a minimal in-memory coordinator for Runner tests.
type fakeCoord struct {
	mu        sync.Mutex
	jobID     string
	job       map[string]any
	polled    int
	completed []string
	failed    map[string]string
	complete404 bool
}

func newFakeCoord() *fakeCoord {
	return &fakeCoord{jobID: uuid.New().String()}
}

func (f *fakeCoord) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/workers/register":
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"w1","status":"alive"}`))
	case r.URL.Path == "/workers/w1/heartbeat":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	case r.URL.Path == "/workers/w1/poll":
		f.mu.Lock()
		defer f.mu.Unlock()
		f.polled++
		if f.polled > 1 || f.job == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(f.job)
	case r.URL.Path == "/jobs/"+f.jobID+"/complete":
		if f.complete404 {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"gone"}`))
			return
		}
		f.mu.Lock()
		f.completed = append(f.completed, f.jobID)
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"` + f.jobID + `","status":"succeeded"}`))
	case r.URL.Path == "/jobs/"+f.jobID+"/fail":
		var body struct {
			WorkerID string `json:"worker_id"`
			Error    string `json:"error"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		if f.failed == nil {
			f.failed = map[string]string{}
		}
		f.failed[f.jobID] = body.Error
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"` + f.jobID + `","status":"pending"}`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func emailJob(id string) map[string]any {
	return map[string]any{
		"id": id, "type": "email", "status": "running", "payload": map[string]any{"to": "a"},
	}
}

func testRunner(t *testing.T, coord *fakeCoord) *Runner {
	t.Helper()
	srv := httptest.NewServer(coord)
	t.Cleanup(srv.Close)
	reg := NewRegistry()
	reg.Register("email", func(ctx context.Context, p json.RawMessage) error { return nil })
	return NewRunner(Config{
		CoordinatorURL:    srv.URL,
		WorkerID:          "w1",
		Hostname:          "h1",
		PollInterval:      10 * time.Millisecond,
		HeartbeatInterval: 10 * time.Millisecond,
	}, reg)
}

func TestRunner_ClaimExecuteComplete(t *testing.T) {
	coord := newFakeCoord()
	coord.job = emailJob(coord.jobID)
	r := testRunner(t, coord)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Run(ctx)
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if len(coord.completed) != 1 {
		t.Fatalf("expected 1 complete, got %v", coord.completed)
	}
}

func TestRunner_HandlerErrorFailsJob(t *testing.T) {
	coord := newFakeCoord()
	coord.job = emailJob(coord.jobID)
	srv := httptest.NewServer(coord)
	t.Cleanup(srv.Close)
	reg := NewRegistry()
	reg.Register("email", func(ctx context.Context, p json.RawMessage) error {
		return errTestBoom
	})
	r := NewRunner(Config{
		CoordinatorURL: srv.URL, WorkerID: "w1", Hostname: "h1",
		PollInterval: 10 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond,
	}, reg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.Run(ctx)
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if coord.failed[coord.jobID] != "boom" {
		t.Fatalf("expected fail with boom, got %v", coord.failed)
	}
	if len(coord.completed) != 0 {
		t.Fatalf("must not complete failed job, got %v", coord.completed)
	}
}

func TestRunner_Complete404MovesOn(t *testing.T) {
	coord := newFakeCoord()
	coord.job = emailJob(coord.jobID)
	coord.complete404 = true
	r := testRunner(t, coord)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	// must return via ctx, not spin forever on the gone job
	if err := r.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
	coord.mu.Lock()
	defer coord.mu.Unlock()
	if len(coord.failed) != 0 {
		t.Fatalf("gone job must not be failed, got %v", coord.failed)
	}
}

var errTestBoom = errors.New("boom")

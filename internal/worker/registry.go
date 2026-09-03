// Package worker runs the worker side of the queue: register with the
// coordinator, heartbeat, poll for jobs, execute them through a Registry
// of per-type Handlers, and report complete/fail.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// ErrUnknownHandler is returned when no handler is registered for a job
// type and no default ("") handler exists.
var ErrUnknownHandler = errors.New("unknown handler for job type")

// Handler executes one job payload. Return nil on success, non-nil to fail
// the job with the error message.
type Handler func(ctx context.Context, payload json.RawMessage) error

// Registry maps job types to Handlers. The zero value is unusable; use NewRegistry.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register binds a job type to a handler. Registering under "" sets the
// default handler used for types with no specific registration.
func (r *Registry) Register(jobType string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = h
}

// Execute runs the handler for jobType (or the default), or returns
// ErrUnknownHandler wrapping the type when neither exists.
func (r *Registry) Execute(ctx context.Context, jobType string, payload json.RawMessage) error {
	r.mu.RLock()
	h, ok := r.handlers[jobType]
	if !ok {
		h, ok = r.handlers[""]
	}
	r.mu.RUnlock()
	if !ok {
		return errors.Join(ErrUnknownHandler, errors.New(jobType))
	}
	return h(ctx, payload)
}

// DefaultHandler logs the payload and succeeds. It preserves the MVP
// behavior for types without a specific handler.
func DefaultHandler(jobType string) Handler {
	return func(ctx context.Context, payload json.RawMessage) error {
		return nil
	}
}

// Scripted is a test/demo handler driven by payload fields:
// {"sleep_ms": N, "fail": "msg"} sleeps N ms (cancellable) then fails
// with msg, or succeeds when fail is empty.
func Scripted(ctx context.Context, payload json.RawMessage) error {
	var s struct {
		SleepMS int    `json:"sleep_ms"`
		Fail    string `json:"fail"`
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &s); err != nil {
			return err
		}
	}
	if s.SleepMS > 0 {
		t := time.NewTimer(time.Duration(s.SleepMS) * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	if s.Fail != "" {
		return errors.New(s.Fail)
	}
	return nil
}

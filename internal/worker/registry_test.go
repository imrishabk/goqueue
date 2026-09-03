package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestRegistry_ExecuteRegistered(t *testing.T) {
	r := NewRegistry()
	called := false
	r.Register("email", func(ctx context.Context, p json.RawMessage) error {
		called = true
		var v struct{ To string `json:"to"` }
		if err := json.Unmarshal(p, &v); err != nil {
			t.Fatalf("payload: %v", err)
		}
		if v.To != "a@b.com" {
			t.Fatalf("unexpected payload: %s", p)
		}
		return nil
	})
	if err := r.Execute(context.Background(), "email", json.RawMessage(`{"to":"a@b.com"}`)); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !called {
		t.Fatal("handler not called")
	}
}

func TestRegistry_UnknownFallsBackToDefault(t *testing.T) {
	r := NewRegistry()
	r.Register("", func(ctx context.Context, p json.RawMessage) error { return nil })
	if err := r.Execute(context.Background(), "nope", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("expected default fallback, got %v", err)
	}
}

func TestRegistry_UnknownWithoutDefault(t *testing.T) {
	r := NewRegistry()
	err := r.Execute(context.Background(), "nope", json.RawMessage(`{}`))
	if !errors.Is(err, ErrUnknownHandler) {
		t.Fatalf("expected ErrUnknownHandler, got %v", err)
	}
}

func TestRegistry_HandlerErrorPropagates(t *testing.T) {
	r := NewRegistry()
	boom := errors.New("boom")
	r.Register("x", func(ctx context.Context, p json.RawMessage) error { return boom })
	if err := r.Execute(context.Background(), "x", json.RawMessage(`{}`)); !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
}

func TestScripted_SleepAndFail(t *testing.T) {
	start := time.Now()
	err := Scripted(context.Background(), json.RawMessage(`{"sleep_ms":50,"fail":"kaput"}`))
	if err == nil || err.Error() != "kaput" {
		t.Fatalf("expected kaput, got %v", err)
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("expected sleep to be honored")
	}
	if err := Scripted(context.Background(), json.RawMessage(`{}`)); err != nil {
		t.Fatalf("empty script should succeed, got %v", err)
	}
}

func TestScripted_Cancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Scripted(ctx, json.RawMessage(`{"sleep_ms":5000}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancel, got %v", err)
	}
}

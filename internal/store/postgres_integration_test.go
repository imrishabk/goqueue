//go:build integration

// Integration tests against real Postgres (docker `make up`, port 5462).
// Run: DATABASE_URL=postgres://distqueue:distqueue@localhost:5462/distqueue?sslmode=disable go test -tags integration ./internal/store/
// Unit suite (`go test ./...`) stays DB-free; CI runs these with a postgres service.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/database"
	"github.com/imrishabk/goqueue/internal/model"
)

// testCleaner is implemented by pgStore for test isolation (not part of Store).
type testCleaner interface {
	DeleteAllForTest(ctx context.Context) error
}

func testPool(t *testing.T) Store {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
	pool, err := database.NewPool(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	st := NewPGStore(pool)
	// isolate: wipe tables (tests run sequentially, unique IDs throughout)
	ctx := context.Background()
	if err := st.(testCleaner).DeleteAllForTest(ctx); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	t.Cleanup(func() { _ = st.(testCleaner).DeleteAllForTest(context.Background()) })
	return st
}

func seedJob(t *testing.T, st Store, jobType string, prio int16, sched time.Time, maxAttempts int16) *model.Job {
	t.Helper()
	j, err := st.CreateJob(context.Background(), &model.Job{
		Type: jobType, Payload: json.RawMessage(`{"n":1}`), Status: model.JobStatusPending,
		Priority: prio, MaxAttempts: maxAttempts, ScheduledAt: sched,
	})
	if err != nil {
		t.Fatalf("seed job: %v", err)
	}
	return j
}

func seedWorker(t *testing.T, st Store, caps []string) string {
	t.Helper()
	id := fmt.Sprintf("itest-%s", uuid.New().String()[:8])
	_, err := st.CreateWorker(context.Background(), &model.Worker{
		ID: id, Hostname: "itest", Status: model.WorkerStatusAlive,
		Capabilities: caps, LastHeartbeat: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("seed worker: %v", err)
	}
	return id
}

func TestIntegration_ClaimOrdering(t *testing.T) {
	st := testPool(t)
	ctx := context.Background()
	now := time.Now().UTC()

	low := seedJob(t, st, "email", 0, now.Add(-time.Minute), 3)
	high := seedJob(t, st, "email", 10, now.Add(-time.Minute), 3)
	seedJob(t, st, "email", 99, now.Add(time.Hour), 3) // future: must not dispatch
	seedJob(t, st, "resize", 50, now.Add(-time.Minute), 3)
	w := seedWorker(t, st, []string{"email"})

	first, err := st.ClaimNextJob(ctx, w, []string{"email"})
	if err != nil || first == nil {
		t.Fatalf("claim 1: %+v err=%v", first, err)
	}
	if first.ID != high.ID {
		t.Fatalf("expected prio-10 job %s first, got %s prio=%d", high.ID, first.ID, first.Priority)
	}
	second, err := st.ClaimNextJob(ctx, w, []string{"email"})
	if err != nil || second == nil {
		t.Fatalf("claim 2: %+v err=%v", second, err)
	}
	if second.ID != low.ID {
		t.Fatalf("expected prio-0 job %s second, got %s", low.ID, second.ID)
	}
	third, err := st.ClaimNextJob(ctx, w, []string{"email"})
	if err != nil {
		t.Fatalf("claim 3 err: %v", err)
	}
	if third != nil {
		t.Fatalf("expected nil (only future/wrong-type left), got %s type=%s", third.ID, third.Type)
	}
}

func TestIntegration_ConcurrentClaimDistinct(t *testing.T) {
	st := testPool(t)
	ctx := context.Background()
	now := time.Now().UTC()

	seedJob(t, st, "email", 0, now.Add(-time.Minute), 3)
	seedJob(t, st, "email", 0, now.Add(-time.Minute), 3)
	w1 := seedWorker(t, st, []string{"email"})
	w2 := seedWorker(t, st, []string{"email"})

	var wg sync.WaitGroup
	got := make([]*model.Job, 2)
	errs := make([]error, 2)
	for i, w := range []string{w1, w2} {
		wg.Add(1)
		go func(i int, w string) {
			defer wg.Done()
			got[i], errs[i] = st.ClaimNextJob(ctx, w, []string{"email"})
		}(i, w)
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil || got[i] == nil {
			t.Fatalf("claim %d: %+v err=%v", i, got[i], errs[i])
		}
	}
	if got[0].ID == got[1].ID {
		t.Fatalf("SKIP LOCKED violated: both got %s", got[0].ID)
	}
}

func TestIntegration_SweepRequeue(t *testing.T) {
	st := testPool(t)
	ctx := context.Background()
	now := time.Now().UTC()

	j := seedJob(t, st, "email", 0, now.Add(-time.Minute), 3)
	w := seedWorker(t, st, []string{"email"})
	claimed, err := st.ClaimNextJob(ctx, w, []string{"email"})
	if err != nil || claimed == nil || claimed.ID != j.ID {
		t.Fatalf("claim: %+v err=%v", claimed, err)
	}
	// age the heartbeat out, then sweep
	old := now.Add(-time.Minute)
	if _, err := st.UpdateWorker(ctx, w, WorkerUpdate{LastHeartbeat: &old}); err != nil {
		t.Fatalf("age heartbeat: %v", err)
	}
	dead, requeued, err := st.SweepDeadWorkers(ctx, now.Add(-45*time.Second))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if dead != 1 || requeued != 1 {
		t.Fatalf("expected 1 dead 1 requeued, got %d %d", dead, requeued)
	}
	got, _ := st.GetJob(ctx, j.ID)
	if got.Status != model.JobStatusPending || got.AttemptCount != 0 {
		t.Fatalf("expected pending attempt=0, got %s attempt=%d", got.Status, got.AttemptCount)
	}
	wk, _ := st.GetWorker(ctx, w)
	if wk.Status != model.WorkerStatusDead {
		t.Fatalf("expected worker dead, got %s", wk.Status)
	}
}

func TestIntegration_IdempotentReplay(t *testing.T) {
	st := testPool(t)
	ctx := context.Background()
	key := "itest-" + uuid.New().String()
	now := time.Now().UTC()

	first, err := st.CreateJob(ctx, &model.Job{
		Type: "email", Payload: json.RawMessage(`{}`), Status: model.JobStatusPending,
		MaxAttempts: 3, ScheduledAt: now, IdempotencyKey: &key,
	})
	if err != nil {
		t.Fatalf("create 1: %v", err)
	}
	_, err = st.CreateJob(ctx, &model.Job{
		Type: "email", Payload: json.RawMessage(`{}`), Status: model.JobStatusPending,
		MaxAttempts: 3, ScheduledAt: now, IdempotencyKey: &key,
	})
	if err != ErrConflict {
		t.Fatalf("expected ErrConflict on replay race, got %v", err)
	}
	existing, err := st.GetJobByIdempotencyKey(ctx, key)
	if err != nil || existing == nil || existing.ID != first.ID {
		t.Fatalf("lookup: %+v err=%v", existing, err)
	}
}

func TestIntegration_CompleteAndBackoffFail(t *testing.T) {
	st := testPool(t)
	ctx := context.Background()
	now := time.Now().UTC()

	j := seedJob(t, st, "email", 0, now.Add(-time.Minute), 3)
	w := seedWorker(t, st, []string{"email"})
	claimed, err := st.ClaimNextJob(ctx, w, []string{"email"})
	if err != nil || claimed == nil || claimed.ID != j.ID {
		t.Fatalf("claim: %+v err=%v", claimed, err)
	}
	done, err := st.CompleteJob(ctx, claimed.ID, w)
	if err != nil || done.Status != model.JobStatusSucceeded || done.CompletedAt == nil {
		t.Fatalf("complete: %+v err=%v", done, err)
	}

	j2 := seedJob(t, st, "email", 0, now.Add(-time.Minute), 2)
	claimed2, err := st.ClaimNextJob(ctx, w, []string{"email"})
	if err != nil || claimed2 == nil || claimed2.ID != j2.ID {
		t.Fatalf("expected to claim j2, got %+v err=%v", claimed2, err)
	}
	f1, err := st.FailJob(ctx, j2.ID, w, "boom")
	if err != nil {
		t.Fatalf("fail 1: %v", err)
	}
	if f1.Status != model.JobStatusPending || f1.AttemptCount != 1 {
		t.Fatalf("fail 1: %+v", f1)
	}
	if !f1.ScheduledAt.After(now) {
		t.Fatalf("fail 1 must reschedule with backoff, got %s", f1.ScheduledAt)
	}
	// second fail -> dead (max_attempts=2); call FailJob directly since
	// backoff rescheduled the job out of the claim window
	f2, err := st.FailJob(ctx, j2.ID, w, "boom")
	if err != nil {
		t.Fatalf("fail 2: %v", err)
	}
	if f2.Status != model.JobStatusDead || f2.DeadAt == nil {
		t.Fatalf("fail 2: %+v", f2)
	}
}

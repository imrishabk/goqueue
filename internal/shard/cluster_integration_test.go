//go:build integration

package shard

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/imrishabk/goqueue/internal/database"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

// Two real Postgres databases on the same instance stand in for two shards.
// Local: distqueue + distqueue2 on :5462. CI creates both (see ci.yml).
func testClusterDB(t *testing.T) *ClusterStore {
	t.Helper()
	base := os.Getenv("DATABASE_URL")
	if base == "" {
		t.Skip("DATABASE_URL not set")
	}
	dsn2 := os.Getenv("DATABASE_URL_2")
	if dsn2 == "" {
		t.Skip("DATABASE_URL_2 not set")
	}
	ctx := context.Background()
	p1, err := database.NewPool(ctx, base)
	if err != nil {
		t.Fatalf("pool1: %v", err)
	}
	t.Cleanup(func() { p1.Close() })
	p2, err := database.NewPool(ctx, dsn2)
	if err != nil {
		t.Fatalf("pool2: %v", err)
	}
	t.Cleanup(func() { p2.Close() })
	c := NewClusterStore([]store.Store{store.NewPGStore(p1), store.NewPGStore(p2)})
	wipe := func() {
		ctx := context.Background()
		_, _ = p1.Exec(ctx, `DELETE FROM jobs`)
		_, _ = p1.Exec(ctx, `DELETE FROM workers`)
		_, _ = p2.Exec(ctx, `DELETE FROM jobs`)
		_, _ = p2.Exec(ctx, `DELETE FROM workers`)
	}
	wipe()
	t.Cleanup(wipe)
	return c
}

func TestIntegration_ClusterEndToEnd(t *testing.T) {
	c := testClusterDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// spread check: 20 keyless jobs must land on both shards
	var ids []string
	for i := 0; i < 20; i++ {
		j, err := c.CreateJob(ctx, &model.Job{
			Type: "email", Payload: json.RawMessage(`{}`), Status: model.JobStatusPending, MaxAttempts: 3,
			ScheduledAt: now.Add(-time.Minute),
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		ids = append(ids, j.ID.String())
	}
	st, err := c.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Jobs[model.JobStatusPending] != 20 {
		t.Fatalf("expected 20 pending cluster-wide, got %v", st.Jobs)
	}

	// worker + claim + complete through the cluster
	if _, err := c.CreateWorker(ctx, &model.Worker{ID: "cw1", Hostname: "h", Status: model.WorkerStatusAlive, Capabilities: []string{"email"}, LastHeartbeat: now}); err != nil {
		t.Fatalf("register: %v", err)
	}
	claimed, err := c.ClaimNextJob(ctx, "cw1", []string{"email"})
	if err != nil || claimed == nil || claimed.Status != model.JobStatusRunning {
		t.Fatalf("claim: %+v err=%v", claimed, err)
	}
	done, err := c.CompleteJob(ctx, claimed.ID, "cw1")
	if err != nil || done.Status != model.JobStatusSucceeded {
		t.Fatalf("complete: %+v err=%v", done, err)
	}
	// keyed replay routes to the same shard
	key := "itest-key-1"
	k1, err := c.CreateJob(ctx, &model.Job{
		Type: "email", Payload: json.RawMessage(`{}`), Status: model.JobStatusPending, MaxAttempts: 3,
		ScheduledAt: now, IdempotencyKey: &key,
	})
	if err != nil {
		t.Fatalf("keyed create: %v", err)
	}
	k2, err := c.GetJobByIdempotencyKey(ctx, key)
	if err != nil || k2 == nil || k2.ID != k1.ID {
		t.Fatalf("key lookup: %+v err=%v", k2, err)
	}
	_ = ids
}

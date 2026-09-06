package shard

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

// memStore is an in-memory store.Store for cluster tests.
type memStore struct {
	jobs     map[uuid.UUID]*model.Job
	workers  map[string]*model.Worker
	attempts []model.JobAttempt
}

func newMemStore() *memStore {
	return &memStore{jobs: map[uuid.UUID]*model.Job{}, workers: map[string]*model.Worker{}}
}

func (m *memStore) CreateJob(_ context.Context, job *model.Job) (*model.Job, error) {
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	cp := *job
	m.jobs[cp.ID] = &cp
	return &cp, nil
}
func (m *memStore) GetJob(_ context.Context, id uuid.UUID) (*model.Job, error) {
	if j, ok := m.jobs[id]; ok {
		cp := *j
		return &cp, nil
	}
	return nil, nil
}
func (m *memStore) GetJobByIdempotencyKey(_ context.Context, key string) (*model.Job, error) {
	for _, j := range m.jobs {
		if j.IdempotencyKey != nil && *j.IdempotencyKey == key {
			cp := *j
			return &cp, nil
		}
	}
	return nil, nil
}
func (m *memStore) ListJobs(_ context.Context, _ store.JobFilter, _ store.Pagination) ([]model.Job, error) {
	var out []model.Job
	for _, j := range m.jobs {
		out = append(out, *j)
	}
	return out, nil
}
func (m *memStore) best(caps []string) *model.Job {
	now := time.Now().UTC()
	var cands []*model.Job
	for _, j := range m.jobs {
		if j.Status != model.JobStatusPending || j.ScheduledAt.After(now) {
			continue
		}
		if len(caps) > 0 {
			match := false
			for _, c := range caps {
				if c == j.Type {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		cands = append(cands, j)
	}
	if len(cands) == 0 {
		return nil
	}
	sort.Slice(cands, func(i, k int) bool { return betterJob(cands[i], cands[k]) })
	return cands[0]
}
func (m *memStore) PeekNextJob(_ context.Context, caps []string) (*model.Job, error) {
	if j := m.best(caps); j != nil {
		cp := *j
		return &cp, nil
	}
	return nil, nil
}
func (m *memStore) ClaimNextJob(_ context.Context, workerID string, caps []string) (*model.Job, error) {
	j := m.best(caps)
	if j == nil {
		return nil, nil
	}
	j.Status = model.JobStatusRunning
	m.attempts = append(m.attempts, model.JobAttempt{ID: uuid.New(), JobID: j.ID, WorkerID: workerID, StartedAt: time.Now().UTC()})
	cp := *j
	return &cp, nil
}
func (m *memStore) UpdateJob(_ context.Context, id uuid.UUID, u store.JobUpdate) (*model.Job, error) {
	j, ok := m.jobs[id]
	if !ok {
		return nil, nil
	}
	if u.Status != nil {
		j.Status = *u.Status
	}
	cp := *j
	return &cp, nil
}
func (m *memStore) DeleteJob(_ context.Context, id uuid.UUID) error {
	delete(m.jobs, id)
	return nil
}
func (m *memStore) CompleteJob(_ context.Context, id uuid.UUID, w string) (*model.Job, error) {
	j, ok := m.jobs[id]
	if !ok || j.Status != model.JobStatusRunning {
		return nil, nil
	}
	now := time.Now().UTC()
	j.Status = model.JobStatusSucceeded
	j.CompletedAt = &now
	for i, a := range m.attempts {
		if a.JobID == id && a.WorkerID == w && a.FinishedAt == nil {
			m.attempts[i].FinishedAt = &now
			m.attempts[i].Success = true
		}
	}
	cp := *j
	return &cp, nil
}
func (m *memStore) FailJob(_ context.Context, id uuid.UUID, w string, msg string) (*model.Job, error) {
	j, ok := m.jobs[id]
	if !ok {
		return nil, nil
	}
	now := time.Now().UTC()
	j.AttemptCount++
	closed := false
	for i, a := range m.attempts {
		if a.JobID == id && a.WorkerID == w && a.FinishedAt == nil {
			m.attempts[i].FinishedAt = &now
			m.attempts[i].Error = msg
			closed = true
		}
	}
	if !closed {
		m.attempts = append(m.attempts, model.JobAttempt{ID: uuid.New(), JobID: id, WorkerID: w, StartedAt: now, FinishedAt: &now, Error: msg})
	}
	if j.AttemptCount >= j.MaxAttempts {
		j.Status = model.JobStatusDead
		j.DeadAt = &now
	} else {
		j.Status = model.JobStatusPending
	}
	cp := *j
	return &cp, nil
}
func (m *memStore) CreateJobAttempt(_ context.Context, ja *model.JobAttempt) (*model.JobAttempt, error) {
	m.attempts = append(m.attempts, *ja)
	return ja, nil
}
func (m *memStore) GetJobAttempt(_ context.Context, id uuid.UUID) (*model.JobAttempt, error) {
	for _, a := range m.attempts {
		if a.ID == id {
			cp := a
			return &cp, nil
		}
	}
	return nil, nil
}
func (m *memStore) ListJobAttempts(_ context.Context, f store.JobAttemptFilter, _ store.Pagination) ([]model.JobAttempt, error) {
	var out []model.JobAttempt
	for _, a := range m.attempts {
		if f.JobID != nil && a.JobID != *f.JobID {
			continue
		}
		out = append(out, a)
	}
	return out, nil
}
func (m *memStore) UpdateJobAttempt(_ context.Context, id uuid.UUID, _ store.JobAttemptUpdate) (*model.JobAttempt, error) {
	return m.GetJobAttempt(context.Background(), id)
}
func (m *memStore) DeleteJobAttempt(_ context.Context, id uuid.UUID) error { return nil }
func (m *memStore) CreateWorker(_ context.Context, w *model.Worker) (*model.Worker, error) {
	cp := *w
	m.workers[cp.ID] = &cp
	return &cp, nil
}
func (m *memStore) GetWorker(_ context.Context, id string) (*model.Worker, error) {
	if w, ok := m.workers[id]; ok {
		cp := *w
		return &cp, nil
	}
	return nil, nil
}
func (m *memStore) ListWorkers(_ context.Context, _ store.WorkerFilter, _ store.Pagination) ([]model.Worker, error) {
	var out []model.Worker
	for _, w := range m.workers {
		out = append(out, *w)
	}
	return out, nil
}
func (m *memStore) UpdateWorker(_ context.Context, id string, u store.WorkerUpdate) (*model.Worker, error) {
	w, ok := m.workers[id]
	if !ok {
		return nil, nil
	}
	if u.Status != nil {
		w.Status = *u.Status
	}
	if u.LastHeartbeat != nil {
		w.LastHeartbeat = *u.LastHeartbeat
	}
	cp := *w
	return &cp, nil
}
func (m *memStore) DeleteWorker(_ context.Context, id string) error {
	delete(m.workers, id)
	return nil
}
func (m *memStore) MarkDeadWorkers(_ context.Context, before time.Time) ([]string, error) {
	var ids []string
	for _, w := range m.workers {
		if w.Status == model.WorkerStatusAlive && w.LastHeartbeat.Before(before) {
			w.Status = model.WorkerStatusDead
			ids = append(ids, w.ID)
		}
	}
	return ids, nil
}
func (m *memStore) RequeueJobsOfWorkers(_ context.Context, ids []string) (int, error) {
	set := map[string]bool{}
	for _, id := range ids {
		set[id] = true
	}
	n := 0
	for _, j := range m.jobs {
		if j.Status != model.JobStatusRunning {
			continue
		}
		for i, a := range m.attempts {
			if a.JobID == j.ID && set[a.WorkerID] && a.FinishedAt == nil {
				j.Status = model.JobStatusPending
				now := time.Now().UTC()
				m.attempts[i].FinishedAt = &now
				m.attempts[i].Error = "worker died"
				n++
				break
			}
		}
	}
	return n, nil
}
func (m *memStore) SweepDeadWorkers(_ context.Context, before time.Time) (int, int, error) {
	ids, _ := m.MarkDeadWorkers(context.Background(), before)
	n, _ := m.RequeueJobsOfWorkers(context.Background(), ids)
	return len(ids), n, nil
}
func (m *memStore) Stats(_ context.Context) (*store.Stats, error) {
	out := &store.Stats{Jobs: map[model.JobStatus]int64{}, Workers: map[model.WorkerStatus]int64{}}
	for _, j := range m.jobs {
		out.Jobs[j.Status]++
	}
	for _, w := range m.workers {
		out.Workers[w.Status]++
	}
	return out, nil
}

var _ store.Store = (*memStore)(nil)

func testCluster() (*ClusterStore, []*memStore) {
	s0, s1 := newMemStore(), newMemStore()
	return NewClusterStore([]store.Store{s0, s1}), []*memStore{s0, s1}
}

func seedPending(t *testing.T, c *ClusterStore, jobType string, prio int16) uuid.UUID {
	t.Helper()
	j, err := c.CreateJob(context.Background(), &model.Job{
		Type: jobType, Status: model.JobStatusPending, Priority: prio,
		MaxAttempts: 3, ScheduledAt: time.Now().Add(-time.Minute),
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	return j.ID
}

func TestCluster_RoutesByID(t *testing.T) {
	c, shards := testCluster()
	ctx := context.Background()
	var ids []uuid.UUID
	for i := 0; i < 20; i++ {
		ids = append(ids, seedPending(t, c, "email", 0))
	}
	for _, id := range ids {
		want := c.Router().ShardForID(id)
		if _, ok := shards[want].jobs[id]; !ok {
			t.Fatalf("job %s not on predicted shard %d", id, want)
		}
		got, err := c.GetJob(ctx, id)
		if err != nil || got == nil || got.ID != id {
			t.Fatalf("get %s: %+v err=%v", id, got, err)
		}
	}
}

func TestCluster_KeyAffinity(t *testing.T) {
	c, shards := testCluster()
	ctx := context.Background()
	key := "order-42"
	j, err := c.CreateJob(ctx, &model.Job{
		Type: "email", Status: model.JobStatusPending, MaxAttempts: 3,
		ScheduledAt: time.Now(), IdempotencyKey: &key,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	want := c.Router().ShardForKey(key)
	if _, ok := shards[want].jobs[j.ID]; !ok {
		t.Fatalf("keyed job not on key shard %d", want)
	}
	got, err := c.GetJobByIdempotencyKey(ctx, key)
	if err != nil || got == nil || got.ID != j.ID {
		t.Fatalf("key lookup: %+v err=%v", got, err)
	}
}

func TestCluster_ClaimMergesBest(t *testing.T) {
	c, shards := testCluster()
	ctx := context.Background()
	loID := seedPending(t, c, "email", 0)
	hiID := seedPending(t, c, "email", 10)
	// force cross-shard split regardless of hash
	lo, hi := shards[0], shards[1]
	for _, s := range shards {
		delete(s.jobs, loID)
		delete(s.jobs, hiID)
	}
	lo.jobs[loID] = &model.Job{ID: loID, Type: "email", Status: model.JobStatusPending, Priority: 0, MaxAttempts: 3, ScheduledAt: time.Now().Add(-time.Minute)}
	hi.jobs[hiID] = &model.Job{ID: hiID, Type: "email", Status: model.JobStatusPending, Priority: 10, MaxAttempts: 3, ScheduledAt: time.Now().Add(-time.Minute)}

	claimed, err := c.ClaimNextJob(ctx, "w1", []string{"email"})
	if err != nil || claimed == nil {
		t.Fatalf("claim: %+v err=%v", claimed, err)
	}
	if claimed.ID != hiID {
		t.Fatalf("expected hi-prio %s, got %s", hiID, claimed.ID)
	}
	if lo.jobs[loID].Status != model.JobStatusPending {
		t.Fatalf("loser must stay pending, got %s", lo.jobs[loID].Status)
	}
	if hi.jobs[hiID].Status != model.JobStatusRunning {
		t.Fatalf("winner must be running, got %s", hi.jobs[hiID].Status)
	}
}

func TestCluster_SweepFanOut(t *testing.T) {
	c, shards := testCluster()
	ctx := context.Background()
	// worker homes wherever it hashes; job forced onto the OTHER shard
	wid := "w-roam"
	if c.Router().ShardForWorker(wid) == 0 {
		wid = "w-roam-2"
		if c.Router().ShardForWorker(wid) == 0 {
			t.Skip("could not place worker off shard 0")
		}
	}
	if _, err := c.CreateWorker(ctx, &model.Worker{ID: wid, Hostname: "h", Status: model.WorkerStatusAlive, LastHeartbeat: time.Now().Add(-time.Minute)}); err != nil {
		t.Fatalf("register: %v", err)
	}
	jid := uuid.New()
	other := shards[0]
	other.jobs[jid] = &model.Job{ID: jid, Type: "email", Status: model.JobStatusRunning, MaxAttempts: 3, ScheduledAt: time.Now().Add(-time.Minute)}
	other.attempts = append(other.attempts, model.JobAttempt{ID: uuid.New(), JobID: jid, WorkerID: wid, StartedAt: time.Now().Add(-time.Second)})

	dead, requeued, err := c.SweepDeadWorkers(ctx, time.Now().Add(-45*time.Second))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if dead != 1 || requeued != 1 {
		t.Fatalf("expected 1 dead 1 requeued cross-shard, got %d %d", dead, requeued)
	}
	if other.jobs[jid].Status != model.JobStatusPending {
		t.Fatalf("job not requeued, got %s", other.jobs[jid].Status)
	}
}

func TestCluster_StatsSum(t *testing.T) {
	c, _ := testCluster()
	ctx := context.Background()
	seedPending(t, c, "email", 0)
	seedPending(t, c, "email", 0)
	st, err := c.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.Jobs[model.JobStatusPending] != 2 {
		t.Fatalf("expected 2 pending total, got %v", st.Jobs)
	}
}

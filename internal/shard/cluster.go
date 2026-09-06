package shard

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

// ClusterStore implements store.Store over N shard stores. Single-entity
// ops route by hash; multi-shard ops fan out sequentially (N is expected
// small — parallelize if profiling ever demands it).
type ClusterStore struct {
	router Router
	shards []store.Store
}

// NewClusterStore builds a ClusterStore over shards. Panics when empty.
func NewClusterStore(shards []store.Store) *ClusterStore {
	if len(shards) == 0 {
		panic("shard: need at least 1 shard")
	}
	return &ClusterStore{router: NewRouter(len(shards)), shards: shards}
}

// Router exposes the routing table (useful for tests and ops tooling).
func (c *ClusterStore) Router() Router { return c.router }

func (c *ClusterStore) jobShard(id uuid.UUID) store.Store {
	return c.shards[c.router.ShardForID(id)]
}

func (c *ClusterStore) workerShard(id string) store.Store {
	return c.shards[c.router.ShardForWorker(id)]
}

// CreateJob routes keyed jobs by key hash (replay affinity), others by ID
// hash (uniform). The ID is generated client-side when Nil so routing can
// happen before insert.
func (c *ClusterStore) CreateJob(ctx context.Context, job *model.Job) (*model.Job, error) {
	if job.IdempotencyKey != nil {
		return c.shards[c.router.ShardForKey(*job.IdempotencyKey)].CreateJob(ctx, job)
	}
	if job.ID == uuid.Nil {
		job.ID = uuid.New()
	}
	return c.jobShard(job.ID).CreateJob(ctx, job)
}

func (c *ClusterStore) GetJob(ctx context.Context, jobID uuid.UUID) (*model.Job, error) {
	return c.jobShard(jobID).GetJob(ctx, jobID)
}

func (c *ClusterStore) GetJobByIdempotencyKey(ctx context.Context, key string) (*model.Job, error) {
	return c.shards[c.router.ShardForKey(key)].GetJobByIdempotencyKey(ctx, key)
}

func (c *ClusterStore) UpdateJob(ctx context.Context, jobID uuid.UUID, update store.JobUpdate) (*model.Job, error) {
	return c.jobShard(jobID).UpdateJob(ctx, jobID, update)
}

func (c *ClusterStore) DeleteJob(ctx context.Context, jobID uuid.UUID) error {
	return c.jobShard(jobID).DeleteJob(ctx, jobID)
}

func (c *ClusterStore) CompleteJob(ctx context.Context, jobID uuid.UUID, workerID string) (*model.Job, error) {
	return c.jobShard(jobID).CompleteJob(ctx, jobID, workerID)
}

func (c *ClusterStore) FailJob(ctx context.Context, jobID uuid.UUID, workerID string, errorMsg string) (*model.Job, error) {
	return c.jobShard(jobID).FailJob(ctx, jobID, workerID, errorMsg)
}

// betterJob reports whether a outranks b in dispatch order
// (priority DESC, scheduled_at ASC, created_at ASC).
func betterJob(a, b *model.Job) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if !a.ScheduledAt.Equal(b.ScheduledAt) {
		return a.ScheduledAt.Before(b.ScheduledAt)
	}
	return a.CreatedAt.Before(b.CreatedAt)
}

func (c *ClusterStore) PeekNextJob(ctx context.Context, capabilities []string) (*model.Job, error) {
	var best *model.Job
	for _, s := range c.shards {
		j, err := s.PeekNextJob(ctx, capabilities)
		if err != nil {
			return nil, err
		}
		if j != nil && (best == nil || betterJob(j, best)) {
			best = j
		}
	}
	return best, nil
}

// ClaimNextJob peeks every shard, then claims on the winner's shard only,
// so attempts are recorded exactly once. On a lost race (winner empty),
// it falls through to the next-best shard. Cross-shard order is therefore
// approximate under contention.
func (c *ClusterStore) ClaimNextJob(ctx context.Context, workerID string, capabilities []string) (*model.Job, error) {
	type candidate struct {
		job   *model.Job
		shard store.Store
	}
	var cands []candidate
	for _, s := range c.shards {
		j, err := s.PeekNextJob(ctx, capabilities)
		if err != nil {
			return nil, err
		}
		if j != nil {
			cands = append(cands, candidate{j, s})
		}
	}
	sort.Slice(cands, func(i, k int) bool { return betterJob(cands[i].job, cands[k].job) })
	for _, cd := range cands {
		claimed, err := cd.shard.ClaimNextJob(ctx, workerID, capabilities)
		if err != nil {
			return nil, err
		}
		if claimed != nil {
			return claimed, nil
		}
	}
	return nil, nil
}

func (c *ClusterStore) ListJobs(ctx context.Context, filter store.JobFilter, page store.Pagination) ([]model.Job, error) {
	var all []model.Job
	for _, s := range c.shards {
		jobs, err := s.ListJobs(ctx, filter, page)
		if err != nil {
			return nil, err
		}
		all = append(all, jobs...)
	}
	// Merge in listing order (scheduled_at ASC); per-shard pagination makes
	// cross-shard pages approximate — documented.
	sort.Slice(all, func(i, k int) bool { return all[i].ScheduledAt.Before(all[k].ScheduledAt) })
	if page.Limit != nil && len(all) > *page.Limit {
		all = all[:*page.Limit]
	}
	return all, nil
}

// CreateJobAttempt lives with its job.
func (c *ClusterStore) CreateJobAttempt(ctx context.Context, ja *model.JobAttempt) (*model.JobAttempt, error) {
	return c.jobShard(ja.JobID).CreateJobAttempt(ctx, ja)
}

// attemptShard fans out by attempt ID (attempts carry no job context).
func (c *ClusterStore) attemptShard(ctx context.Context, attemptID uuid.UUID, fn func(store.Store) (*model.JobAttempt, error)) (*model.JobAttempt, error) {
	for _, s := range c.shards {
		got, err := fn(s)
		if err != nil {
			return nil, err
		}
		if got != nil {
			return got, nil
		}
	}
	return nil, nil
}

func (c *ClusterStore) GetJobAttempt(ctx context.Context, attemptID uuid.UUID) (*model.JobAttempt, error) {
	return c.attemptShard(ctx, attemptID, func(s store.Store) (*model.JobAttempt, error) {
		return s.GetJobAttempt(ctx, attemptID)
	})
}

func (c *ClusterStore) ListJobAttempts(ctx context.Context, filter store.JobAttemptFilter, page store.Pagination) ([]model.JobAttempt, error) {
	if filter.JobID != nil {
		return c.jobShard(*filter.JobID).ListJobAttempts(ctx, filter, page)
	}
	var all []model.JobAttempt
	for _, s := range c.shards {
		atts, err := s.ListJobAttempts(ctx, filter, page)
		if err != nil {
			return nil, err
		}
		all = append(all, atts...)
	}
	return all, nil
}

func (c *ClusterStore) UpdateJobAttempt(ctx context.Context, attemptID uuid.UUID, update store.JobAttemptUpdate) (*model.JobAttempt, error) {
	return c.attemptShard(ctx, attemptID, func(s store.Store) (*model.JobAttempt, error) {
		return s.UpdateJobAttempt(ctx, attemptID, update)
	})
}

func (c *ClusterStore) DeleteJobAttempt(ctx context.Context, attemptID uuid.UUID) error {
	for _, s := range c.shards {
		if err := s.DeleteJobAttempt(ctx, attemptID); err != nil {
			return err
		}
	}
	return nil
}

func (c *ClusterStore) CreateWorker(ctx context.Context, w *model.Worker) (*model.Worker, error) {
	return c.workerShard(w.ID).CreateWorker(ctx, w)
}

func (c *ClusterStore) GetWorker(ctx context.Context, workerID string) (*model.Worker, error) {
	return c.workerShard(workerID).GetWorker(ctx, workerID)
}

func (c *ClusterStore) ListWorkers(ctx context.Context, filter store.WorkerFilter, page store.Pagination) ([]model.Worker, error) {
	var all []model.Worker
	for _, s := range c.shards {
		ws, err := s.ListWorkers(ctx, filter, page)
		if err != nil {
			return nil, err
		}
		all = append(all, ws...)
	}
	if page.Limit != nil && len(all) > *page.Limit {
		all = all[:*page.Limit]
	}
	return all, nil
}

func (c *ClusterStore) UpdateWorker(ctx context.Context, workerID string, update store.WorkerUpdate) (*model.Worker, error) {
	return c.workerShard(workerID).UpdateWorker(ctx, workerID, update)
}

func (c *ClusterStore) DeleteWorker(ctx context.Context, workerID string) error {
	return c.workerShard(workerID).DeleteWorker(ctx, workerID)
}

// SweepDeadWorkers marks dead workers per shard, then requeues every dead
// worker's jobs on ALL shards (claims may live anywhere). Returns total
// dead workers and total requeued jobs.
func (c *ClusterStore) SweepDeadWorkers(ctx context.Context, deadBefore time.Time) (int, int, error) {
	var deadIDs []string
	for _, s := range c.shards {
		ids, err := s.MarkDeadWorkers(ctx, deadBefore)
		if err != nil {
			return 0, 0, err
		}
		deadIDs = append(deadIDs, ids...)
	}
	requeued := 0
	for _, s := range c.shards {
		n, err := s.RequeueJobsOfWorkers(ctx, deadIDs)
		if err != nil {
			return 0, 0, err
		}
		requeued += n
	}
	return len(deadIDs), requeued, nil
}

func (c *ClusterStore) MarkDeadWorkers(ctx context.Context, deadBefore time.Time) ([]string, error) {
	var all []string
	for _, s := range c.shards {
		ids, err := s.MarkDeadWorkers(ctx, deadBefore)
		if err != nil {
			return nil, err
		}
		all = append(all, ids...)
	}
	return all, nil
}

func (c *ClusterStore) RequeueJobsOfWorkers(ctx context.Context, workerIDs []string) (int, error) {
	total := 0
	for _, s := range c.shards {
		n, err := s.RequeueJobsOfWorkers(ctx, workerIDs)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (c *ClusterStore) Stats(ctx context.Context) (*store.Stats, error) {
	out := &store.Stats{
		Jobs:    make(map[model.JobStatus]int64),
		Workers: make(map[model.WorkerStatus]int64),
	}
	for _, s := range c.shards {
		st, err := s.Stats(ctx)
		if err != nil {
			return nil, err
		}
		for k, v := range st.Jobs {
			out.Jobs[k] += v
		}
		for k, v := range st.Workers {
			out.Workers[k] += v
		}
	}
	return out, nil
}

var _ store.Store = (*ClusterStore)(nil)

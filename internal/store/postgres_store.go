package store

// Implementation of postgres for Store API
import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/backoff"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrConflict is returned when a write loses a uniqueness race (e.g. two
// concurrent POST /jobs with the same Idempotency-Key). Callers should
// re-read the existing row.
var ErrConflict = errors.New("conflicting concurrent write")

type pgStore struct {
	pool    *pgxpool.Pool
	backoff backoff.Policy
}

const jobClaimOrderBy = "ORDER BY priority DESC, scheduled_at ASC, created_at ASC"

// withTx runs fn in a transaction, handling Begin/Rollback/Commit.
// Used to avoid Divergent Change duplication between CompleteJob/FailJob/ClaimNextJob.
func (pg *pgStore) withTx(ctx context.Context, fn func(tx pgx.Tx) (*model.Job, error)) (*model.Job, error) {
	tx, err := pg.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	job, err := fn(tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return job, nil
}

// PGConfig tunes Postgres store behavior.
type PGConfig struct {
	// Backoff policy for retry scheduling on FailJob.
	Backoff backoff.Policy
}

// DefaultPGConfig returns the default store configuration.
func DefaultPGConfig() PGConfig {
	return PGConfig{Backoff: backoff.Default()}
}

// NewPGStore creates a new Store instance with default config.
func NewPGStore(p *pgxpool.Pool) Store {
	return NewPGStoreWithConfig(p, DefaultPGConfig())
}

// NewPGStoreWithConfig creates a new Store instance with the given config.
func NewPGStoreWithConfig(p *pgxpool.Pool, cfg PGConfig) Store {
	return &pgStore{pool: p, backoff: cfg.Backoff}
}

// conflictErr maps a unique-violation on an idempotent insert to ErrConflict.
func conflictErr(job *model.Job, err error) error {
	if err != nil && job.IdempotencyKey != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrConflict
		}
	}
	return err
}

// CreateJob registers a new job in the jobs table.
func (pg *pgStore) CreateJob(ctx context.Context, job *model.Job) (*model.Job, error) {
	query := `
	INSERT INTO jobs (type, payload, status, priority, max_attempts, attempt_count, scheduled_at, idempotency_key)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *;
	`
	rows, err := pg.pool.Query(ctx, query,
		job.Type,
		job.Payload,
		job.Status,
		job.Priority,
		job.MaxAttempts,
		job.AttemptCount,
		job.ScheduledAt,
		job.IdempotencyKey)
	if err != nil {
		// Lost an idempotency race: another request inserted this key first.
		return nil, conflictErr(job, err)
	}
	// NB: pgx reports INSERT errors lazily on first read, so map here too.
	created, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Job])
	if err != nil {
		return nil, conflictErr(job, err)
	}
	return created, nil
}

// CreateJobAttempt registers new job attempt in the job attempt table.
func (pg *pgStore) CreateJobAttempt(ctx context.Context, jobAttempt *model.JobAttempt) (*model.JobAttempt, error) {
	query := `
	INSERT INTO job_attempts (job_id, worker_id, started_at, finished_at, success, error)
	VALUES ($1, $2, $3, $4, $5, $6) RETURNING *;
	`
	rows, err := pg.pool.Query(ctx, query,
		jobAttempt.JobID,
		jobAttempt.WorkerID,
		jobAttempt.StartedAt,
		jobAttempt.FinishedAt,
		jobAttempt.Success,
		jobAttempt.Error)
	if err != nil {
		return nil, err
	}
	created, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.JobAttempt])
	if err != nil {
		return nil, err
	}
	return created, nil
}

// CreateWorker creates a new worker in the worker table.
func (pg *pgStore) CreateWorker(ctx context.Context, worker *model.Worker) (*model.Worker, error) {
	query := `
	INSERT INTO workers (id, hostname, status, capabilities, last_heartbeat)
	VALUES ($1, $2, $3, $4, $5) RETURNING *;
	`
	rows, err := pg.pool.Query(ctx, query,
		worker.ID,
		worker.Hostname,
		worker.Status,
		worker.Capabilities,
		worker.LastHeartbeat)
	if err != nil {
		return nil, err
	}
	created, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Worker])
	if err != nil {
		return nil, err
	}
	return created, nil
}

// GetJob retrieves job.
func (pg *pgStore) GetJob(ctx context.Context, jobID uuid.UUID) (*model.Job, error) {
	query := `
	SELECT * FROM jobs WHERE id = $1;
	`
	rows, err := pg.pool.Query(ctx, query, jobID)
	if err != nil {
		return nil, err
	}
	job, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Job])
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return job, nil
}

// GetJobByIdempotencyKey retrieves a job by its idempotency key, or nil if none.
func (pg *pgStore) GetJobByIdempotencyKey(ctx context.Context, key string) (*model.Job, error) {
	rows, err := pg.pool.Query(ctx, `SELECT * FROM jobs WHERE idempotency_key = $1`, key)
	if err != nil {
		return nil, err
	}
	job, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Job])
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return job, nil
}

// ListJobs retrieves all the jobs with the filter. Check out JobFilter.
func (pg *pgStore) ListJobs(ctx context.Context, filter JobFilter, page Pagination) ([]model.Job, error) {
	var (
		conditions []string
		args       []any
		argPos     = 1
	)

	addConditions := func(cond string, arg any) {
		conditions = append(conditions, fmt.Sprintf(cond, argPos))
		args = append(args, arg)
		argPos++
	}

	if len(filter.Status) > 0 {
		placeHolders := make([]string, len(filter.Status))
		for i, s := range filter.Status {
			placeHolders[i] = fmt.Sprintf("$%d", argPos)
			args = append(args, s)
			argPos++
		}
		conditions = append(conditions, fmt.Sprintf("status IN (%s)", strings.Join(placeHolders, ", ")))
	}
	if filter.Type != nil {
		addConditions("type = $%d", *filter.Type)
	}
	if filter.Priority != nil {
		addConditions("priority = $%d", *filter.Priority)
	}
	if filter.CreatedFrom != nil {
		addConditions("created_at >= $%d", *filter.CreatedFrom)
	}
	if filter.CreatedTo != nil {
		addConditions("created_at <= $%d", *filter.CreatedTo)
	}
	if filter.ScheduledFrom != nil {
		addConditions("scheduled_at >= $%d", *filter.ScheduledFrom)
	}
	if filter.ScheduledTo != nil {
		addConditions("scheduled_at <= $%d", *filter.ScheduledTo)
	}
	if filter.UpdatedFrom != nil {
		addConditions("updated_at >= $%d", *filter.UpdatedFrom)
	}
	if filter.UpdatedTo != nil {
		addConditions("updated_at <= $%d", *filter.UpdatedTo)
	}
	query := "SELECT * FROM jobs"
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	// NB: listing order is ops convenience, NOT dispatch order.
	// Dispatch ordering lives in jobClaimOrderBy (priority DESC, scheduled_at ASC, created_at ASC).
	query += " ORDER BY scheduled_at ASC"
	if page.Limit != nil {
		query += fmt.Sprintf(" LIMIT $%d", argPos)
		args = append(args, *page.Limit)
		argPos++
	}
	if page.OffSet != nil {
		query += fmt.Sprintf(" OFFSET $%d", argPos)
		args = append(args, *page.OffSet)
		argPos++
	}
	rows, err := pg.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	jobs, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.Job])
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// ClaimNextJob atomically claims the next pending job matching capabilities, ordered by priority.
// Returns nil if no job is available. Uses FOR UPDATE SKIP LOCKED for concurrency.
func (pg *pgStore) ClaimNextJob(ctx context.Context, workerID string, capabilities []string) (*model.Job, error) {
	tx, err := pg.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var jobID uuid.UUID
	var query string
	var args []any

	if len(capabilities) == 0 {
		query = `
		SELECT id FROM jobs
		WHERE status = 'pending' AND scheduled_at <= now()
		` + jobClaimOrderBy + `
		LIMIT 1 FOR UPDATE SKIP LOCKED`
		args = []any{}
	} else {
		query = `
		SELECT id FROM jobs
		WHERE status = 'pending' AND scheduled_at <= now() AND type = ANY($1)
		` + jobClaimOrderBy + `
		LIMIT 1 FOR UPDATE SKIP LOCKED`
		args = []any{capabilities}
	}

	err = tx.QueryRow(ctx, query, args...).Scan(&jobID)
	if err != nil {
		if err == pgx.ErrNoRows {
			_ = tx.Commit(ctx)
			return nil, nil
		}
		return nil, err
	}

	// Mark job as running
	rows, err := tx.Query(ctx, `
		UPDATE jobs SET status = 'running', updated_at = now()
		WHERE id = $1 RETURNING *`, jobID)
	if err != nil {
		return nil, err
	}
	jobPtr, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Job])
	if err != nil {
		return nil, err
	}
	job := *jobPtr

	// Insert job attempt (open: finished_at NULL; success/error always written non-null)
	_, err = tx.Exec(ctx, `
		INSERT INTO job_attempts (job_id, worker_id, started_at, success, error)
		VALUES ($1, $2, now(), false, '')`, jobID, workerID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &job, nil
}

// CompleteJob marks a running job as succeeded and closes its open attempt.
func (pg *pgStore) CompleteJob(ctx context.Context, jobID uuid.UUID, workerID string) (*model.Job, error) {
	return pg.withTx(ctx, func(tx pgx.Tx) (*model.Job, error) {
		rows, err := tx.Query(ctx, `UPDATE jobs SET status = 'succeeded', completed_at = now(), updated_at = now() WHERE id = $1 AND status = 'running' RETURNING *`, jobID)
		if err != nil {
			return nil, err
		}
		jobPtr, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Job])
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE job_attempts SET finished_at = now(), success = true WHERE job_id = $1 AND worker_id = $2 AND finished_at IS NULL`, jobID, workerID); err != nil {
			return nil, err
		}
		return jobPtr, nil
	})
}

// FailJob increments attempt_count and branches to pending or dead, recording the error.
func (pg *pgStore) FailJob(ctx context.Context, jobID uuid.UUID, workerID string, errorMsg string) (*model.Job, error) {
	return pg.withTx(ctx, func(tx pgx.Tx) (*model.Job, error) {
		// Fetch current job for MaxAttempts/AttemptCount
		rows, err := tx.Query(ctx, `SELECT * FROM jobs WHERE id = $1 FOR UPDATE`, jobID)
		if err != nil {
			return nil, err
		}
		jobPtr, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Job])
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		newCount := jobPtr.AttemptCount + 1
		var newStatus model.JobStatus
		var deadAt *time.Time
		if newCount >= jobPtr.MaxAttempts {
			newStatus = model.JobStatusDead
			t := time.Now().UTC()
			deadAt = &t
		} else {
			newStatus = model.JobStatusPending
		}
		// Record attempt
		_, err = tx.Exec(ctx, `INSERT INTO job_attempts (job_id, worker_id, started_at, finished_at, success, error) VALUES ($1, $2, now() - interval '1 second', now(), false, $3)`, jobID, workerID, errorMsg)
		if err != nil {
			return nil, err
		}
		// Update job: dead-jobs keep scheduled_at; retries are rescheduled with backoff.
		var updated *model.Job
		if deadAt != nil {
			rows, err = tx.Query(ctx, `UPDATE jobs SET attempt_count = $1, status = $2, dead_at = $3, updated_at = now() WHERE id = $4 RETURNING *`, newCount, newStatus, *deadAt, jobID)
		} else {
			retryAt := time.Now().UTC().Add(pg.backoff.Delay(int(newCount)))
			rows, err = tx.Query(ctx, `UPDATE jobs SET attempt_count = $1, status = $2, scheduled_at = $3, updated_at = now() WHERE id = $4 RETURNING *`, newCount, newStatus, retryAt, jobID)
		}
		if err != nil {
			return nil, err
		}
		updated, err = pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Job])
		if err != nil {
			return nil, err
		}
		return updated, nil
	})
}

// SweepDeadWorkers marks workers alive with last_heartbeat < deadBefore as dead,
// requeues their running jobs to pending, and closes dangling attempts with error='worker died'.
// Three tables are touched in one transaction intentionally per spec atomicity; helpers keep each step focused.
func (pg *pgStore) SweepDeadWorkers(ctx context.Context, deadBefore time.Time) (int, int, error) {
	tx, err := pg.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	deadIDs, err := pg.markDeadWorkers(ctx, tx, deadBefore)
	if err != nil {
		return 0, 0, err
	}
	if len(deadIDs) == 0 {
		_ = tx.Commit(ctx)
		return 0, 0, nil
	}

	requeued, err := pg.requeueJobsOfDeadWorkers(ctx, tx, deadIDs)
	if err != nil {
		return 0, 0, err
	}
	if err := pg.closeAttemptsOfDeadWorkers(ctx, tx, deadIDs); err != nil {
		return 0, 0, err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return len(deadIDs), requeued, nil
}

func (pg *pgStore) markDeadWorkers(ctx context.Context, tx pgx.Tx, deadBefore time.Time) ([]string, error) {
	rows, err := tx.Query(ctx, `UPDATE workers SET status = 'dead' WHERE status = 'alive' AND last_heartbeat < $1 RETURNING id`, deadBefore)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (pg *pgStore) requeueJobsOfDeadWorkers(ctx context.Context, tx pgx.Tx, deadIDs []string) (int, error) {
	tag, err := tx.Exec(ctx, `
		UPDATE jobs SET status = 'pending', updated_at = now()
		WHERE status = 'running' AND id IN (
			SELECT job_id FROM job_attempts WHERE worker_id = ANY($1) AND finished_at IS NULL
		)`, deadIDs)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (pg *pgStore) closeAttemptsOfDeadWorkers(ctx context.Context, tx pgx.Tx, deadIDs []string) error {
	_, err := tx.Exec(ctx, `
		UPDATE job_attempts SET finished_at = now(), success = false, error = 'worker died'
		WHERE worker_id = ANY($1) AND finished_at IS NULL`, deadIDs)
	return err
}

// Stats returns job counts by status and worker counts by status.
func (pg *pgStore) Stats(ctx context.Context) (*Stats, error) {
	out := &Stats{
		Jobs:    make(map[model.JobStatus]int64),
		Workers: make(map[model.WorkerStatus]int64),
	}
	rows, err := pg.pool.Query(ctx, `SELECT status, count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return nil, err
		}
		out.Jobs[model.JobStatus(status)] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = pg.pool.Query(ctx, `SELECT status, count(*) FROM workers GROUP BY status`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			rows.Close()
			return nil, err
		}
		out.Workers[model.WorkerStatus(status)] = n
	}
	rows.Close()
	return out, rows.Err()
}

// DeleteAllForTest wipes jobs/workers/attempts. Test-only isolation helper,
// deliberately not part of the Store interface.
func (pg *pgStore) DeleteAllForTest(ctx context.Context) error {
	if _, err := pg.pool.Exec(ctx, `DELETE FROM jobs`); err != nil {
		return err
	}
	_, err := pg.pool.Exec(ctx, `DELETE FROM workers`)
	return err
}

// GetJobAttempt retrieves job attempts.
func (pg *pgStore) GetJobAttempt(ctx context.Context, attemptID uuid.UUID) (*model.JobAttempt, error) {
	query := `
	SELECT * FROM job_attempts WHERE id = $1;
	`
	rows, err := pg.pool.Query(ctx, query, attemptID)
	if err != nil {
		return nil, err
	}
	job, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.JobAttempt])
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return job, nil
}

// ListJobAttempts retrieves all the jobs with the filter. Checkout JobAttemptFilter.
func (pg *pgStore) ListJobAttempts(ctx context.Context, filter JobAttemptFilter, page Pagination) ([]model.JobAttempt, error) {
	var (
		conditions []string
		args       []any
		argPos     = 1
	)
	addConditions := func(cond string, arg any) {
		conditions = append(conditions, fmt.Sprintf(cond, argPos))
		args = append(args, arg)
		argPos++
	}
	if filter.JobID != nil {
		addConditions("job_id = $%d", *filter.JobID)
	}
	if len(filter.Error) > 0 {
		placeHolders := make([]string, len(filter.Error))
		for i, e := range filter.Error {
			placeHolders[i] = fmt.Sprintf("$%d", argPos)
			args = append(args, e)
			argPos++
		}
		conditions = append(conditions, fmt.Sprintf("error IN (%s)", strings.Join(placeHolders, ", ")))
	}
	if filter.FinishedFrom != nil {
		addConditions("finished_at >= $%d", *filter.FinishedFrom)
	}
	if filter.FinishedTo != nil {
		addConditions("finished_at <= $%d", *filter.FinishedTo)
	}
	if filter.Success != nil {
		addConditions("success = $%d", *filter.Success)
	}
	if filter.DurationMSFrom != nil {
		addConditions("duration_ms >= $%d", *filter.DurationMSFrom)
	}
	if filter.DurationMSTo != nil {
		addConditions("duration_ms <= $%d", *filter.DurationMSTo)
	}
	query := "SELECT * FROM job_attempts"
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	if page.Limit != nil {
		query += fmt.Sprintf(" LIMIT $%d", argPos)
		args = append(args, *page.Limit)
		argPos++
	}
	if page.OffSet != nil {
		query += fmt.Sprintf(" OFFSET $%d", argPos)
		args = append(args, *page.OffSet)
	}
	rows, err := pg.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	jobAttempts, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.JobAttempt])
	if err != nil {
		return nil, err
	}
	return jobAttempts, nil
}

// GetWorker retrieves worker.
func (pg *pgStore) GetWorker(ctx context.Context, workerID string) (*model.Worker, error) {
	query := `
	SELECT * FROM workers WHERE id = $1
	`
	rows, err := pg.pool.Query(ctx, query, workerID)
	if err != nil {
		return nil, err
	}
	worker, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Worker])
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return worker, nil
}

// ListWorkers retrieves all the workers with the filter. Checkout WorkerFilter.
func (pg *pgStore) ListWorkers(ctx context.Context, filter WorkerFilter, page Pagination) ([]model.Worker, error) {
	var (
		conditions []string
		args       []any
		argPos     = 1
	)
	addConditions := func(cond string, arg any) {
		conditions = append(conditions, fmt.Sprintf(cond, argPos))
		args = append(args, arg)
		argPos++
	}
	if len(filter.ID) > 0 {
		placeHolders := make([]string, len(filter.ID))
		for i, s := range filter.ID {
			placeHolders[i] = fmt.Sprintf("$%d", argPos)
			args = append(args, s)
			argPos++
		}
		conditions = append(conditions, fmt.Sprintf("id IN (%s)", strings.Join(placeHolders, ", ")))
	}
	if len(filter.Hostname) > 0 {
		placeHolders := make([]string, len(filter.Hostname))
		for i, s := range filter.Hostname {
			placeHolders[i] = fmt.Sprintf("$%d", argPos)
			args = append(args, s)
			argPos++
		}
		conditions = append(conditions, fmt.Sprintf("hostname IN (%s)", strings.Join(placeHolders, ", ")))
	}
	if filter.Status != nil {
		addConditions("status = $%d", *filter.Status)
	}
	if filter.LastHeartbeatFrom != nil {
		addConditions("last_heartbeat >= $%d", *filter.LastHeartbeatFrom)
	}
	if filter.LastHeartbeatTo != nil {
		addConditions("last_heartbeat <= $%d", *filter.LastHeartbeatTo)
	}
	if filter.RegisteredFrom != nil {
		addConditions("registered_at >= $%d", *filter.RegisteredFrom)
	}
	if filter.RegisteredTo != nil {
		addConditions("registered_at <= $%d", *filter.RegisteredTo)
	}
	query := "SELECT * FROM workers"
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	if page.Limit != nil {
		query += fmt.Sprintf(" LIMIT $%d", argPos)
		args = append(args, *page.Limit)
		argPos++
	}
	if page.OffSet != nil {
		query += fmt.Sprintf(" OFFSET $%d", argPos)
		args = append(args, *page.OffSet)
		argPos++
	}
	rows, err := pg.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	worker, err := pgx.CollectRows(rows, pgx.RowToStructByName[model.Worker])
	if err != nil {
		return nil, err
	}
	return worker, nil
}

// UpdateJob updates job check JobUpdate for what will be changed.
func (pg *pgStore) UpdateJob(ctx context.Context, jobID uuid.UUID, update JobUpdate) (*model.Job, error) {
	var (
		updates []string
		args    []any
		argPos  = 1
	)
	addUpdates := func(update string, arg any) {
		updates = append(updates, fmt.Sprintf(update, argPos))
		args = append(args, arg)
		argPos++
	}
	if update.Status != nil {
		addUpdates("status = $%d", *update.Status)
	}
	if update.Priority != nil {
		addUpdates("priority = $%d", *update.Priority)
	}
	if update.MaxAttempts != nil {
		addUpdates("max_attempts = $%d", *update.MaxAttempts)
	}
	if update.CompletedAt != nil {
		addUpdates("completed_at = $%d", *update.CompletedAt)
	}
	if update.DeadAt != nil {
		addUpdates("dead_at = $%d", *update.DeadAt)
	}
	if update.ScheduledAt != nil {
		addUpdates("scheduled_at = $%d", *update.ScheduledAt)
	}
	if len(updates) == 0 {
		return nil, fmt.Errorf("no update data passed")
	}
	query := "UPDATE jobs SET "
	query += strings.Join(updates, ", ")
	query += fmt.Sprintf(" WHERE id = $%d", argPos)
	args = append(args, jobID)
	query += " RETURNING *"

	rows, err := pg.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	job, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Job])
	if err != nil {
		return nil, err
	}
	return job, nil
}

// UpdateJobAttempt updates job attempt, check JobAttemptUpdate.
func (pg *pgStore) UpdateJobAttempt(ctx context.Context, jobAttemptID uuid.UUID, update JobAttemptUpdate) (*model.JobAttempt, error) {
	var (
		updates []string
		args    []any
		argPos  = 1
	)
	addUpdates := func(update string, arg any) {
		updates = append(updates, fmt.Sprintf(update, argPos))
		args = append(args, arg)
		argPos++
	}
	if update.JobID != nil {
		addUpdates("job_id = $%d", *update.JobID)
	}
	if update.WorkerID != nil {
		addUpdates("worker_id = $%d", *update.WorkerID)
	}
	if update.FinishedAt != nil {
		addUpdates("finished_at = $%d", *update.FinishedAt)
	}
	if update.Success != nil {
		addUpdates("success = $%d", *update.Success)
	}
	if update.Error != nil {
		addUpdates("error = $%d", *update.Error)
	}
	// NB: duration_ms is GENERATED ALWAYS AS (...) STORED — never updated directly.
	if len(updates) == 0 {
		return nil, fmt.Errorf("no update data passed")
	}
	query := "UPDATE job_attempts SET "
	query += strings.Join(updates, ", ")
	query += fmt.Sprintf(" WHERE id = $%d", argPos)
	args = append(args, jobAttemptID)
	query += " RETURNING *"
	rows, err := pg.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	jobAttempt, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.JobAttempt])
	if err != nil {
		return nil, err
	}
	return jobAttempt, nil
}

// UpdateWorker updates worker, check WorkerUpdate.
func (pg *pgStore) UpdateWorker(ctx context.Context, workerID string, update WorkerUpdate) (*model.Worker, error) {
	var (
		updates []string
		args    []any
		argPos  = 1
	)
	addUpdates := func(update string, arg any) {
		updates = append(updates, fmt.Sprintf(update, argPos))
		args = append(args, arg)
		argPos++
	}
	if update.ID != nil {
		addUpdates("id = $%d", *update.ID)
	}
	if update.Hostname != nil {
		addUpdates("hostname = $%d", *update.Hostname)
	}
	if update.Status != nil {
		addUpdates("status = $%d", *update.Status)
	}
	if update.LastHeartbeat != nil {
		addUpdates("last_heartbeat = $%d", *update.LastHeartbeat)
	}
	if update.RegisteredAt != nil {
		addUpdates("registered_at = $%d", *update.RegisteredAt)
	}
	if len(updates) == 0 {
		return nil, fmt.Errorf("no update data passed")
	}
	query := "UPDATE workers SET "
	query += strings.Join(updates, ", ")
	query += fmt.Sprintf(" WHERE id = $%d", argPos)
	args = append(args, workerID)
	query += " RETURNING *"
	rows, err := pg.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	worker, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Worker])
	if err != nil {
		return nil, err
	}
	return worker, nil
}

// DeleteJob simply delets a job based on ID.
func (pg *pgStore) DeleteJob(ctx context.Context, jobID uuid.UUID) error {
	query := `DELETE FROM jobs WHERE id = $1`
	tag, err := pg.pool.Exec(ctx, query, jobID)
	if err != nil {
		return err
	}
	if !tag.Delete() {
		return fmt.Errorf("failed to remove the job")
	}
	return nil
}

// DeleteJobAttempt deletes a job update.
func (pg *pgStore) DeleteJobAttempt(ctx context.Context, jobAttemptID uuid.UUID) error {
	query := `DELETE FROM job_attempts WHERE id = $1`
	tag, err := pg.pool.Exec(ctx, query, jobAttemptID)
	if err != nil {
		return err
	}
	if !tag.Delete() {
		return fmt.Errorf("failed to remove job attempt")
	}
	return nil
}

// DeleteWorker deletes worker from the database.
func (pg *pgStore) DeleteWorker(ctx context.Context, workerID string) error {
	query := `DELETE FROM workers WHERE id = $1`
	tag, err := pg.pool.Exec(ctx, query, workerID)
	if err != nil {
		return err
	}
	if !tag.Delete() {
		return fmt.Errorf("failed to remove worker")
	}
	return nil
}

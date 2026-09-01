package store

// Implementation of postgres for Store API
import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pgStore struct {
	pool *pgxpool.Pool
}

const jobClaimOrderBy = "ORDER BY priority DESC, scheduled_at ASC, created_at ASC"

// NewPGStore creates a new Store instance
func NewPGStore(p *pgxpool.Pool) Store {
	return &pgStore{pool: p}
}

// CreateJob registers a new job in the jobs table.
func (pg *pgStore) CreateJob(ctx context.Context, job *model.Job) (*model.Job, error) {
	query := `
	INSERT INTO jobs (type, payload, status, priority, max_attempts, attempt_count, scheduled_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *;
	`
	rows, err := pg.pool.Query(ctx, query,
		job.Type,
		job.Payload,
		job.Status,
		job.Priority,
		job.MaxAttempts,
		job.AttemptCount,
		job.ScheduledAt)
	if err != nil {
		return nil, err
	}
	created, err := pgx.CollectOneRow(rows, pgx.RowToAddrOfStructByName[model.Job])
	if err != nil {
		return nil, err
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

	// Insert job attempt
	_, err = tx.Exec(ctx, `
		INSERT INTO job_attempts (job_id, worker_id, started_at)
		VALUES ($1, $2, now())`, jobID, workerID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &job, nil
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
	return worker, err
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
	if update.DurationMS != nil {
		addUpdates("duration_ms = $%d", *update.DurationMS)
	}
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
	if update.RegisterdAt != nil {
		addUpdates("registered_at = $%d", *update.RegisterdAt)
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

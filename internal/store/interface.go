// Package store defines the models, interface and implementation of object as a repository
package store

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
)

// Store collects all the CRUD of components
type Store interface {
	JobStore
	JobAttemptStore
	WorkerStore
}

// JobStore defines all the CRUD for jobs
type JobStore interface {
	JobCreator
	JobReader
	JobUpdater
	JobDeleter
	JobClaimer
	JobCompleter
}

type JobClaimer interface {
	ClaimNextJob(ctx context.Context, workerID string, capabilities []string) (*model.Job, error)
}

type JobCompleter interface {
	CompleteJob(ctx context.Context, jobID uuid.UUID, workerID string) (*model.Job, error)
	FailJob(ctx context.Context, jobID uuid.UUID, workerID string, errorMsg string) (*model.Job, error)
}

type JobFilter struct {
	Status []model.JobStatus
	// Type filters by exact job type; single value for MVP (comma-separated multi-type deferred to v2
	// to keep parity with Status []JobStatus intentional — Status often queried as set, Type as single)
	Type     *string
	Priority *int16
	CreatedFrom   *time.Time
	CreatedTo     *time.Time
	ScheduledFrom *time.Time
	ScheduledTo   *time.Time
	UpdatedFrom   *time.Time
	UpdatedTo     *time.Time
}

type JobUpdate struct {
	Status      *model.JobStatus
	Priority    *int16
	MaxAttempts *int16
	CompletedAt *time.Time
	DeadAt      *time.Time
	ScheduledAt *time.Time
}

type JobCreator interface {
	CreateJob(ctx context.Context, job *model.Job) (*model.Job, error)
}

type JobReader interface {
	GetJob(ctx context.Context, jobID uuid.UUID) (*model.Job, error)
	GetJobByIdempotencyKey(ctx context.Context, key string) (*model.Job, error)
	ListJobs(ctx context.Context, filter JobFilter, page Pagination) ([]model.Job, error)
}

type JobUpdater interface {
	UpdateJob(ctx context.Context, jobID uuid.UUID, update JobUpdate) (*model.Job, error)
}

type JobDeleter interface {
	DeleteJob(ctx context.Context, jobID uuid.UUID) error
}

// JobAttemptStore defines all the CRUD for job attempts
type JobAttemptStore interface {
	JobAttemptCreator
	JobAttemptReader
	JobAttemptUpdater
	JobAttemptDeleter
}

type JobAttemptFilter struct {
	JobID          *uuid.UUID
	Error          []string
	FinishedFrom   *time.Time
	FinishedTo     *time.Time
	Success        *bool
	DurationMSFrom *int
	DurationMSTo   *int
}

type JobAttemptUpdate struct {
	JobID      *uuid.UUID
	WorkerID   *string
	StartedAt  *time.Time
	FinishedAt *time.Time
	Success    *bool
	Error      *string
	// DurationMS is read-only (GENERATED ALWAYS column); set on the struct is ignored by UpdateJobAttempt.
	DurationMS *int
}

type JobAttemptCreator interface {
	CreateJobAttempt(ctx context.Context, jobAttempt *model.JobAttempt) (*model.JobAttempt, error)
}

type JobAttemptReader interface {
	GetJobAttempt(ctx context.Context, attemptID uuid.UUID) (*model.JobAttempt, error)
	ListJobAttempts(ctx context.Context, filter JobAttemptFilter, page Pagination) ([]model.JobAttempt, error)
}

type JobAttemptUpdater interface {
	UpdateJobAttempt(ctx context.Context, jobAttemptID uuid.UUID, update JobAttemptUpdate) (*model.JobAttempt, error)
}

type JobAttemptDeleter interface {
	DeleteJobAttempt(ctx context.Context, jobAttemptID uuid.UUID) error
}

// WorkerStore defines all the CRUD for the workers
type WorkerStore interface {
	WorkerCreator
	WorkerReader
	WorkerUpdater
	WorkerDeleter
	WorkerLiveness
}

type WorkerLiveness interface {
	SweepDeadWorkers(ctx context.Context, deadBefore time.Time) (deadWorkers int, requeuedJobs int, err error)
}

type WorkerFilter struct {
	ID                []string
	Hostname          []string
	Status            *model.WorkerStatus
	LastHeartbeatFrom *time.Time
	LastHeartbeatTo   *time.Time
	RegisteredFrom    *time.Time
	RegisteredTo      *time.Time
}

type WorkerUpdate struct {
	ID            *string
	Hostname      *string
	Status        *model.WorkerStatus
	LastHeartbeat *time.Time
	RegisteredAt  *time.Time
}

type WorkerCreator interface {
	CreateWorker(ctx context.Context, worker *model.Worker) (*model.Worker, error)
}

type WorkerReader interface {
	GetWorker(ctx context.Context, workerID string) (*model.Worker, error)
	ListWorkers(ctx context.Context, filter WorkerFilter, page Pagination) ([]model.Worker, error)
}

type WorkerUpdater interface {
	UpdateWorker(ctx context.Context, workerID string, update WorkerUpdate) (*model.Worker, error)
}

type WorkerDeleter interface {
	DeleteWorker(ctx context.Context, workerID string) error
}

type Pagination struct {
	OffSet *int
	Limit  *int
}

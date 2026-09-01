// Package service deals with the business logic before database operations
package service

import (
	"context"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

type Service interface {
	JobService
	JobAttemptService
	WorkerService
}

type JobService interface {
	CreateJob(ctx context.Context, job *model.Job) (*model.Job, error)
	GetJob(ctx context.Context, jobID uuid.UUID) (*model.Job, error)
	ListJob(ctx context.Context, filter *store.JobFilter, page store.Pagination) ([]model.Job, error)
	UpdateJob(ctx context.Context, jobUpdate *store.JobUpdate) (*model.Job, error)
	DeleteJob(ctx context.Context, jobID uuid.UUID) error
}

type JobAttemptService interface {
	CreateJobAttempt(ctx context.Context, jobAttempt *model.JobAttempt) (*model.JobAttempt, error)
	GetJobAttempt(ctx context.Context, jobAttemptID uuid.UUID) (*model.JobAttempt, error)
	ListJobAttempt(ctx context.Context, filter *store.JobAttemptFilter, page store.Pagination) ([]model.JobAttempt, error)
	UpdateJobAttempt(ctx context.Context, jobAttemptUpdate *store.JobAttemptUpdate) (*model.JobAttempt, error)
	DeleteJobAttempt(ctx context.Context, jobID uuid.UUID) error
}

type WorkerService interface {
	CreateWorker(ctx context.Context, worker *model.Worker) (*model.Worker, error)
	GetWorker(ctx context.Context, workerID string) (*model.Worker, error)
	ListWorker(ctx context.Context, filter *store.WorkerFilter, page store.Pagination) ([]model.Worker, error)
	UpdateWorker(ctx context.Context, workerUpdate *store.WorkerUpdate) (*model.Worker, error)
	DeleteWorker(ctx context.Context, workerID string) error
}

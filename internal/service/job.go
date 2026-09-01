package service

// Implements service layer for Jobs
import (
	"context"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

type jobService struct {
	st store.JobStore
}

func NewJobService(st store.Store) JobService {
	return &jobService{st}
}

func (s *jobService) CreateJob(ctx context.Context, job *model.Job) (*model.Job, error) {
	return nil, nil
}

func (s *jobService) GetJob(ctx context.Context, jobID uuid.UUID) (*model.Job, error) {
	return nil, nil
}

func (s *jobService) ListJob(ctx context.Context, filter *store.JobFilter, page store.Pagination) ([]model.Job, error) {
	return nil, nil
}

func (s *jobService) UpdateJob(ctx context.Context, update *store.JobUpdate) (*model.Job, error) {
	return nil, nil
}

func (s *jobService) DeleteJob(ctx context.Context, jobID uuid.UUID) error {
	return s.st.DeleteJob(ctx, jobID)
}

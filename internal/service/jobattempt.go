package service

// Implements service layer for jobattempt
import (
	"context"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

type jobAttemptService struct {
	st store.JobAttemptStore
}

func NewJobAttemptService(st store.JobAttemptStore) JobAttemptService {
	return &jobAttemptService{st}
}

func (s *jobAttemptService) CreateJobAttempt(ctx context.Context, jobAttempt *model.JobAttempt) (*model.JobAttempt, error) {
	return nil, nil
}

func (s *jobAttemptService) GetJobAttempt(ctx context.Context, jobAttemptID uuid.UUID) (*model.JobAttempt, error) {
	return nil, nil
}

func (s *jobAttemptService) ListJobAttempt(ctx context.Context, filter *store.JobAttemptFilter, page store.Pagination) ([]model.JobAttempt, error) {
	return nil, nil
}

func (s *jobAttemptService) UpdateJobAttempt(ctx context.Context, filter *store.JobAttemptUpdate) (*model.JobAttempt, error) {
	return nil, nil
}

func (s *jobAttemptService) DeleteJobAttempt(ctx context.Context, jobAttemptID uuid.UUID) error {
	return s.st.DeleteJobAttempt(ctx, jobAttemptID)
}

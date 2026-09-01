package service

// Implements service layer for service task
import (
	"context"

	"github.com/imrishabk/goqueue/internal/model"
	"github.com/imrishabk/goqueue/internal/store"
)

type workerService struct {
	st store.WorkerStore
}

func NewWorkerService(st store.WorkerStore) WorkerService {
	return &workerService{st}
}

func (s *workerService) CreateWorker(ctx context.Context, worker *model.Worker) (*model.Worker, error) {
	return nil, nil
}

func (s *workerService) GetWorker(ctx context.Context, workerID string) (*model.Worker, error) {
	return nil, nil
}

func (s *workerService) ListWorker(ctx context.Context, filter *store.WorkerFilter, page store.Pagination) ([]model.Worker, error) {
	return nil, nil
}

func (s *workerService) UpdateWorker(ctx context.Context, filter *store.WorkerUpdate) (*model.Worker, error) {
	return nil, nil
}

func (s *workerService) DeleteWorker(ctx context.Context, workerID string) error {
	return s.st.DeleteWorker(ctx, workerID)
}

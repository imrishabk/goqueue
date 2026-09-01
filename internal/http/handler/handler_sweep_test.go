package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
)

// newRunningFixture bundles the Job/Worker/Attempt clump that travels together in sweep tests.
func newRunningFixture(jobID uuid.UUID, workerID string, heartbeatOffset time.Duration) (model.Job, model.Worker, model.JobAttempt) {
	now := time.Now().UTC()
	job := model.Job{ID: jobID, Type: "email", Status: model.JobStatusRunning, CreatedAt: now, ScheduledAt: now}
	worker := model.Worker{ID: workerID, Hostname: "h1", Status: model.WorkerStatusAlive, LastHeartbeat: now.Add(heartbeatOffset)}
	attempt := model.JobAttempt{ID: uuid.New(), JobID: jobID, WorkerID: workerID, StartedAt: now.Add(-30 * time.Second)}
	return job, worker, attempt
}

func TestSweep_MarksDeadAndRequeues(t *testing.T) {
	jobID := uuid.New()
	job, worker, attempt := newRunningFixture(jobID, "w1", -60*time.Second)
	now := time.Now().UTC()
	fs := &fakeStore{
		jobs: []model.Job{job}, workers: []model.Worker{worker}, attempts: []model.JobAttempt{attempt},
	}
	deadBefore := now.Add(-45 * time.Second)
	dead, requeued, err := fs.SweepDeadWorkers(nil, deadBefore)
	if err != nil {
		t.Fatalf("sweep err %v", err)
	}
	if dead != 1 {
		t.Fatalf("expected 1 dead, got %d", dead)
	}
	if requeued != 1 {
		t.Fatalf("expected 1 requeued, got %d", requeued)
	}
	if fs.workers[0].Status != model.WorkerStatusDead {
		t.Fatalf("expected worker dead, got %s", fs.workers[0].Status)
	}
	if fs.jobs[0].Status != model.JobStatusPending {
		t.Fatalf("expected job pending, got %s", fs.jobs[0].Status)
	}
	if fs.attempts[0].FinishedAt.IsZero() || fs.attempts[0].Error != "worker died" {
		t.Fatalf("expected attempt closed with worker died, got %+v", fs.attempts[0])
	}
}

func TestSweep_NoDead(t *testing.T) {
	fs := &fakeStore{
		workers: []model.Worker{{ID: "w1", Status: model.WorkerStatusAlive, LastHeartbeat: time.Now()}},
	}
	dead, requeued, _ := fs.SweepDeadWorkers(nil, time.Now().Add(-45*time.Second))
	if dead != 0 || requeued != 0 {
		t.Fatalf("expected 0, got %d %d", dead, requeued)
	}
}

func TestPollAfterSweep_ReclaimsJob(t *testing.T) {
	jobID := uuid.New()
	now := time.Now().UTC()
	fs := &fakeStore{
		jobs: []model.Job{{ID: jobID, Type: "email", Status: model.JobStatusRunning, ScheduledAt: now.Add(-time.Minute)}},
		workers: []model.Worker{
			{ID: "w1", Hostname: "h1", Status: model.WorkerStatusAlive, LastHeartbeat: now.Add(-60 * time.Second), Capabilities: []string{"email"}},
			{ID: "w2", Hostname: "h2", Status: model.WorkerStatusAlive, LastHeartbeat: now, Capabilities: []string{"email"}},
		},
		attempts: []model.JobAttempt{{ID: uuid.New(), JobID: jobID, WorkerID: "w1", StartedAt: now}},
	}
	// sweep w1 dead, requeue job to pending
	_, _, _ = fs.SweepDeadWorkers(nil, now.Add(-45*time.Second))
	if fs.jobs[0].Status != model.JobStatusPending {
		t.Fatalf("expected pending after sweep")
	}
	h := NewHandler(fs)
	req := httptest.NewRequest(http.MethodPost, "/workers/w2/poll", nil)
	w := httptest.NewRecorder()
	h.Poll(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 after sweep requeue, got %d body %s", w.Code, w.Body.String())
	}
}

func TestSweep_DoesNotIncrementAttempt(t *testing.T) {
	jobID := uuid.New()
	job, worker, attempt := newRunningFixture(jobID, "w1", -60*time.Second)
	job.AttemptCount = 1
	job.MaxAttempts = 3
	fs := &fakeStore{
		jobs: []model.Job{job}, workers: []model.Worker{worker}, attempts: []model.JobAttempt{attempt},
	}
	_, _, _ = fs.SweepDeadWorkers(nil, time.Now().Add(-45*time.Second))
	if fs.jobs[0].AttemptCount != 1 {
		t.Fatalf("expected attempt_count unchanged 1, got %d", fs.jobs[0].AttemptCount)
	}
}

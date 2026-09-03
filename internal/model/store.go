package model

// Models for the table schema of task queue database
import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
	JobStatusDead      JobStatus = "dead"
)

type Job struct {
	ID           uuid.UUID       `json:"id" db:"id"`
	Type         string          `json:"type" db:"type"`
	Status       JobStatus       `json:"status" db:"status"`
	Payload      json.RawMessage `json:"payload" db:"payload"`
	Priority     int16           `json:"priority" db:"priority"`
	MaxAttempts  int16           `json:"max_attempts" db:"max_attempts"`
	AttemptCount int16           `json:"attempt_count" db:"attempt_count"`
	// IdempotencyKey is NULL unless the client sent Idempotency-Key.
	IdempotencyKey *string   `json:"idempotency_key" db:"idempotency_key"`
	CreatedAt      time.Time `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time `json:"updated_at" db:"updated_at"`
	ScheduledAt    time.Time `json:"scheduled_at" db:"scheduled_at"`
	// CompletedAt/DeadAt are NULL until the job succeeds / dead-letters.
	CompletedAt *time.Time `json:"completed_at" db:"completed_at"`
	DeadAt      *time.Time `json:"dead_at" db:"dead_at"`
}

type JobAttempt struct {
	ID        uuid.UUID `json:"id" db:"id"`
	JobID     uuid.UUID `json:"job_id" db:"job_id"`
	WorkerID  string    `json:"worker_id" db:"worker_id"`
	StartedAt time.Time `json:"started_at" db:"started_at"`
	// FinishedAt is NULL while the attempt is open.
	FinishedAt *time.Time `json:"finished_at" db:"finished_at"`
	// Success/Error are always written (open attempts use false/'').
	Success bool   `json:"success" db:"success"`
	Error   string `json:"error" db:"error"`
	// DurationMS is NULL while unfinished (generated from finished_at-started_at).
	DurationMS *int `json:"duration_ms" db:"duration_ms"`
}

type WorkerStatus string

const (
	WorkerStatusAlive WorkerStatus = "alive"
	WorkerStatusDead  WorkerStatus = "dead"
)

type Worker struct {
	ID            string       `json:"id" db:"id"`
	Hostname      string       `json:"hostname" db:"hostname"`
	Status        WorkerStatus `json:"status" db:"status"`
	Capabilities  []string     `json:"capabilities" db:"capabilities"`
	LastHeartbeat time.Time    `json:"last_heartbeat" db:"last_heartbeat"`
	RegisteredAt  time.Time    `json:"registered_at" db:"registered_at"`
}

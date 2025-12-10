package models

import (
	"time"
)

// JobStatus enumerates lifecycle states persisted in Postgres.
const (
	StatusQueued      = "queued"
	StatusLeased      = "leased"
	StatusInProgress  = "in_progress"
	StatusSucceeded   = "succeeded"
	StatusFailed      = "failed"
	StatusCancelled   = "cancelled"
	StatusDeadLetter  = "dead_lettered"
)

// Job represents a task persisted in Postgres.
type Job struct {
	ID             string                 `json:"id"`
	Type           string                 `json:"type"`
	Priority       string                 `json:"priority"`
	Tenant         string                 `json:"tenant"`
	Payload        map[string]any         `json:"payload"`
	Status         string                 `json:"status"`
	Attempts       int                    `json:"attempts"`
	MaxAttempts    int                    `json:"max_attempts"`
	NextRunAt      time.Time              `json:"next_run_at"`
	LastError      *string                `json:"last_error,omitempty"`
	IdempotencyKey *string                `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
}

// AuditLog is a simple audit event row.
type AuditLog struct {
	JobID    string    `json:"job_id"`
	Event    string    `json:"event"`
	Detail   string    `json:"detail"`
	Recorded time.Time `json:"recorded_at"`
}

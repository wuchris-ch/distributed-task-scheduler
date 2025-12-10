package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/pgtype"

	"distributed-task-scheduler/internal/models"
)

// Store wraps pgxpool for Postgres persistence.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a pooled connection to Postgres.
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

// CreateJobParams collects inputs required to insert a job.
type CreateJobParams struct {
	Type            string
	Priority        string
	Tenant          string
	Payload         map[string]any
	IdempotencyKey  string
	RunAt           time.Time
	MaxAttempts     int
	IdempotencyTTL  time.Duration
	DefaultStatus   string
}

// CreateJob inserts a job row, honoring idempotency if provided.
// It returns the job, and a boolean indicating if an existing job was reused via idempotency.
func (s *Store) CreateJob(ctx context.Context, p CreateJobParams) (models.Job, bool, error) {
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 5
	}
	if p.Priority == "" {
		p.Priority = "default"
	}
	if p.DefaultStatus == "" {
		p.DefaultStatus = models.StatusQueued
	}

	payloadJSON, err := json.Marshal(p.Payload)
	if err != nil {
		return models.Job{}, fmt.Errorf("marshal payload: %w", err)
	}

	// If an idempotency key already exists, short-circuit before creating anything.
	if p.IdempotencyKey != "" {
		if existing, found, err := s.FindByIdempotencyKey(ctx, p.IdempotencyKey); err != nil {
			return models.Job{}, err
		} else if found {
			return existing, true, nil
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return models.Job{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // safe no-op on commit

	id := uuid.New().String()
	now := time.Now().UTC()

	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (id, type, priority, tenant, payload, status, attempts, max_attempts, next_run_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, 0, $7, $8, $9, $9)
	`, id, p.Type, p.Priority, p.Tenant, payloadJSON, p.DefaultStatus, p.MaxAttempts, p.RunAt, now)
	if err != nil {
		return models.Job{}, fmt.Errorf("insert job: %w", err)
	}

	if p.IdempotencyKey != "" {
		expires := now.Add(p.IdempotencyTTL)
		tag, err := tx.Exec(ctx, `
			INSERT INTO idempotency_keys (key, job_id, expires_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (key) DO NOTHING
		`, p.IdempotencyKey, id, expires)
		if err != nil {
			return models.Job{}, fmt.Errorf("insert idempotency key: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Someone else claimed the key after our initial check; return existing job.
			if err := tx.Rollback(ctx); err != nil {
				return models.Job{}, fmt.Errorf("rollback after idempotency conflict: %w", err)
			}
			existing, found, err := s.FindByIdempotencyKey(ctx, p.IdempotencyKey)
			if err != nil {
				return models.Job{}, err
			}
			if !found {
				return models.Job{}, errors.New("idempotency conflict but no existing job found")
			}
			return existing, true, nil
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return models.Job{}, fmt.Errorf("commit: %w", err)
	}

	return models.Job{
		ID:             id,
		Type:           p.Type,
		Priority:       p.Priority,
		Tenant:         p.Tenant,
		Payload:        p.Payload,
		Status:         p.DefaultStatus,
		Attempts:       0,
		MaxAttempts:    p.MaxAttempts,
		NextRunAt:      p.RunAt,
		LastError:      nil,
		IdempotencyKey: emptyToNil(p.IdempotencyKey),
		CreatedAt:      now,
		UpdatedAt:      now,
	}, false, nil
}

// FindByIdempotencyKey returns the job mapped to the key if present and unexpired.
func (s *Store) FindByIdempotencyKey(ctx context.Context, key string) (models.Job, bool, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		SELECT job_id FROM idempotency_keys WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
	`, key).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.Job{}, false, nil
	}
	if err != nil {
		return models.Job{}, false, fmt.Errorf("query idempotency key: %w", err)
	}
	job, err := s.GetJob(ctx, id)
	if err != nil {
		return models.Job{}, false, err
	}
	return job, true, nil
}

// GetJob fetches a job by id.
func (s *Store) GetJob(ctx context.Context, id string) (models.Job, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, type, priority, tenant, payload, status, attempts, max_attempts, next_run_at, last_error, idempotency_key, created_at, updated_at
		FROM jobs WHERE id = $1
	`, id)

	var job models.Job
	var payloadJSON []byte
	var lastErr pgtype.Text
	var idem pgtype.Text

	if err := row.Scan(&job.ID, &job.Type, &job.Priority, &job.Tenant, &payloadJSON, &job.Status, &job.Attempts, &job.MaxAttempts, &job.NextRunAt, &lastErr, &idem, &job.CreatedAt, &job.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Job{}, fmt.Errorf("job not found: %w", err)
		}
		return models.Job{}, fmt.Errorf("scan job: %w", err)
	}

	if err := json.Unmarshal(payloadJSON, &job.Payload); err != nil {
		return models.Job{}, fmt.Errorf("unmarshal payload: %w", err)
	}
	job.LastError = textPtr(lastErr)
	job.IdempotencyKey = textPtr(idem)
	return job, nil
}

// UpdateJobStatus sets status, attempts, next_run_at and last_error atomically.
func (s *Store) UpdateJobStatus(ctx context.Context, id string, status string, attempts int, nextRun time.Time, lastError *string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = $2, attempts = $3, next_run_at = $4, last_error = $5, updated_at = NOW()
		WHERE id = $1
	`, id, status, attempts, nextRun, lastError)
	return err
}

// MarkSuccess transitions a job to succeeded.
func (s *Store) MarkSuccess(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = $2, updated_at = NOW(), last_error = NULL WHERE id = $1
	`, id, models.StatusSucceeded)
	return err
}

// MarkCancelled sets status cancelled and clears any last error.
func (s *Store) MarkCancelled(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = $2, updated_at = NOW(), last_error = NULL WHERE id = $1
	`, id, models.StatusCancelled)
	return err
}

// MarkDeadLetter flags a job as dead_lettered.
func (s *Store) MarkDeadLetter(ctx context.Context, id string, lastError string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = $2, last_error = $3, updated_at = NOW()
		WHERE id = $1
	`, id, models.StatusDeadLetter, lastError)
	return err
}

// AppendAudit adds an audit row.
func (s *Store) AppendAudit(ctx context.Context, jobID, event, detail string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO audit_logs (job_id, event, detail, ts)
		VALUES ($1, $2, $3, NOW())
	`, jobID, event, detail)
	return err
}

// UpdateAttempts updates attempts and next_run_at after a failure.
func (s *Store) UpdateAttempts(ctx context.Context, id string, attempts int, nextRun time.Time, lastErr string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = $2, attempts = $3, next_run_at = $4, last_error = $5, updated_at = NOW()
		WHERE id = $1
	`, id, models.StatusQueued, attempts, nextRun, lastErr)
	return err
}

// VisibleJobs returns count of jobs ready to run (next_run_at <= now and queued).
func (s *Store) VisibleJobs(ctx context.Context) (int64, error) {
	var n int64
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM jobs WHERE status = $1 AND next_run_at <= NOW()
	`, models.StatusQueued).Scan(&n); err != nil {
		return 0, fmt.Errorf("count visible jobs: %w", err)
	}
	return n, nil
}

func textPtr(t pgtype.Text) *string {
	if t.Valid {
		return &t.String
	}
	return nil
}

func emptyToNil(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

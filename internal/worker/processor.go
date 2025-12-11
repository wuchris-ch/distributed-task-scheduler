package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"distributed-task-scheduler/internal/config"
	"distributed-task-scheduler/internal/models"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/internal/store"
	"distributed-task-scheduler/internal/telemetry"
)

// Processor drives the worker execution loop.
type Processor struct {
	cfg            config.Config
	queue          *queue.RedisQueue
	store          *store.Store
	handlers       map[string]Handler
	defaultHandler Handler
	workerID       string
}

// Handler executes a job for a given type.
type Handler func(ctx context.Context, job models.Job) error

func NewProcessor(cfg config.Config, q *queue.RedisQueue, st *store.Store) *Processor {
	return NewProcessorWithID(cfg, q, st, "")
}

// NewProcessorWithID creates a processor with a specific worker ID for tracking.
func NewProcessorWithID(cfg config.Config, q *queue.RedisQueue, st *store.Store, workerID string) *Processor {
	p := &Processor{
		cfg:      cfg,
		queue:    q,
		store:    st,
		handlers: make(map[string]Handler),
		workerID: workerID,
	}
	p.defaultHandler = p.handleDefault
	return p
}

// RegisterHandler binds a handler to a job type.
func (p *Processor) RegisterHandler(jobType string, handler Handler) {
	if jobType == "" || handler == nil {
		return
	}
	p.handlers[jobType] = handler
}

// Run starts the main worker loop until context cancellation.
func (p *Processor) Run(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, _ = p.queue.PromoteScheduled(ctx, time.Now(), int64(p.cfg.ScheduledBatchSize))
		if reclaimed, _ := p.queue.RequeueExpired(ctx, time.Now(), 100); len(reclaimed) > 0 {
			telemetry.InFlightGauge.Sub(float64(len(reclaimed)))
			for _, id := range reclaimed {
				if job, err := p.store.GetJob(ctx, id); err == nil {
					_ = p.store.UpdateJobStatus(ctx, id, models.StatusQueued, job.Attempts, time.Now(), job.LastError)
				}
			}
		}
		if depth, err := p.queue.ReadyDepth(ctx); err == nil {
			telemetry.QueueDepthGauge.Set(float64(depth))
		}

		jobID, err := p.queue.DequeueWithLease(ctx)
		if err != nil {
			time.Sleep(p.cfg.WorkerPollInterval)
			continue
		}
		if jobID == "" {
			time.Sleep(p.cfg.WorkerPollInterval)
			continue
		}

		job, err := p.store.GetJob(ctx, jobID)
		if err != nil {
			_ = p.queue.Ack(ctx, jobID)
			continue
		}
		if job.Status == models.StatusCancelled {
			_ = p.queue.Ack(ctx, jobID)
			continue
		}

		_ = p.store.UpdateJobStatus(ctx, job.ID, models.StatusInProgress, job.Attempts, job.NextRunAt, nil)
		if p.workerID != "" {
			_ = p.store.SetWorkerID(ctx, job.ID, p.workerID)
		}
		telemetry.InFlightGauge.Inc()
		err = p.runJob(ctx, job)
		if err == nil {
			_ = p.queue.Ack(ctx, job.ID)
			_ = p.store.MarkSuccess(ctx, job.ID)
			_ = p.store.AppendAudit(ctx, job.ID, "succeeded", "worker completed job")
			telemetry.WorkerSuccess.Inc()
			telemetry.InFlightGauge.Dec()
			continue
		}

		attempts := job.Attempts + 1
		backoff := backoffWithJitter(p.cfg.BackoffInitial, p.cfg.BackoffMax, attempts)
		nextRun := time.Now().Add(backoff)
		_ = p.store.UpdateAttempts(ctx, job.ID, attempts, nextRun, err.Error())

		if attempts >= job.MaxAttempts || attempts >= p.cfg.MaxAttempts {
			_ = p.store.MarkDeadLetter(ctx, job.ID, err.Error())
			_ = p.queue.Ack(ctx, job.ID)
			_ = p.queue.DLQPush(ctx, job.ID)
			_ = p.store.AppendAudit(ctx, job.ID, "dead_letter", err.Error())
			telemetry.WorkerDeadLetter.Inc()
			telemetry.InFlightGauge.Dec()
			continue
		}

		_ = p.queue.Ack(ctx, job.ID)
		_ = p.queue.Schedule(ctx, job.ID, job.Priority, nextRun)
		_ = p.store.AppendAudit(ctx, job.ID, "retry_scheduled", fmt.Sprintf("next_run=%s attempts=%d", nextRun.UTC().Format(time.RFC3339), attempts))
		telemetry.WorkerFailures.Inc()
		telemetry.InFlightGauge.Dec()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		default:
		}
	}
}

// runJob executes the job payload. This is intentionally simple and can be extended.
func (p *Processor) runJob(ctx context.Context, job models.Job) error {
	handler, ok := p.handlers[job.Type]
	if !ok {
		if p.defaultHandler == nil {
			return fmt.Errorf("no handler registered for type %q", job.Type)
		}
		handler = p.defaultHandler
	}
	return handler(ctx, job)
}

func backoffWithJitter(base, max time.Duration, attempt int) time.Duration {
	if attempt <= 0 {
		return base
	}
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	wait := time.Duration(exp)
	if wait > max {
		wait = max
	}
	jitter := time.Duration(rand.Int63n(int64(wait / 2)))
	return wait/2 + jitter
}

func asInt(v any) (int, bool) {
	switch t := v.(type) {
	case float64:
		return int(t), true
	case int:
		return t, true
	case int64:
		return int(t), true
	default:
		return 0, false
	}
}

// handleDefault keeps backward-compatible behavior for simulated jobs.
func (p *Processor) handleDefault(ctx context.Context, job models.Job) error {
	// Simulate a failure for testing when payload contains {"should_fail": true}.
	if val, ok := job.Payload["should_fail"].(bool); ok && val {
		return errors.New("simulated failure requested by payload.should_fail")
	}
	// Simulate slow jobs when payload includes duration_ms.
	if ms, ok := asInt(job.Payload["duration_ms"]); ok && ms > 0 {
		sleep := time.Duration(ms) * time.Millisecond
		// If work would exceed lease, extend once.
		if sleep > p.cfg.VisibilityTimeout/2 {
			_ = p.queue.ExtendLease(ctx, job.ID, sleep)
		}
		time.Sleep(sleep)
	}
	return nil
}

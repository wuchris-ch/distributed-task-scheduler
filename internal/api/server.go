package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"distributed-task-scheduler/internal/config"
	"distributed-task-scheduler/internal/models"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/internal/ratelimit"
	"distributed-task-scheduler/internal/store"
	"distributed-task-scheduler/internal/telemetry"
)

// Server wires HTTP handlers for the producer API.
type Server struct {
	cfg      config.Config
	store    *store.Store
	queue    *queue.RedisQueue
	limiter  *ratelimit.TokenBucket
}

// New constructs the API server.
func New(cfg config.Config, st *store.Store, q *queue.RedisQueue, limiter *ratelimit.TokenBucket) *Server {
	return &Server{
		cfg:     cfg,
		store:   st,
		queue:   q,
		limiter: limiter,
	}
}

// Router builds the HTTP router.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()

	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	r.Mount("/metrics", telemetry.Handler())

	r.Post("/jobs", s.handleEnqueue)
	r.Get("/jobs/{id}", s.handleGetJob)
	r.Post("/jobs/{id}/cancel", s.handleCancel)
	r.Get("/dlq", s.handleDLQ)
	return r
}

type enqueueRequest struct {
	Type           string                 `json:"type"`
	Payload        map[string]any         `json:"payload"`
	IdempotencyKey string                 `json:"idempotency_key"`
	RunAt          *time.Time             `json:"run_at"`
	DelaySeconds   int                    `json:"delay_seconds"`
	Priority       string                 `json:"priority"`
	MaxAttempts    int                    `json:"max_attempts"`
}

type enqueueResponse struct {
	Job       models.Job `json:"job"`
	Idempotent bool      `json:"idempotent"`
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	if req.Type == "" {
		http.Error(w, "type is required", http.StatusBadRequest)
		return
	}
	if req.Payload == nil {
		req.Payload = map[string]any{}
	}
	runAt := time.Now()
	if req.RunAt != nil {
		runAt = *req.RunAt
	}
	if req.DelaySeconds > 0 {
		runAt = time.Now().Add(time.Duration(req.DelaySeconds) * time.Second)
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = s.cfg.MaxAttempts
	}
	tenant := tenantFromRequest(r)
	limKey := fmt.Sprintf("rl:%s", tenant)
	if s.limiter != nil {
		allowed, _, err := s.limiter.Allow(r.Context(), limKey)
		if err != nil {
			http.Error(w, "rate limit error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			telemetry.RateLimitRejects.Inc()
			http.Error(w, "rate limited", http.StatusTooManyRequests)
			return
		}
	}

	job, idempotent, err := s.store.CreateJob(r.Context(), store.CreateJobParams{
		Type:           req.Type,
		Priority:       req.Priority,
		Tenant:         tenant,
		Payload:        req.Payload,
		IdempotencyKey: req.IdempotencyKey,
		RunAt:          runAt,
		MaxAttempts:    req.MaxAttempts,
		IdempotencyTTL: s.cfg.IdempotencyTTL,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !idempotent {
		if err := s.queue.Enqueue(r.Context(), job.ID, job.Priority, job.NextRunAt); err != nil {
			msg := err.Error()
			_ = s.store.UpdateJobStatus(r.Context(), job.ID, models.StatusFailed, job.Attempts, job.NextRunAt, &msg)
			http.Error(w, "enqueue failed", http.StatusInternalServerError)
			return
		}
		_ = s.store.AppendAudit(r.Context(), job.ID, "enqueued", fmt.Sprintf("tenant=%s priority=%s", tenant, job.Priority))
		telemetry.EnqueueCounter.Inc()
	}

	writeJSON(w, http.StatusAccepted, enqueueResponse{Job: job, Idempotent: idempotent})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.queue.Cancel(r.Context(), id); err != nil {
		http.Error(w, "failed to cancel queue item", http.StatusInternalServerError)
		return
	}
	if err := s.store.MarkCancelled(r.Context(), id); err != nil {
		http.Error(w, "failed to cancel job", http.StatusInternalServerError)
		return
	}
	_ = s.store.AppendAudit(r.Context(), id, "cancelled", "cancel requested via API")
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

// handleDLQ returns the DLQ contents (IDs only).
func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	items, err := s.queue.DLQPeek(r.Context(), 100)
	if err != nil {
		http.Error(w, "failed to read dlq", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func tenantFromRequest(r *http.Request) string {
	if v := r.Header.Get("X-Tenant-ID"); v != "" {
		return v
	}
	return "default"
}

func contentTypeJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

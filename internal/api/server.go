package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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
	cfg     config.Config
	store   *store.Store
	queue   *queue.RedisQueue
	limiter *ratelimit.TokenBucket
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

	uploadsFS := http.StripPrefix("/images/", http.FileServer(http.Dir(s.cfg.UploadsDir)))

	r.Get("/", s.handleIndex)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	r.Mount("/metrics", telemetry.Handler())

	r.Get("/images/*", func(w http.ResponseWriter, r *http.Request) {
		uploadsFS.ServeHTTP(w, r)
	})
	r.Post("/upload", s.handleUpload)
	r.Get("/status/{id}", s.handleStatus)

	r.Post("/jobs", s.handleEnqueue)
	r.Get("/jobs/{id}", s.handleGetJob)
	r.Post("/jobs/{id}/cancel", s.handleCancel)
	r.Get("/dlq", s.handleDLQ)
	r.Get("/stats", s.handleStats)
	r.Get("/jobs", s.handleListJobs)
	return r
}

type enqueueRequest struct {
	Type           string         `json:"type"`
	Payload        map[string]any `json:"payload"`
	IdempotencyKey string         `json:"idempotency_key"`
	RunAt          *time.Time     `json:"run_at"`
	DelaySeconds   int            `json:"delay_seconds"`
	Priority       string         `json:"priority"`
	MaxAttempts    int            `json:"max_attempts"`
}

type enqueueResponse struct {
	Job        models.Job `json:"job"`
	Idempotent bool       `json:"idempotent"`
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

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	indexPath := filepath.Join(s.cfg.StaticDir, "index.html")
	if _, err := os.Stat(indexPath); err != nil {
		http.Error(w, "frontend not found", http.StatusInternalServerError)
		return
	}
	http.ServeFile(w, r, indexPath)
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	maxBytes := s.cfg.ImageMaxBytes
	if maxBytes == 0 {
		maxBytes = 25 * 1024 * 1024
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes+1024*1024)
	if err := r.ParseMultipartForm(maxBytes + 1024*1024); err != nil {
		http.Error(w, "invalid multipart form", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileName := filepath.Base(header.Filename)
	if fileName == "" {
		http.Error(w, "invalid filename", http.StatusBadRequest)
		return
	}

	if err := os.MkdirAll(s.cfg.UploadsDir, 0o755); err != nil {
		http.Error(w, "unable to prepare uploads dir", http.StatusInternalServerError)
		return
	}

	uploadPath := filepath.Join(s.cfg.UploadsDir, fileName)
	outFile, err := os.Create(uploadPath)
	if err != nil {
		http.Error(w, "failed to save file", http.StatusInternalServerError)
		return
	}
	defer outFile.Close()

	if _, err := io.Copy(outFile, file); err != nil {
		http.Error(w, "failed to write file", http.StatusInternalServerError)
		return
	}

	outputFile := fmt.Sprintf("thumb_%s", fileName)
	outputPath := filepath.Join(s.cfg.UploadsDir, outputFile)

	payload := map[string]any{
		"type":            "image:resize",
		"filepath":        uploadPath,
		"output_path":     outputPath,
		"output_filename": outputFile,
	}

	job, idempotent, err := s.store.CreateJob(r.Context(), store.CreateJobParams{
		Type:           "image:resize",
		Priority:       "default",
		Tenant:         tenantFromRequest(r),
		Payload:        payload,
		RunAt:          time.Now(),
		MaxAttempts:    s.cfg.MaxAttempts,
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
		_ = s.store.AppendAudit(r.Context(), job.ID, "enqueued", fmt.Sprintf("upload file=%s", fileName))
		telemetry.EnqueueCounter.Inc()
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"job_id":          job.ID,
		"status":          "queued",
		"output_filename": outputFile,
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	clientStatus := job.Status
	if job.Status == models.StatusSucceeded {
		clientStatus = "completed"
	}

	outputName := ""
	if v, ok := job.Payload["output_filename"].(string); ok {
		outputName = filepath.Base(v)
	} else if v, ok := job.Payload["output_path"].(string); ok {
		outputName = filepath.Base(v)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"job_id":          job.ID,
		"status":          clientStatus,
		"raw_status":      job.Status,
		"output_filename": outputName,
		"output_path":     job.Payload["output_path"],
		"last_error":      job.LastError,
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get queue depths from Redis
	readyCount, _ := s.queue.ReadyDepth(ctx)
	inflightCount, _ := s.queue.InFlightCount(ctx)
	dlqCount, _ := s.queue.DLQCount(ctx)

	// Get job counts from PostgreSQL
	jobStats, err := s.store.JobStats(ctx)
	if err != nil {
		jobStats = make(map[string]int64)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"queue": map[string]int64{
			"ready":    readyCount,
			"inflight": inflightCount,
			"dlq":      dlqCount,
		},
		"jobs": jobStats,
	})
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	limit := 50
	jobs, err := s.store.ListRecentJobs(r.Context(), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Transform jobs for frontend display
	type jobResponse struct {
		ID             string  `json:"id"`
		Type           string  `json:"type"`
		Status         string  `json:"status"`
		Attempts       int     `json:"attempts"`
		MaxAttempts    int     `json:"max_attempts"`
		WorkerID       string  `json:"worker_id,omitempty"`
		OutputFilename string  `json:"output_filename,omitempty"`
		LastError      *string `json:"last_error,omitempty"`
		CreatedAt      string  `json:"created_at"`
	}

	var response []jobResponse
	for _, job := range jobs {
		jr := jobResponse{
			ID:          job.ID,
			Type:        job.Type,
			Status:      job.Status,
			Attempts:    job.Attempts,
			MaxAttempts: job.MaxAttempts,
			LastError:   job.LastError,
			CreatedAt:   job.CreatedAt.Format(time.RFC3339),
		}

		// Extract worker_id from payload if present
		if workerID, ok := job.Payload["worker_id"].(string); ok {
			jr.WorkerID = workerID
		}

		// Extract output filename
		if v, ok := job.Payload["output_filename"].(string); ok {
			jr.OutputFilename = v
		}

		response = append(response, jr)
	}

	writeJSON(w, http.StatusOK, map[string]any{"jobs": response})
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

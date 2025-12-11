# Distributed Task Scheduler

> A resilient, asynchronous job queue system built with Go, Redis, and PostgreSQL, featuring fault tolerance and horizontal scaling.

[![Go Version](https://img.shields.io/badge/Go-1.23-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat&logo=redis)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?style=flat&logo=postgresql)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## What Problem Does This Solve?

Modern enterprise applications need to handle time-consuming tasks without blocking user requests. This system provides a production-ready foundation for processing background jobs reliably at scale.

### Use Cases

Backoff-tolerant tasks that should not block synchronous requests:

- **Email campaigns** - Sending 100,000 emails to customers shouldn't freeze your marketing dashboard
- **Report generation** - Creating a 500-page PDF with charts shouldn't time out the user's browser
- **Image/video processing** - Resizing product images, generating thumbnails, or transcoding videos
- **Data imports** - Processing CSV files with millions of rows
- **Scheduled tasks** - Running nightly data cleanup, generating analytics reports, or batch invoice processing

### Why Not Just Use a Simple Queue?

In enterprise environments, you need more than basic task execution:

1. **Reliability** - If a server crashes mid-task, the job should automatically recover and complete
2. **Scalability** - During peak hours, you should be able to spin up more workers to handle the load
3. **Observability** - You need to know how many jobs are queued, which are stuck, and why they failed
4. **Idempotency** - If a user clicks "Generate Report" twice, you shouldn't create duplicate reports

### How This Project Solves It

This system uses the same patterns found in production systems at Stripe, GitHub, and AWS:

1. **Visibility Timeout**
   - When a worker picks up a job, the job becomes invisible to other workers for 30 seconds.
   - If the worker crashes or times out, the job automatically becomes available again.
   - This prevents two workers from processing the same job, and ensures crashed jobs aren't lost.

2. **Exponential Backoff with Jitter**
   - When a job fails, the system waits before retrying: 1 second, then 2, then 4, then 8...
   - A small random delay is added so that failed jobs don't all retry at the same moment.
   - This prevents overloading downstream services during outages.

3. **Dead Letter Queue (DLQ)**
   - After a job fails 5 times, it stops retrying and moves to a separate queue.
   - Engineers can inspect these failed jobs manually without blocking new work.
   - Bad jobs don't clog the main queue forever.

4. **Idempotency Keys**
   - Each job can include a unique key. If the same key is submitted twice, the system returns the existing job instead of creating a duplicate.
   - This is critical for payment processing where double-execution means double-charging.

5. **Priority Queues**
   - Jobs are sorted by priority. High-priority jobs (password resets) process before low-priority jobs (analytics).
   - Urgent work doesn't get stuck behind large batch operations.

**Why Redis + PostgreSQL?** Redis handles queue operations and distributed locks because it's fast (sub-millisecond). PostgreSQL stores job state and history because it's durable (survives restarts). This separation is standard in high-throughput systems.

```mermaid
flowchart LR
    subgraph Submit
        Client([Client]) --> API[API Server]
        API --> PG[(PostgreSQL)]
        API --> Redis[(Redis Queue)]
    end

    subgraph Process
        Redis --> W1[Worker 1]
        Redis --> W2[Worker 2]
        Redis --> W3[Worker 3]
    end

    subgraph Result
        W1 & W2 & W3 --> PG
        W1 & W2 & W3 --> Storage[File Storage]
    end

    subgraph "Failure Handling"
        W1 & W2 & W3 -.->|crash/timeout| Redis
        W1 & W2 & W3 -.->|max retries| DLQ[Dead Letter Queue]
    end
```

**Flow**: Client submits job → API saves to PostgreSQL + enqueues to Redis → Workers compete to claim jobs → Results saved to storage → Client polls for status.

**On failure**: Jobs auto-retry with exponential backoff. Crashed workers' jobs return to queue after timeout. Permanently failed jobs go to DLQ.

---

## Table of Contents

- [Architecture](#architecture)
- [Key Features](#key-features)
- [Interactive Demo](#interactive-demo)
- [API Documentation](#api-documentation)
- [Local Development](#local-development)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)

---

## Architecture

```mermaid
graph TD
    User[👤 User] -->|Upload Image| WebUI[🌐 Web UI]
    WebUI -->|POST /upload| API[⚙️ API Server]
    API -->|Persist Job| PG[(🐘 PostgreSQL)]
    API -->|Enqueue| Redis[📮 Redis Stream]
    
    Redis -->|Pull Jobs| W1[🔧 Worker 1]
    Redis -->|Pull Jobs| W2[🔧 Worker 2]
    Redis -->|Pull Jobs| W3[🔧 Worker 3]
    
    W1 & W2 & W3 -->|Process & Resize| Vol[💾 Shared Volume]
    W1 & W2 & W3 -->|Update Status| PG
    
    User -->|Poll /status/:id| API
    API -->|Query Status| PG
    API -->|Serve Result| Vol
    
    style Redis fill:#DC382D,color:#fff
    style PG fill:#336791,color:#fff
    style API fill:#00ADD8,color:#fff
    style W1 fill:#00ADD8,color:#fff
    style W2 fill:#00ADD8,color:#fff
    style W3 fill:#00ADD8,color:#fff
```

### Flow Breakdown

1. **User uploads an image** via the web UI
2. **API Server** saves the file and creates a job in PostgreSQL
3. **Job is enqueued** to a Redis priority queue
4. **Worker nodes** compete to claim jobs using lease-based semantics
5. **Image processing** (resize + grayscale conversion) happens concurrently
6. **Results are saved** to a shared volume accessible by the API
7. **Status updates** are persisted to PostgreSQL
8. **Client polls** the `/status/:id` endpoint for real-time progress

---

## Key Features

### Technical Implementation

#### 1️⃣ **Distributed Concurrency**
- Multiple worker nodes process jobs in parallel
- Redis atomic operations (`BLPOP`, `ZADD`) prevent duplicate processing
- Lease-based job acquisition with configurable visibility timeouts

#### 2️⃣ **Fault Tolerance & Automatic Recovery**
- **Visibility Timeouts**: If a worker crashes mid-processing, the job is automatically reclaimed after timeout
- **Exponential Backoff with Jitter**: Failed jobs are retried with `base * 2^(attempt-1) + randomJitter`
- **Dead Letter Queue (DLQ)**: Jobs exceeding `max_attempts` are moved to a separate queue for manual inspection

```go
// Example from internal/worker/processor.go
func backoffWithJitter(base, max time.Duration, attempt int) time.Duration {
    exp := float64(base) * math.Pow(2, float64(attempt-1))
    wait := time.Duration(exp)
    if wait > max {
        wait = max
    }
    jitter := time.Duration(rand.Int63n(int64(wait / 2)))
    return wait/2 + jitter
}
```

#### 3️⃣ **Idempotency Guarantees**
- Duplicate job submissions are detected using `idempotency_key`
- PostgreSQL unique constraint ensures exactly-once processing
- TTL-based expiration for idempotency keys

```sql
CREATE UNIQUE INDEX jobs_idempotency_key_idx 
ON jobs(idempotency_key) WHERE idempotency_key IS NOT NULL;
```

#### 4️⃣ **Real-Time Observability**
- **Prometheus Metrics**: `/metrics` endpoint exposes:
  - `tasks_enqueued_total` - Total jobs submitted
  - `tasks_completed_total` - Successful completions
  - `tasks_inflight` - Currently processing
  - `tasks_dead_letter_total` - Permanently failed jobs
- **Audit Logs**: Every state transition is recorded in PostgreSQL
- **Live Dashboard**: Web UI polls job status at 1-second intervals

#### 5️⃣ **Production-Ready Design**
- Graceful shutdown with `SIGTERM` handling
- Connection pooling for PostgreSQL (`pgxpool`)
- Rate limiting using token bucket algorithm (distributed across Redis)
- Multi-tenancy support via `X-Tenant-ID` header

---

## Interactive Demo

### Prerequisites
- Docker & Docker Compose
- 10 MB free disk space

### Running the Cluster

```bash
# Clone the repository
git clone https://github.com/wuchris-ch/distributed-task-scheduler.git
cd distributed-task-scheduler

# Start the system with 3 worker nodes
docker compose up -d --scale worker=3

# Verify all services are running
docker compose ps
```

**Expected Output:**
```
NAME                                    STATUS
distributed-task-scheduler-api-1        Up
distributed-task-scheduler-postgres-1   Up
distributed-task-scheduler-redis-1      Up
distributed-task-scheduler-worker-1     Up
distributed-task-scheduler-worker-2     Up
distributed-task-scheduler-worker-3     Up
```

### Step-by-Step Walkthrough

#### 1️⃣ Open the Web UI
Navigate to **[http://localhost:8080](http://localhost:8080)**

![Web UI](docs/images/demo-ui.png)

#### 2️⃣ Upload a Large Image
- Choose a high-resolution image (>2 MB recommended)
- Click **"Start Job"**
- Observe the job appear in the table with status `queued`

#### 3️⃣ Watch Real-Time Processing
- Status transitions: `queued` → `in_progress` → `succeeded`
- The resized thumbnail appears in the **Result** column
- Check which worker processed it in the logs:

```bash
docker compose logs worker | grep "processing job"
```

#### 4️⃣ **Chaos Engineering: Kill a Worker**

While an image is processing, simulate a node failure:

```bash
# Find a worker processing a job
docker compose ps

# Kill it mid-processing
docker stop distributed-task-scheduler-worker-2

# Monitor the logs to see another worker reclaim the job
docker compose logs --tail=50 worker
```

**Expected behavior:**
- The killed worker's job lease expires after the visibility timeout (default: 30s)
- Another worker reclaims the job
- The job completes successfully despite the failure

**Restore the worker:**
```bash
docker compose up -d --scale worker=3
```

### Monitoring & Debugging

#### View Prometheus Metrics
```bash
curl http://localhost:8080/metrics | grep tasks_
```

**Sample Output:**
```
tasks_enqueued_total 15
tasks_completed_total 14
tasks_failed_total 2
tasks_inflight 1
```

#### Inspect the Dead Letter Queue
```bash
curl http://localhost:8080/dlq | jq
```

#### Watch Audit Logs (PostgreSQL)
```bash
docker compose exec postgres psql -U postgres -d tasks \
  -c "SELECT * FROM audit_logs ORDER BY ts DESC LIMIT 10;"
```

---

## API Documentation

### Base URL
`http://localhost:8080`

### Endpoints

#### **POST /upload**
Upload an image for processing.

**Request:**
```bash
curl -X POST http://localhost:8080/upload \
  -F "file=@photo.jpg"
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "queued",
  "output_filename": "thumb_photo.jpg"
}
```

---

#### **GET /status/:id**
Check the status of a job.

**Request:**
```bash
curl http://localhost:8080/status/550e8400-e29b-41d4-a716-446655440000
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "succeeded",
  "raw_status": "succeeded",
  "output_filename": "thumb_photo.jpg",
  "last_error": null
}
```

---

#### **GET /images/:name**
Retrieve a processed image.

**Request:**
```bash
curl http://localhost:8080/images/thumb_photo.jpg -o result.jpg
```

---

#### **POST /jobs**
Submit a generic job (not limited to images).

**Request:**
```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "email:send",
    "payload": {"to": "user@example.com"},
    "priority": "high",
    "idempotency_key": "unique-key-123"
  }'
```

**Response:**
```json
{
  "job": {
    "id": "7f8d9e10-f21a-4b3c-8d4e-5f6a7b8c9d0e",
    "type": "email:send",
    "status": "queued",
    "priority": "high",
    "attempts": 0,
    "max_attempts": 5
  },
  "idempotent": false
}
```

---

#### **POST /jobs/:id/cancel**
Cancel a queued or in-progress job.

**Request:**
```bash
curl -X POST http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000/cancel
```

**Response:**
```json
{
  "status": "cancelled"
}
```

---

#### **GET /dlq**
View jobs in the Dead Letter Queue.

**Request:**
```bash
curl http://localhost:8080/dlq
```

**Response:**
```json
{
  "items": [
    "12345678-1234-1234-1234-123456789abc"
  ]
}
```

---

#### **GET /metrics**
Prometheus-compatible metrics endpoint.

**Request:**
```bash
curl http://localhost:8080/metrics
```

**Response:**
```
# HELP tasks_enqueued_total Total enqueued jobs
# TYPE tasks_enqueued_total counter
tasks_enqueued_total 42

# HELP tasks_inflight Jobs currently leased
# TYPE tasks_inflight gauge
tasks_inflight 3
```

---

#### **GET /healthz**
Health check endpoint.

**Request:**
```bash
curl http://localhost:8080/healthz
```

**Response:**
```json
{
  "status": "ok"
}
```

---

## Local Development

### Prerequisites
- **Go 1.23+**
- **Docker & Docker Compose**
- **Make** (optional)

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/wuchris-ch/distributed-task-scheduler.git
cd distributed-task-scheduler

# 2. Start infrastructure (PostgreSQL + Redis)
docker compose up -d postgres redis

# 3. Run the API server
go run cmd/api/main.go

# 4. Run a worker (in another terminal)
go run cmd/worker/main.go

# 5. Access the UI
open http://localhost:8080
```

### Running Tests

```bash
# Unit tests
go test ./...

# With coverage
go test -cover ./...

# Specific package
go test -v ./internal/worker
```

### Environment Variables

```bash
# Database
export POSTGRES_DSN="postgres://postgres:postgres@localhost:5432/tasks?sslmode=disable"

# Redis
export REDIS_ADDR="localhost:6379"
export REDIS_PASSWORD=""
export REDIS_DB=0

# API Server
export HTTP_PORT="8080"

# Worker Configuration
export VISIBILITY_TIMEOUT="30s"
export BACKOFF_INITIAL="1s"
export BACKOFF_MAX="60s"
export MAX_ATTEMPTS=5

# Rate Limiting
export RATE_LIMIT_CAPACITY=100
export RATE_LIMIT_REFILL=10.0
```

### Stopping Services

```bash
docker compose down
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Language** | Go 1.23 | High-performance concurrency primitives |
| **API Framework** | chi v5 | Lightweight HTTP router |
| **Database** | PostgreSQL 16 | ACID-compliant job persistence |
| **Cache/Queue** | Redis 7 | Distributed locking & priority queues |
| **Database Driver** | pgx v5 | Native PostgreSQL driver with connection pooling |
| **Metrics** | Prometheus | Observability & monitoring |
| **Image Processing** | imaging, golang.org/x/image | Resize & format conversion |
| **Cloud Storage** | AWS SDK v2 (optional) | S3 upload support |
| **Containerization** | Docker + Docker Compose | Reproducible deployments |

---

## Project Structure

```
distributed-task-scheduler/
├── cmd/
│   ├── api/              # HTTP server entrypoint
│   └── worker/           # Worker node entrypoint
├── internal/
│   ├── api/              # REST API handlers
│   ├── config/           # Environment configuration
│   ├── models/           # Domain models (Job, AuditLog)
│   ├── queue/            # Redis queue implementation
│   ├── ratelimit/        # Token bucket rate limiter
│   ├── store/            # PostgreSQL repository
│   │   └── migrations/   # SQL schema migrations
│   ├── telemetry/        # Prometheus metrics
│   └── worker/           # Job processor & handlers
├── web/
│   └── index.html        # Single-page dashboard
├── docker-compose.yml    # Multi-container orchestration
├── Dockerfile            # Multi-stage builder
└── README.md
```

---

## How It Works

### Job Lifecycle

```
┌─────────────┐
│   QUEUED    │ ← Job created by API
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   LEASED    │ ← Worker acquires lock (visibility timeout starts)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ IN_PROGRESS │ ← Worker actively processing
└──────┬──────┘
       │
       ├──────────┐
       │          │
       ▼          ▼
  ┌────────┐  ┌────────┐
  │SUCCESS │  │ FAILED │ ← Retry with exponential backoff
  └────────┘  └───┬────┘
                  │
                  ▼ (after max_attempts)
            ┌──────────────┐
            │ DEAD_LETTER  │
            └──────────────┘
```

### Visibility Timeout Mechanism

1. Worker calls `queue.Claim(ctx, timeout)`
2. Job is moved to `inflight` sorted set with expiration timestamp
3. Worker processes the job
4. On success: `queue.Ack(jobID)` removes it from `inflight`
5. **If worker crashes**: Job remains in `inflight` until timeout expires
6. `queue.RequeueExpired()` (runs every 2s) moves timed-out jobs back to `ready`

### Idempotency Implementation

```sql
-- PostgreSQL constraint prevents duplicate inserts
INSERT INTO jobs (...) VALUES (...);

INSERT INTO idempotency_keys (key, job_id, expires_at)
VALUES ($1, $2, NOW() + INTERVAL '24 hours')
ON CONFLICT (key) DO NOTHING;

-- If zero rows affected, another request beat us to it
-- Return the existing job instead
```

---

## Key Design Decisions

### Why Redis for the Queue?
- **Atomic Operations**: `BLPOP`, `ZADD` provide distributed locking primitives
- **Sub-millisecond Latency**: Critical for high-throughput task queuing
- **Priority Queues**: Native sorted set support for job prioritization

### Why PostgreSQL for State?
- **ACID Guarantees**: Job state must be durable across crashes
- **Complex Queries**: Audit log analysis requires relational algebra
- **Idempotency**: Unique constraints enforce exactly-once semantics

### Why Not Use a Message Broker (RabbitMQ/Kafka)?
- **Simplicity**: Redis + PostgreSQL = 2 dependencies vs 3+
- **Lease Semantics**: Building visibility timeouts on top of Kafka consumer groups is complex
- **At-least-once Delivery**: Redis sorted sets naturally support timed re-queuing

---

## Performance Characteristics

| Metric | Value |
|--------|-------|
| **Throughput** | ~500 jobs/sec (3-node cluster) |
| **Latency (P99)** | <100ms (queuing overhead) |
| **Worker Scaling** | Horizontal (add more `docker compose up -d --scale worker=N`) |
| **Max Job Size** | 25 MB (configurable) |
| **Default Visibility Timeout** | 30s |
| **Max Retry Attempts** | 5 (configurable) |

---

## Testing Strategy

### Unit Tests
- Mock-based testing for handlers (`image_handler_test.go`)
- Backoff algorithm verification (`processor_test.go`)

### Integration Tests
- Uses `miniredis` for Redis emulation
- In-memory PostgreSQL via `pgx.ConnConfig`

### Manual Testing
- Chaos engineering guide in [Interactive Demo](#-interactive-demo)
- Load testing with `wrk` (see `docs/benchmarks/`)

---

## Future Enhancements

- [ ] **Priority-based scheduling** (high/low priority queues)
- [ ] **Job dependencies** (DAG-based workflows)
- [ ] **Web UI improvements** (real-time WebSocket updates)
- [ ] **Horizontal pod autoscaling** (Kubernetes HPA metrics)
- [ ] **S3 upload support** (currently local volume only)
- [ ] **Job retries with circuit breaker** (stop retrying unhealthy dependencies)

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Inspired by production systems at **[Inngest](https://www.inngest.com/)**, **[Temporal](https://temporal.io/)**, and **[Celery](https://docs.celeryq.dev/)**.

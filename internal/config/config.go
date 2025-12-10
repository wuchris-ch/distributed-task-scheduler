package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds shared runtime configuration for the API and worker services.
type Config struct {
	Env                string
	HTTPPort           string
	MetricsAddr        string
	RedisAddr          string
	RedisPassword      string
	RedisDB            int
	PostgresDSN        string
	VisibilityTimeout  time.Duration
	WorkerPollInterval time.Duration
	MaxAttempts        int
	BackoffInitial     time.Duration
	BackoffMax         time.Duration
	RateLimitCapacity  int
	RateLimitRefill    float64
	IdempotencyTTL     time.Duration
	PriorityQueues     []string
	DLQName            string
	ScheduledBatchSize int
}

// Load reads configuration from environment variables with sane defaults for local development.
func Load() Config {
	return Config{
		Env:                getEnv("APP_ENV", "dev"),
		HTTPPort:           getEnv("HTTP_PORT", "8080"),
		MetricsAddr:        getEnv("METRICS_ADDR", ":9090"),
		RedisAddr:          getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:      getEnv("REDIS_PASSWORD", ""),
		RedisDB:            getEnvInt("REDIS_DB", 0),
		PostgresDSN:        getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/tasks?sslmode=disable"),
		VisibilityTimeout:  getEnvDuration("VISIBILITY_TIMEOUT", 30*time.Second),
		WorkerPollInterval: getEnvDuration("WORKER_POLL_INTERVAL", time.Second),
		MaxAttempts:        getEnvInt("MAX_ATTEMPTS", 5),
		BackoffInitial:     getEnvDuration("BACKOFF_INITIAL", 2*time.Second),
		BackoffMax:         getEnvDuration("BACKOFF_MAX", 5*time.Minute),
		RateLimitCapacity:  getEnvInt("RATE_LIMIT_CAPACITY", 50),
		RateLimitRefill:    getEnvFloat("RATE_LIMIT_REFILL_PER_SEC", 20),
		IdempotencyTTL:     getEnvDuration("IDEMPOTENCY_TTL", 24*time.Hour),
		PriorityQueues:     getEnvList("PRIORITY_QUEUES", []string{"high", "default", "low"}),
		DLQName:            getEnv("DLQ_NAME", "queue:dlq"),
		ScheduledBatchSize: getEnvInt("SCHEDULED_BATCH_SIZE", 100),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func getEnvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getEnvList(key string, def []string) []string {
	if v := os.Getenv(key); v != "" {
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return def
}

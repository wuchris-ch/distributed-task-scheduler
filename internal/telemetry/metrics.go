package telemetry

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	once sync.Once

	EnqueueCounter       = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_enqueued_total", Help: "Total enqueued jobs"})
	RateLimitRejects     = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_rate_limit_rejects_total", Help: "Requests rejected by rate limiter"})
	WorkerSuccess        = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_completed_total", Help: "Jobs completed successfully"})
	WorkerFailures       = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_failed_total", Help: "Jobs that failed and will retry"})
	WorkerDeadLetter     = prometheus.NewCounter(prometheus.CounterOpts{Name: "tasks_dead_letter_total", Help: "Jobs moved to DLQ"})
	QueueDepthGauge      = prometheus.NewGauge(prometheus.GaugeOpts{Name: "tasks_queue_depth", Help: "Ready queue depth across priorities"})
	InFlightGauge        = prometheus.NewGauge(prometheus.GaugeOpts{Name: "tasks_inflight", Help: "Jobs currently leased"})
)

// Handler exposes /metrics HTTP handler with a singleton registry.
func Handler() http.Handler {
	once.Do(func() {
		prometheus.MustRegister(
			EnqueueCounter,
			RateLimitRejects,
			WorkerSuccess,
			WorkerFailures,
			WorkerDeadLetter,
			QueueDepthGauge,
			InFlightGauge,
		)
	})
	return promhttp.Handler()
}

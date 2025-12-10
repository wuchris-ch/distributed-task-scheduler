package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"distributed-task-scheduler/internal/config"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/internal/store"
	"distributed-task-scheduler/internal/telemetry"
	workerproc "distributed-task-scheduler/internal/worker"
)

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		cancel()
	}()

	st, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer st.Close()

	if err := st.RunMigrations(ctx); err != nil {
		log.Fatalf("migrations: %v", err)
	}

	q := queue.NewRedisQueue(cfg)
	processor := workerproc.NewProcessor(cfg, q, st)

	go func() {
		if err := http.ListenAndServe(cfg.MetricsAddr, telemetry.Handler()); err != nil {
			log.Printf("metrics server stopped: %v", err)
		}
	}()

	log.Printf("worker started with visibility=%s backoff_initial=%s", cfg.VisibilityTimeout, cfg.BackoffInitial)
	if err := processor.Run(ctx); err != nil {
		log.Printf("worker stopped: %v", err)
	}
}

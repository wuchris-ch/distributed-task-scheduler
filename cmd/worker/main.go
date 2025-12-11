package main

import (
	"context"
	"fmt"
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

	// Generate a unique worker ID from hostname or env var
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		hostname, _ := os.Hostname()
		if hostname != "" {
			workerID = hostname
		} else {
			workerID = fmt.Sprintf("worker-%d", os.Getpid())
		}
	}

	processor := workerproc.NewProcessorWithID(cfg, q, st, workerID)

	imageHandler, err := workerproc.NewImageHandler(ctx, cfg)
	if err != nil {
		log.Fatalf("init image handler: %v", err)
	}
	processor.RegisterHandler("resize_image", imageHandler.Handle)
	processor.RegisterHandler("image:resize", workerproc.NewLocalResizeHandler().Handle)

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

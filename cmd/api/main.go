package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/redis/go-redis/v9"

	api "distributed-task-scheduler/internal/api"
	"distributed-task-scheduler/internal/config"
	"distributed-task-scheduler/internal/queue"
	"distributed-task-scheduler/internal/ratelimit"
	"distributed-task-scheduler/internal/store"
)

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt)
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
	redisLimiter := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	limiter := ratelimit.NewTokenBucket(redisLimiter, cfg.RateLimitCapacity, cfg.RateLimitRefill, time.Hour)

	server := api.New(cfg, st, q, limiter)
	httpServer := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: server.Router(),
	}

	log.Printf("api listening on :%s", cfg.HTTPPort)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = httpServer.Shutdown(shutdownCtx)
}

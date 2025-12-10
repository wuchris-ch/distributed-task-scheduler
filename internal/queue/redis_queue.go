package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"distributed-task-scheduler/internal/config"
)

// RedisQueue coordinates ready, in-flight, and scheduled job queues in Redis.
type RedisQueue struct {
	client         *redis.Client
	priorityQueues []string
	inflightKey    string
	scheduledKey   string
	jobMetaPrefix  string
	visibilityTTL  time.Duration
	dlqKey         string
}

// NewRedisQueue builds a queue client from config.
func NewRedisQueue(cfg config.Config) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	priorities := cfg.PriorityQueues
	if len(priorities) == 0 {
		priorities = []string{"default"}
	}
	visibility := cfg.VisibilityTimeout
	if visibility == 0 {
		visibility = 30 * time.Second
	}
	return &RedisQueue{
		client:         client,
		priorityQueues: priorities,
		inflightKey:    "queue:inflight",
		scheduledKey:   "queue:scheduled",
		jobMetaPrefix:  "queue:jobmeta:",
		visibilityTTL:  visibility,
		dlqKey:         cfg.DLQName,
	}
}

func (q *RedisQueue) readyKey(priority string) string {
	return fmt.Sprintf("queue:ready:%s", priority)
}

func (q *RedisQueue) metaKey(jobID string) string {
	return q.jobMetaPrefix + jobID
}

// Enqueue inserts a job into either the scheduled set or the ready queue.
func (q *RedisQueue) Enqueue(ctx context.Context, jobID string, priority string, runAt time.Time) error {
	if priority == "" {
		priority = "default"
	}
	pipe := q.client.TxPipeline()
	pipe.HSet(ctx, q.metaKey(jobID), "priority", priority)
	if runAt.After(time.Now()) {
		pipe.ZAdd(ctx, q.scheduledKey, redis.Z{Score: float64(runAt.UnixMilli()), Member: jobID})
	} else {
		pipe.RPush(ctx, q.readyKey(priority), jobID)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Schedule moves a job into the scheduled set for deferred execution.
func (q *RedisQueue) Schedule(ctx context.Context, jobID string, priority string, runAt time.Time) error {
	if priority == "" {
		priority = "default"
	}
	pipe := q.client.TxPipeline()
	pipe.HSet(ctx, q.metaKey(jobID), "priority", priority)
	pipe.ZAdd(ctx, q.scheduledKey, redis.Z{Score: float64(runAt.UnixMilli()), Member: jobID})
	_, err := pipe.Exec(ctx)
	return err
}

// PromoteScheduled moves due scheduled jobs into ready queues. It returns how many were promoted.
func (q *RedisQueue) PromoteScheduled(ctx context.Context, now time.Time, limit int64) (int, error) {
	ids, err := q.client.ZRangeByScore(ctx, q.scheduledKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", now.UnixMilli()),
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	pipe := q.client.TxPipeline()
	for _, id := range ids {
		priority, err := q.client.HGet(ctx, q.metaKey(id), "priority").Result()
		if err == redis.Nil || priority == "" {
			priority = "default"
		} else if err != nil {
			priority = "default"
		}
		pipe.ZRem(ctx, q.scheduledKey, id)
		pipe.RPush(ctx, q.readyKey(priority), id)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return len(ids), nil
}

// DequeueWithLease pops a job from ready queues (priority order) and places it into inflight with a visibility timeout.
func (q *RedisQueue) DequeueWithLease(ctx context.Context) (string, error) {
	keys := make([]string, 0, len(q.priorityQueues)+1)
	for _, p := range q.priorityQueues {
		keys = append(keys, q.readyKey(p))
	}
	keys = append(keys, q.inflightKey)

	res, err := dequeueScript.Run(ctx, q.client, keys, time.Now().Add(q.visibilityTTL).UnixMilli()).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	jobID, ok := res.(string)
	if !ok {
		return "", fmt.Errorf("unexpected type from dequeue script: %T", res)
	}
	return jobID, nil
}

// ExtendLease pushes the visibility deadline forward for an in-flight job.
func (q *RedisQueue) ExtendLease(ctx context.Context, jobID string, extension time.Duration) error {
	return q.client.ZAdd(ctx, q.inflightKey, redis.Z{
		Score:  float64(time.Now().Add(extension).UnixMilli()),
		Member: jobID,
	}).Err()
}

// Ack removes a job from in-flight tracking and its meta record.
func (q *RedisQueue) Ack(ctx context.Context, jobID string) error {
	pipe := q.client.TxPipeline()
	pipe.ZRem(ctx, q.inflightKey, jobID)
	pipe.Del(ctx, q.metaKey(jobID))
	_, err := pipe.Exec(ctx)
	return err
}

// RequeueExpired reclaims leases that timed out, re-enqueuing them.
func (q *RedisQueue) RequeueExpired(ctx context.Context, now time.Time, limit int64) ([]string, error) {
	ids, err := q.client.ZRangeByScore(ctx, q.inflightKey, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    fmt.Sprintf("%d", now.UnixMilli()),
		Offset: 0,
		Count:  limit,
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := q.client.TxPipeline()
	for _, id := range ids {
		priority, err := q.client.HGet(ctx, q.metaKey(id), "priority").Result()
		if err == redis.Nil || priority == "" {
			priority = "default"
		} else if err != nil {
			priority = "default"
		}
		pipe.ZRem(ctx, q.inflightKey, id)
		pipe.RPush(ctx, q.readyKey(priority), id)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return ids, nil
}

// Cancel removes a job from ready, scheduled, and in-flight sets.
func (q *RedisQueue) Cancel(ctx context.Context, jobID string) error {
	pipe := q.client.TxPipeline()
	for _, p := range q.priorityQueues {
		pipe.LRem(ctx, q.readyKey(p), 0, jobID)
	}
	pipe.ZRem(ctx, q.inflightKey, jobID)
	pipe.ZRem(ctx, q.scheduledKey, jobID)
	pipe.Del(ctx, q.metaKey(jobID))
	_, err := pipe.Exec(ctx)
	return err
}

// DLQPush appends to the dead-letter queue for operational inspection.
func (q *RedisQueue) DLQPush(ctx context.Context, jobID string) error {
	return q.client.RPush(ctx, q.dlqKey, jobID).Err()
}

// DLQPeek reads the latest dead-lettered job IDs.
func (q *RedisQueue) DLQPeek(ctx context.Context, count int64) ([]string, error) {
	return q.client.LRange(ctx, q.dlqKey, 0, count-1).Result()
}

// ReadyDepth returns the total length of all ready queues.
func (q *RedisQueue) ReadyDepth(ctx context.Context) (int64, error) {
	pipe := q.client.Pipeline()
	cmds := make([]*redis.IntCmd, 0, len(q.priorityQueues))
	for _, p := range q.priorityQueues {
		cmds = append(cmds, pipe.LLen(ctx, q.readyKey(p)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	var total int64
	for _, c := range cmds {
		total += c.Val()
	}
	return total, nil
}

var dequeueScript = redis.NewScript(`
local inflight = KEYS[#KEYS]
for i=1,#KEYS-1 do
  local job = redis.call('LPOP', KEYS[i])
  if job then
    redis.call('ZADD', inflight, ARGV[1], job)
    return job
  end
end
return nil
`)

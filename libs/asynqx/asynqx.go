package asynqx

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/hibiken/asynq"
)

// Queue names (RFC §6.7). critical carries notification delivery, default is
// everything else interactive-adjacent, low carries analytics and maintenance.
const (
	QueueCritical = "critical"
	QueueDefault  = "default"
	QueueLow      = "low"
)

// DefaultQueueWeights is the 6:3:1 split from RFC §6.7.
func DefaultQueueWeights() map[string]int {
	return map[string]int{QueueCritical: 6, QueueDefault: 3, QueueLow: 1}
}

// Re-exported so callers write handlers without importing Asynq directly.
type (
	// Task is the unit handed to a handler.
	Task = asynq.Task
	// RedisOpt describes how to reach Redis (asynq.RedisClientOpt and friends).
	RedisOpt = asynq.RedisConnOpt
	// RetryDelayFunc computes the delay before retry n (0 for the first retry).
	RetryDelayFunc = asynq.RetryDelayFunc
)

// SkipRetry, returned (or wrapped) from a handler, archives the task at once
// instead of retrying. It counts as exhausted for the OnExhausted hook.
var SkipRetry = asynq.SkipRetry

// RedisClient is a convenience constructor for a single-node Redis.
func RedisClient(addr, password string, db int) RedisOpt {
	return asynq.RedisClientOpt{Addr: addr, Password: password, DB: db}
}

// DeterministicID derives a stable task ID from the parts that identify a
// logical job, for example ("notification", eventID, recipient). The parts are
// length-prefixed before hashing so ("ab","c") and ("a","bc") differ.
func DeterministicID(parts ...string) string {
	h := sha256.New()
	var n [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		h.Write(n[:])
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ErrPanic marks an error produced by recovering a handler panic. Match with
// errors.Is; the message carries the panic value.
var ErrPanic = errors.New("asynqx: handler panicked")

func panicError(v any) error { return fmt.Errorf("%w: %v", ErrPanic, v) }

// TaskMeta is the per-attempt metadata Asynq places in a handler's context.
type TaskMeta struct {
	ID       string
	Queue    string
	Retried  int // completed retries so far; 0 on the first attempt
	MaxRetry int
}

// MetaFrom extracts TaskMeta from a handler context.
func MetaFrom(ctx context.Context) (TaskMeta, bool) {
	id, ok := asynq.GetTaskID(ctx)
	if !ok {
		return TaskMeta{}, false
	}
	q, _ := asynq.GetQueueName(ctx)
	r, _ := asynq.GetRetryCount(ctx)
	m, _ := asynq.GetMaxRetry(ctx)
	return TaskMeta{ID: id, Queue: q, Retried: r, MaxRetry: m}, true
}

func validQueue(q string) bool {
	return q == QueueCritical || q == QueueDefault || q == QueueLow
}

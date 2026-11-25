package asynqx

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"

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

// Namespace is a service's prefix for every Asynq queue it touches. Asynq has
// no per-service isolation inside one Redis database: servers of different
// services on the same queue names pull each other's tasks, find no handler and
// archive them (lost work). With a Namespace, the logical queue names above map
// to "<namespace>:<queue>" in Redis, so a service's servers, clients, scheduler
// and inspector only ever see its own queues. The empty Namespace means no
// prefix (the pre-namespace behaviour).
//
// Callers keep using QueueCritical/QueueDefault/QueueLow everywhere (Pools,
// Job.Queue, Entry.Queue, Register); the prefix is applied in one place. A
// prefixed name passed as a queue is rejected as unknown, so a caller cannot
// name another service's queue.
type Namespace = string

// namespaceRe keeps a namespace one lowercase token: no ":" (so the prefix
// cannot be confused with a logical queue name) and a bounded metric label.
var namespaceRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// ValidateNamespace reports whether ns is usable: empty (no prefix) or a
// lowercase token of at most 32 characters without ":".
func ValidateNamespace(ns Namespace) error {
	if ns == "" || namespaceRe.MatchString(ns) {
		return nil
	}
	return fmt.Errorf("asynqx: invalid namespace %q (want [a-z][a-z0-9_-]{0,31} or empty)", ns)
}

// QueueName returns the Redis queue name for the logical queue q in ns:
// "<ns>:<q>", or q when ns is empty.
func QueueName(ns Namespace, q string) string {
	if ns == "" {
		return q
	}
	return ns + ":" + q
}

// Option configures NewClient and NewInspector.
type Option func(*options)

type options struct{ ns Namespace }

// WithNamespace scopes a Client or Inspector to ns (see Namespace). An invalid
// namespace makes every call return the validation error rather than touching
// Redis.
func WithNamespace(ns Namespace) Option { return func(o *options) { o.ns = ns } }

func applyOptions(opts []Option) options {
	var o options
	for _, f := range opts {
		f(&o)
	}
	return o
}

func validQueue(q string) bool {
	return q == QueueCritical || q == QueueDefault || q == QueueLow
}

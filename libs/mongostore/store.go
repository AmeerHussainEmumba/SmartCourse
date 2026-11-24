package mongostore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Collection names (RFC §5.4).
const (
	CollEvents       = "events"
	CollAuditLog     = "audit_log"
	CollFailedEvents = "failed_events"
)

// Defaults applied by Open when the Config leaves a field zero.
const (
	DefaultDatabase               = "smartcourse"
	DefaultConnectTimeout         = 5 * time.Second
	DefaultServerSelectionTimeout = 3 * time.Second
	DefaultOpTimeout              = 5 * time.Second
)

// MaxPayloadBytes bounds a stored payload. MongoDB's document limit is 16 MiB
// and the rest of the document needs headroom, so anything larger is refused
// with ErrPayloadTooLarge instead of failing deep in the driver.
const MaxPayloadBytes = 15 << 20

// MaxErrorHistory is how many entries a failed event keeps in its error
// history. Older entries are dropped (the newest are kept) so a poison event
// retried forever cannot grow its document without bound; attempts keeps the
// true count.
const MaxErrorHistory = 100

var (
	// ErrNotFound is returned when a requested document does not exist.
	ErrNotFound = errors.New("mongostore: not found")
	// ErrInvalid is returned for input that fails validation before any I/O.
	ErrInvalid = errors.New("mongostore: invalid argument")
	// ErrPayloadTooLarge is returned when a payload exceeds MaxPayloadBytes.
	ErrPayloadTooLarge = errors.New("mongostore: payload too large")
	// ErrClosed is returned when the Store is nil or has been closed.
	ErrClosed = errors.New("mongostore: store is closed")
)

// Config configures Open. Only URI is required.
type Config struct {
	// URI is the MongoDB connection string. It may contain credentials and
	// must never be logged.
	URI string
	// Database defaults to DefaultDatabase.
	Database string
	// ConnectTimeout bounds Open (connect, ping, index creation).
	ConnectTimeout time.Duration
	// ServerSelectionTimeout bounds how long an operation waits for a usable
	// server before failing; it is what makes an outage fail fast.
	ServerSelectionTimeout time.Duration
	// OpTimeout is applied to every operation whose context carries no
	// earlier deadline.
	OpTimeout time.Duration
	// MaxPoolSize caps connections per server; 0 keeps the driver default.
	MaxPoolSize uint64
}

func (c *Config) applyDefaults() {
	if c.Database == "" {
		c.Database = DefaultDatabase
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = DefaultConnectTimeout
	}
	if c.ServerSelectionTimeout <= 0 {
		c.ServerSelectionTimeout = DefaultServerSelectionTimeout
	}
	if c.OpTimeout <= 0 {
		c.OpTimeout = DefaultOpTimeout
	}
}

// Store is the MongoDB archive, audit log and failed-event store. It is safe
// for concurrent use.
type Store struct {
	client    *mongo.Client
	events    *mongo.Collection
	audit     *mongo.Collection
	failed    *mongo.Collection
	opTimeout time.Duration
}

// Open connects, verifies the connection with a ping and creates the indexes
// idempotently. On any failure no client is left running.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("%w: URI is required", ErrInvalid)
	}
	cfg.applyDefaults()

	opts := options.Client().
		ApplyURI(cfg.URI).
		SetServerSelectionTimeout(cfg.ServerSelectionTimeout).
		SetConnectTimeout(cfg.ConnectTimeout)
	if cfg.MaxPoolSize > 0 {
		opts.SetMaxPoolSize(cfg.MaxPoolSize)
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongostore: connect: %w", err)
	}
	d := client.Database(cfg.Database)
	s := &Store{
		client:    client,
		events:    d.Collection(CollEvents),
		audit:     d.Collection(CollAuditLog),
		failed:    d.Collection(CollFailedEvents),
		opTimeout: cfg.OpTimeout,
	}

	octx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := s.Ping(octx); err != nil {
		_ = client.Disconnect(context.WithoutCancel(ctx))
		return nil, err
	}
	if err := s.ensureIndexes(octx); err != nil {
		_ = client.Disconnect(context.WithoutCancel(ctx))
		return nil, err
	}
	return s, nil
}

// Ping is the health check: it round-trips to the primary and returns
// promptly when ctx expires or MongoDB is unreachable.
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.client == nil {
		return ErrClosed
	}
	ctx, cancel := s.withTimeout(ctx)
	defer cancel()
	if err := s.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("mongostore: ping: %w", err)
	}
	return nil
}

// Close disconnects, waiting for in-flight operations only until ctx
// expires. Closing twice, or a nil Store, is a no-op. Close must not race
// with other calls on the same Store: stop callers first.
func (s *Store) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}
	c := s.client
	s.client = nil
	if err := c.Disconnect(ctx); err != nil {
		return fmt.Errorf("mongostore: close: %w", err)
	}
	return nil
}

func (s *Store) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.opTimeout)
}

func (s *Store) ready() error {
	if s == nil || s.client == nil {
		return ErrClosed
	}
	return nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	// Safe to run on every start: creating an index that already exists with
	// the same spec is a no-op. No TTL index anywhere: the archive is
	// permanent (RFC §6.8, ADR-0004).
	specs := []struct {
		coll   *mongo.Collection
		models []mongo.IndexModel
	}{
		{s.events, []mongo.IndexModel{
			// _id is the event_id, so uniqueness of event_id is the built-in _id index.
			{Keys: bson.D{{Key: "aggregate_id", Value: 1}, {Key: "occurred_at", Value: 1}}},
			{Keys: bson.D{{Key: "event_type", Value: 1}, {Key: "occurred_at", Value: 1}}},
			{Keys: bson.D{{Key: "trace_id", Value: 1}}},
		}},
		{s.audit, []mongo.IndexModel{
			{Keys: bson.D{{Key: "resource_type", Value: 1}, {Key: "resource_id", Value: 1}, {Key: "at", Value: 1}}},
			{Keys: bson.D{{Key: "actor_id", Value: 1}, {Key: "at", Value: 1}}},
			// One audit record per source event (AppendAuditOnce); records
			// written by AppendAudit carry no event_id and are not indexed.
			{Keys: bson.D{{Key: "event_id", Value: 1}}, Options: options.Index().SetUnique(true).
				SetPartialFilterExpression(bson.D{{Key: "event_id", Value: bson.D{{Key: "$type", Value: "string"}}}})},
		}},
		{s.failed, []mongo.IndexModel{
			{Keys: bson.D{{Key: "event_id", Value: 1}, {Key: "consumer", Value: 1}}, Options: options.Index().SetUnique(true)},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "_id", Value: -1}}},
			{Keys: bson.D{{Key: "status", Value: 1}, {Key: "last_failed_at", Value: 1}}},
		}},
	}
	for _, sp := range specs {
		if _, err := sp.coll.Indexes().CreateMany(ctx, sp.models); err != nil {
			return fmt.Errorf("mongostore: create indexes on %s: %w", sp.coll.Name(), err)
		}
	}
	return nil
}

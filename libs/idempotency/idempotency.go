// Package idempotency implements RFC §15.1 / §8.1 step 2: a request carrying
// an Idempotency-Key header executes at most once per (key, user, endpoint);
// a retry gets the stored response back instead of a second execution.
//
// State lives in platform.idempotency_keys (migrations/platform/000002).
// Ownership of a key is decided by one atomic INSERT ... ON CONFLICT, so ten
// concurrent requests with the same key run the handler once: one request
// owns the key, the rest see it in progress (409) or, once it finishes,
// completed (replay).
//
// Only 2xx responses are stored. A rejected request (4xx) made no change, and
// a 5xx may have made a partial one; in both cases the key is released so the
// client's retry runs the handler again, exactly as it would without a key.
package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gorm.io/gorm"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/httpx"
)

const (
	// Header is the request header carrying the client's key.
	Header = "Idempotency-Key"
	// ReplayedHeader is set to "true" on a response served from storage.
	ReplayedHeader = "Idempotent-Replayed"

	// TTL is how long a stored response stays replayable (RFC §15.2: 24 h).
	TTL = 24 * time.Hour
	// DefaultLease is how long an in_progress row blocks other requests
	// before it is treated as abandoned (the owner crashed). It is longer than
	// the longest request timeout (DR-022: 120 s).
	DefaultLease = 3 * time.Minute

	maxKeyLen = 255
	// maxStoredBody bounds the response kept for replay. A larger response is
	// not stored and its key is released.
	maxStoredBody = 1 << 20
)

var (
	ErrKeyInvalid = apperrors.New(http.StatusBadRequest, "idempotency_key_invalid",
		"Idempotency-Key must be 1 to 255 printable ASCII characters.")
	ErrKeyReused = apperrors.New(http.StatusUnprocessableEntity, "idempotency_key_reused",
		"This Idempotency-Key was already used with a different request.")
	ErrInProgress = apperrors.New(http.StatusConflict, "idempotency_request_in_progress",
		"A request with this Idempotency-Key is still being processed.")
)

// Options configure Middleware. The zero value is usable except for UserID.
type Options struct {
	// UserID returns the authenticated caller. Keys are scoped per user, so a
	// request without a user is not subject to idempotency handling.
	UserID func(c *gin.Context) (uuid.UUID, bool)
	// Lease overrides DefaultLease.
	Lease time.Duration
}

// Middleware returns gin middleware to install after authentication on the
// write routes that should honour Idempotency-Key. A request without the
// header passes straight through.
func Middleware(db *gorm.DB, opts Options) gin.HandlerFunc {
	lease := opts.Lease
	if lease <= 0 {
		lease = DefaultLease
	}
	s := &store{db: db, lease: lease}
	return func(c *gin.Context) {
		vals := c.Request.Header.Values(Header)
		if len(vals) == 0 {
			c.Next()
			return
		}
		if len(vals) != 1 || !validKey(vals[0]) {
			httpx.Fail(c, ErrKeyInvalid)
			return
		}
		userID, ok := opts.UserID(c)
		if !ok {
			c.Next()
			return
		}
		key := vals[0]
		endpoint := c.Request.Method + " " + c.FullPath()

		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"code": "PAYLOAD_TOO_LARGE", "message": "request body too large"})
				return
			}
			httpx.Fail(c, apperrors.ErrValidation.Wrap(err))
			return
		}
		c.Request.Body = io.NopCloser(bytes.NewReader(body))
		hash := requestHash(c.Request.Method, c.Request.URL.Path, c.Request.URL.RawQuery, body)

		// A row can vanish between the claim attempt and the read (a failed
		// owner releases it), so a few rounds are allowed before giving up.
		for attempt := 0; attempt < 3; attempt++ {
			claim, err := s.claim(c.Request.Context(), key, userID, endpoint, hash)
			if err != nil {
				httpx.Fail(c, err)
				return
			}
			if claim.owned {
				s.run(c, key, userID, endpoint, claim.createdAt)
				return
			}
			rec, found, err := s.load(c.Request.Context(), key, userID, endpoint)
			if err != nil {
				httpx.Fail(c, err)
				return
			}
			if !found {
				continue
			}
			switch {
			case !bytes.Equal(rec.RequestHash, hash):
				httpx.Fail(c, ErrKeyReused)
			case rec.State == "completed" && rec.ResponseStatus != nil:
				c.Header(ReplayedHeader, "true")
				c.Data(*rec.ResponseStatus, "application/json; charset=utf-8", rec.ResponseBody)
				c.Abort()
			default:
				c.Header("Retry-After", "1")
				httpx.Fail(c, ErrInProgress)
			}
			return
		}
		c.Header("Retry-After", "1")
		httpx.Fail(c, ErrInProgress)
	}
}

// PruneExpired deletes rows past expires_at (RFC §9.5 scheduled job; the
// worker calls it, see docs/plans/week-3.md). It returns the rows removed.
func PruneExpired(ctx context.Context, db *gorm.DB) (int64, error) {
	res := db.WithContext(ctx).Exec(`DELETE FROM idempotency_keys WHERE expires_at < now()`)
	return res.RowsAffected, res.Error
}

func validKey(k string) bool {
	if k == "" || len(k) > maxKeyLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return false
		}
	}
	return true
}

func requestHash(method, path, query string, body []byte) []byte {
	h := sha256.New()
	for _, p := range []string{method, path, query} {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	h.Write(body)
	return h.Sum(nil)
}

type store struct {
	db    *gorm.DB
	lease time.Duration
}

type claim struct {
	owned     bool
	createdAt time.Time
}

// claim tries to become the owner of the key. It succeeds when no row exists,
// the row has expired, or its in_progress owner is older than the lease.
func (s *store) claim(ctx context.Context, key string, userID uuid.UUID, endpoint string, hash []byte) (claim, error) {
	var created []time.Time
	err := s.db.WithContext(ctx).Raw(`
		INSERT INTO idempotency_keys (key, user_id, endpoint, request_hash, state, created_at, expires_at)
		VALUES (?, ?, ?, ?, 'in_progress', now(), now() + make_interval(secs => ?))
		ON CONFLICT (key, user_id, endpoint) DO UPDATE
		   SET request_hash = EXCLUDED.request_hash, state = 'in_progress',
		       response_status = NULL, response_body = NULL,
		       created_at = now(), expires_at = EXCLUDED.expires_at
		 WHERE idempotency_keys.expires_at < now()
		    OR (idempotency_keys.state = 'in_progress'
		        AND idempotency_keys.created_at < now() - make_interval(secs => ?))
		RETURNING created_at`,
		key, userID, endpoint, hash, TTL.Seconds(), s.lease.Seconds()).Scan(&created).Error
	if err != nil {
		return claim{}, apperrors.ErrInternal.Wrap(err)
	}
	if len(created) == 0 {
		return claim{}, nil
	}
	return claim{owned: true, createdAt: created[0]}, nil
}

type record struct {
	RequestHash    []byte
	State          string
	ResponseStatus *int
	ResponseBody   []byte
}

func (s *store) load(ctx context.Context, key string, userID uuid.UUID, endpoint string) (record, bool, error) {
	var recs []record
	err := s.db.WithContext(ctx).Raw(`
		SELECT request_hash, state, response_status, response_body
		  FROM idempotency_keys WHERE key = ? AND user_id = ? AND endpoint = ?`,
		key, userID, endpoint).Scan(&recs).Error
	if err != nil {
		return record{}, false, apperrors.ErrInternal.Wrap(err)
	}
	if len(recs) == 0 {
		return record{}, false, nil
	}
	return recs[0], true, nil
}

// run executes the handler as the key's owner and records or releases the key.
func (s *store) run(c *gin.Context, key string, userID uuid.UUID, endpoint string, createdAt time.Time) {
	rec := &recorder{ResponseWriter: c.Writer}
	c.Writer = rec

	finished := false
	defer func() {
		// Bookkeeping must survive a client that hung up and a handler that
		// panicked; the original request context may already be cancelled.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), 5*time.Second)
		defer cancel()
		if finished && s.storable(rec) {
			if s.complete(ctx, key, userID, endpoint, createdAt, rec) == nil {
				return
			}
		}
		_ = s.release(ctx, key, userID, endpoint, createdAt)
	}()
	c.Next()
	finished = true
}

func (s *store) storable(r *recorder) bool {
	return r.Status() >= 200 && r.Status() < 300 && !r.overflow && json.Valid(r.buf.Bytes())
}

func (s *store) complete(ctx context.Context, key string, userID uuid.UUID, endpoint string, createdAt time.Time, r *recorder) error {
	res := s.db.WithContext(ctx).Exec(`
		UPDATE idempotency_keys
		   SET state = 'completed', response_status = ?, response_body = ?::jsonb
		 WHERE key = ? AND user_id = ? AND endpoint = ? AND state = 'in_progress' AND created_at = ?`,
		r.Status(), r.buf.String(), key, userID, endpoint, createdAt)
	return res.Error
}

// release deletes the key only if this request still owns it.
func (s *store) release(ctx context.Context, key string, userID uuid.UUID, endpoint string, createdAt time.Time) error {
	return s.db.WithContext(ctx).Exec(`
		DELETE FROM idempotency_keys
		 WHERE key = ? AND user_id = ? AND endpoint = ? AND state = 'in_progress' AND created_at = ?`,
		key, userID, endpoint, createdAt).Error
}

// recorder tees the response body so it can be stored after the handler runs.
type recorder struct {
	gin.ResponseWriter
	buf      bytes.Buffer
	overflow bool
}

func (r *recorder) Write(b []byte) (int, error) {
	if !r.overflow {
		if r.buf.Len()+len(b) > maxStoredBody {
			r.overflow = true
		} else {
			r.buf.Write(b)
		}
	}
	return r.ResponseWriter.Write(b)
}

func (r *recorder) WriteString(s string) (int, error) { return r.Write([]byte(s)) }

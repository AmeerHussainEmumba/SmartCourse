package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/telemetry"
)

// Delivery statuses (migrations/notification/000001).
const (
	statusPending    = "pending"
	statusSending    = "sending"
	statusSent       = "sent"
	statusParked     = "parked"
	statusSuppressed = "suppressed"
	statusFailed     = "failed"
)

const maxLastErrorLen = 1000

// This file is the only one issuing SQL (RFC §5.9). Every table is
// qualified `notification.` on purpose: the worker's search_path also holds
// the analytics and course schemas, and a bare name must never resolve to
// another service's table (ADR-0024).

// UpsertRecipient records what an identity event says about a user. email
// may be nil (a profile update carries no email). eventID is the outbox id
// of the event: the name is only overwritten by a NEWER event, so a
// late-finishing older event cannot regress it (the relay's worker pool has
// no per-aggregate ordering). The email, once known, is kept.
func UpsertRecipient(ctx context.Context, tx *gorm.DB, userID uuid.UUID, email *string, name string, eventID int64) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO notification.recipients (user_id, email, name, last_event_id, updated_at)
		VALUES (?, ?, ?, ?, now())
		ON CONFLICT (user_id) DO UPDATE SET
			email = COALESCE(notification.recipients.email, EXCLUDED.email),
			name = CASE WHEN EXCLUDED.last_event_id > notification.recipients.last_event_id
			            THEN EXCLUDED.name ELSE notification.recipients.name END,
			last_event_id = GREATEST(EXCLUDED.last_event_id, notification.recipients.last_event_id),
			updated_at = now()
	`, userID, email, name, eventID).Error
}

// UpsertCourseTitle mirrors a published course's title (newest event wins).
func UpsertCourseTitle(ctx context.Context, tx *gorm.DB, courseID uuid.UUID, title string, eventID int64) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO notification.course_titles (course_id, title, last_event_id, updated_at)
		VALUES (?, ?, ?, now())
		ON CONFLICT (course_id) DO UPDATE SET
			title = EXCLUDED.title, last_event_id = EXCLUDED.last_event_id, updated_at = now()
		WHERE EXCLUDED.last_event_id > notification.course_titles.last_event_id
	`, courseID, title, eventID).Error
}

// UpsertEnrollmentCourse records which course an enrollment belongs to.
func UpsertEnrollmentCourse(ctx context.Context, tx *gorm.DB, enrollmentID, courseID uuid.UUID) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO notification.enrollment_courses (enrollment_id, course_id) VALUES (?, ?)
		ON CONFLICT (enrollment_id) DO NOTHING
	`, enrollmentID, courseID).Error
}

// InsertDelivery records one notification for (eventID, kind, recipient)
// and reports whether a row was created. A duplicate event, a replay or a
// concurrent handler inserts nothing: the UNIQUE constraint is the
// arbiter, not a check-then-insert. The initial status reflects what is
// known now: no usable email -> parked, non-active recipient -> suppressed.
//
// The row also stores the traceparent of the span active in ctx (the relay's or
// the Kafka consumer's, itself a descendant of the originating request), so the
// sweeper can enqueue the delivery task under that trace. No span in ctx
// (tracing off) stores NULL.
func InsertDelivery(ctx context.Context, tx *gorm.DB, eventID uuid.UUID, kind string, recipientID uuid.UUID, params map[string]string) (bool, error) {
	var r struct {
		Email  *string
		Status string
	}
	res := tx.WithContext(ctx).Raw(`SELECT email, status FROM notification.recipients WHERE user_id = ?`, recipientID).Scan(&r)
	if res.Error != nil {
		return false, res.Error
	}
	status := statusPending
	switch {
	case res.RowsAffected == 0 || r.Email == nil || *r.Email == "":
		status = statusParked
	case r.Status != "active":
		status = statusSuppressed
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return false, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return false, err
	}
	_, _, traceparent := telemetry.InjectTraceContext(ctx)
	ins := tx.WithContext(ctx).Exec(`
		INSERT INTO notification.deliveries (id, event_id, kind, recipient_id, params, status, traceparent)
		VALUES (?, ?, ?, ?, ?::jsonb, ?, NULLIF(?, ''))
		ON CONFLICT ON CONSTRAINT deliveries_event_kind_recipient_key DO NOTHING
	`, id, eventID, kind, recipientID, string(raw), status, traceparent)
	return ins.RowsAffected == 1, ins.Error
}

// PromoteParked turns parked deliveries whose recipient now has an email
// into pending (they are picked up by the next EnqueuePending). Called by
// the identity handler for one user, and by the sweep for all users, which
// closes the race where a parked row was inserted just after the
// recipient's own promotion ran. Returns the number promoted.
func PromoteParked(ctx context.Context, db *gorm.DB, recipientID *uuid.UUID) (int64, error) {
	q := `UPDATE notification.deliveries d SET status = 'pending', enqueued_at = NULL
		FROM notification.recipients r
		WHERE d.status = 'parked' AND r.user_id = d.recipient_id AND r.email IS NOT NULL AND r.email <> ''`
	args := []any{}
	if recipientID != nil {
		q += ` AND d.recipient_id = ?`
		args = append(args, *recipientID)
	}
	res := db.WithContext(ctx).Exec(q, args...)
	return res.RowsAffected, res.Error
}

type deliveryRow struct {
	ID          uuid.UUID
	EventID     uuid.UUID
	Kind        string
	RecipientID uuid.UUID
	Params      []byte
	Attempts    int
}

// claimDelivery moves a delivery to 'sending' (a lease starting now) when it
// is pending, or when a previous claim's lease has expired (a crashed
// worker). It returns nil when there is nothing to claim, and the current
// status then explains why.
func claimDelivery(ctx context.Context, db *gorm.DB, id uuid.UUID, lease time.Duration) (*deliveryRow, error) {
	var rows []deliveryRow
	err := db.WithContext(ctx).Raw(`
		UPDATE notification.deliveries
		SET status = 'sending', claimed_at = now(), attempts = attempts + 1
		WHERE id = ? AND (status = 'pending'
		   OR (status = 'sending' AND claimed_at < now() - make_interval(secs => ?)))
		RETURNING id, event_id, kind, recipient_id, params, attempts
	`, id, lease.Seconds()).Scan(&rows).Error
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return &rows[0], nil
}

var errNoSuchDelivery = errors.New("delivery not found")

func deliveryStatus(ctx context.Context, db *gorm.DB, id uuid.UUID) (string, error) {
	var st []string
	if err := db.WithContext(ctx).Raw(`SELECT status FROM notification.deliveries WHERE id = ?`, id).Scan(&st).Error; err != nil {
		return "", err
	}
	if len(st) == 0 {
		return "", errNoSuchDelivery
	}
	return st[0], nil
}

// finishDelivery moves a claimed delivery out of 'sending'. The status guard
// means a task whose lease expired and was re-claimed elsewhere cannot
// overwrite the newer claim's outcome.
func finishDelivery(ctx context.Context, db *gorm.DB, id uuid.UUID, status string, cause error) error {
	var msg *string
	if cause != nil {
		s := strings.ToValidUTF8(cause.Error(), "")
		if len(s) > maxLastErrorLen {
			s = s[:maxLastErrorLen]
		}
		msg = &s
	}
	return db.WithContext(ctx).Exec(`
		UPDATE notification.deliveries
		SET status = ?, last_error = ?, sent_at = CASE WHEN ? = 'sent' THEN now() ELSE sent_at END
		WHERE id = ? AND status = 'sending'
	`, status, msg, status, id).Error
}

type recipientRow struct {
	Email  *string
	Name   string
	Status string
}

func loadRecipient(ctx context.Context, db *gorm.DB, id uuid.UUID) (*recipientRow, error) {
	var rows []recipientRow
	if err := db.WithContext(ctx).Raw(
		`SELECT email, name, status FROM notification.recipients WHERE user_id = ?`, id).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
}

// courseTitle resolves the title for a delivery's params: a title carried in
// the params wins, then the course_id's mirrored title, then the course of
// the enrollment. Empty when none is known yet (the template falls back).
func courseTitle(ctx context.Context, db *gorm.DB, params map[string]string) (string, error) {
	if t := params["title"]; t != "" {
		return t, nil
	}
	var titles []string
	var err error
	switch {
	case params["course_id"] != "":
		err = db.WithContext(ctx).Raw(`SELECT title FROM notification.course_titles WHERE course_id = ?`, params["course_id"]).Scan(&titles).Error
	case params["enrollment_id"] != "":
		err = db.WithContext(ctx).Raw(`
			SELECT t.title FROM notification.enrollment_courses e
			JOIN notification.course_titles t ON t.course_id = e.course_id
			WHERE e.enrollment_id = ?`, params["enrollment_id"]).Scan(&titles).Error
	}
	if err != nil || len(titles) == 0 {
		return "", err
	}
	return titles[0], nil
}

// failExhausted marks pending deliveries that have used up their attempts as
// permanently failed, so a dead provider does not retry forever.
func failExhausted(ctx context.Context, db *gorm.DB, maxAttempts int) (int64, error) {
	res := db.WithContext(ctx).Exec(`
		UPDATE notification.deliveries
		SET status = 'failed', last_error = COALESCE(last_error, '') || ' [gave up after ' || attempts::text || ' attempts]'
		WHERE status = 'pending' AND attempts >= ?`, maxAttempts)
	return res.RowsAffected, res.Error
}

var errBadParams = fmt.Errorf("%w: unreadable delivery params", ErrPermanent)

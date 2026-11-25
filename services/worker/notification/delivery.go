package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const (
	defaultLease       = 5 * time.Minute
	defaultMaxAttempts = 10
)

// DeliveryOption customises NewDeliveryHandler.
type DeliveryOption func(*deliverer)

// WithLease sets how long a 'sending' claim is honoured before another run
// may take the row over (default 5m). It must be longer than the provider's
// send timeout plus the queue's task timeout, or a slow-but-alive send can
// be sent a second time.
func WithLease(d time.Duration) DeliveryOption { return func(x *deliverer) { x.lease = d } }

type deliverer struct {
	db       *gorm.DB
	provider Provider
	lease    time.Duration
}

// NewDeliveryHandler returns the body of the notification:deliver task. It is
// a plain function (no queue import); the Asynq adapter calls it with the
// task payload and should map errors wrapping ErrPermanent to SkipRetry and
// retry every other error with backoff.
//
// One run: claim the delivery row (pending -> sending, a lease), render,
// send through the provider, mark it sent.
//
//   - Already sent (or failed, parked, suppressed, or being sent by a live
//     claim): returns nil without touching the provider. A duplicate event,
//     a duplicate task or a re-run never sends twice in the normal case.
//   - Provider failure: the row goes back to pending with last_error and the
//     error is returned, so the queue retries with backoff.
//   - Recipient not in the mirror yet: the row is parked and nil returned;
//     nothing is lost, the recipient's arrival promotes it.
//
// Duplicate-send window that remains, and cannot be closed without a
// provider that accepts an idempotency key: the provider accepted the mail,
// then marking the row sent failed (database error, process killed). The row
// stays 'sending'; once the lease expires it is claimed again and the mail
// goes out a second time. The same happens if a send outlives its lease, or
// if an SMTP connection drops after the final "." of DATA but before the
// reply (the server may have accepted it). The Message-ID is the delivery
// id, so the rare duplicate is recognisable in the mailbox. This is the
// accepted trade-off of RFC §6.6: a rare duplicate rather than a lost mail.
func NewDeliveryHandler(db *gorm.DB, provider Provider, opts ...DeliveryOption) func(ctx context.Context, payload []byte) error {
	d := &deliverer{db: db, provider: provider, lease: defaultLease}
	for _, o := range opts {
		o(d)
	}
	return d.handle
}

func (d *deliverer) handle(ctx context.Context, payload []byte) error {
	var p struct {
		DeliveryID uuid.UUID `json:"delivery_id"`
	}
	if err := json.Unmarshal(payload, &p); err != nil || p.DeliveryID == uuid.Nil {
		return fmt.Errorf("%w: unreadable delivery payload", ErrPermanent)
	}
	row, err := claimDelivery(ctx, d.db, p.DeliveryID, d.lease)
	if err != nil {
		return fmt.Errorf("claim delivery %s: %w", p.DeliveryID, err)
	}
	if row == nil {
		st, err := deliveryStatus(ctx, d.db, p.DeliveryID)
		if errors.Is(err, errNoSuchDelivery) {
			return fmt.Errorf("%w: delivery %s does not exist", ErrPermanent, p.DeliveryID)
		}
		if err != nil {
			return err
		}
		slog.Debug("notification delivery already handled", slog.String("delivery_id", p.DeliveryID.String()), slog.String("status", st))
		return nil
	}

	// From here the row is 'sending'. Every exit moves it somewhere.
	var params map[string]string
	if err := json.Unmarshal(row.Params, &params); err != nil {
		_ = finishDelivery(ctx, d.db, row.ID, statusFailed, errBadParams)
		return errBadParams
	}
	rcpt, err := loadRecipient(ctx, d.db, row.RecipientID)
	if err != nil {
		d.release(row.ID, err)
		return fmt.Errorf("load recipient: %w", err)
	}
	if rcpt == nil || rcpt.Email == nil || *rcpt.Email == "" {
		return d.finish(row.ID, statusParked, nil)
	}
	if rcpt.Status != "active" {
		return d.finish(row.ID, statusSuppressed, nil)
	}
	title, err := courseTitle(ctx, d.db, params)
	if err != nil {
		d.release(row.ID, err)
		return fmt.Errorf("load course title: %w", err)
	}
	subject, text, html, err := Render(row.Kind, TemplateData{
		Name: rcpt.Name, CourseTitle: title, FailedActivity: params["failed_activity"], Reason: params["reason"],
	})
	if err != nil {
		_ = d.finish(row.ID, statusFailed, err)
		return err
	}

	sendErr := d.provider.Send(ctx, Message{
		ID: row.ID.String(), To: *rcpt.Email, ToName: rcpt.Name, Subject: subject, Text: text, HTML: html,
	})
	if sendErr != nil {
		if errors.Is(sendErr, ErrPermanent) {
			_ = d.finish(row.ID, statusFailed, sendErr)
			return sendErr
		}
		d.release(row.ID, sendErr)
		return fmt.Errorf("send delivery %s: %w", row.ID, sendErr)
	}
	// The mail is out. Mark it with a context that survives cancellation
	// (shutdown at this exact moment must not strand the row) and retry a
	// few times: a failure here is the duplicate-send window above.
	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	var markErr error
	for attempt := 0; attempt < 3; attempt++ {
		if markErr = finishDelivery(markCtx, d.db, row.ID, statusSent, nil); markErr == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	slog.Error("notification sent but could not be marked sent; a duplicate send is possible once the lease expires",
		slog.String("delivery_id", row.ID.String()), slog.Any("error", markErr))
	return fmt.Errorf("delivery %s sent but not marked: %w", row.ID, markErr)
}

// release puts a claimed row back to pending after a retryable failure. It
// uses a detached context so a cancelled task still releases its claim.
func (d *deliverer) release(id uuid.UUID, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := finishDelivery(ctx, d.db, id, statusPending, cause); err != nil {
		slog.Error("notification: release claim failed", slog.String("delivery_id", id.String()), slog.Any("error", err))
	}
}

func (d *deliverer) finish(id uuid.UUID, status string, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return finishDelivery(ctx, d.db, id, status, cause)
}

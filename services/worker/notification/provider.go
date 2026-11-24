// Package notification is the notification-dispatcher (RFC §6.5, §6.7,
// ADR-0014): relay handlers that turn enrollment, progress, course and
// identity events into delivery records, and the delivery task that sends
// them through a Provider.
//
// Three stages, deliberately separate:
//
//  1. Handlers (handlers.go) run inside the relay's per-event transaction
//     and only write rows: the recipient mirror and a delivery record
//     (UNIQUE per event, kind, recipient). No network, nothing to undo if
//     the transaction rolls back.
//  2. EnqueuePending (dispatcher.go) hands rows that still need work to an
//     Enqueuer. The delivery row, not the queue, is the source of truth, so
//     a crash anywhere between commit and enqueue is repaired by the next
//     sweep rather than lost.
//  3. NewDeliveryHandler (delivery.go) is the queue task: claim the row,
//     send, mark it sent.
//
// Schema-per-service (ADR-0024): every query here is qualified with the
// `notification` schema and nothing reads identity or course tables.
package notification

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrPermanent marks a failure that retrying cannot fix (a rejected
// recipient, a malformed payload). The delivery handler returns it wrapped;
// the queue adapter should map it to "do not retry" (asynq.SkipRetry).
var ErrPermanent = errors.New("permanent delivery failure")

// Message is one outbound email, already rendered. Provider implementations
// own the wire format and must treat every field as untrusted text.
type Message struct {
	// ID is a stable identifier for this logical message (the delivery id).
	// SMTP puts it in Message-ID, so a rare duplicate send is recognisable.
	ID      string
	To      string // bare address
	ToName  string // display name, optional
	Subject string
	Text    string
	HTML    string // optional; sent as multipart/alternative when set
}

// Provider delivers one Message. A nil error means the provider accepted it.
// Errors wrapping ErrPermanent will not be retried; anything else will.
type Provider interface {
	Send(ctx context.Context, msg Message) error
}

// ProviderFunc adapts a function to Provider (handy in tests).
type ProviderFunc func(ctx context.Context, msg Message) error

func (f ProviderFunc) Send(ctx context.Context, msg Message) error { return f(ctx, msg) }

// hasControl reports whether s contains any ASCII control character,
// including CR, LF, NUL and DEL: the raw material of header injection.
func hasControl(s string) bool {
	return strings.IndexFunc(s, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0
}

// cleanLine makes untrusted text safe for a single header line: every
// control character (CR and LF included) becomes a space, then runs of
// whitespace collapse. It is applied to subjects and display names.
func cleanLine(s string) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == 0x2028 || r == 0x2029 {
			return ' '
		}
		return r
	}, s)
	return strings.Join(strings.Fields(s), " ")
}

func (m Message) validate() error {
	if m.To == "" || hasControl(m.To) || strings.ContainsAny(m.To, "<>,;\" ") {
		return fmt.Errorf("%w: invalid recipient address", ErrPermanent)
	}
	if hasControl(m.ID) || strings.ContainsAny(m.ID, "<>@ ") {
		return fmt.Errorf("%w: invalid message id", ErrPermanent)
	}
	return nil
}

package kafka

import (
	"errors"
	"fmt"
)

var (
	// ErrInvalidConfig wraps every configuration validation failure.
	ErrInvalidConfig = errors.New("kafka: invalid config")
	// ErrClosed is returned by Produce after Close.
	ErrClosed = errors.New("kafka: producer closed")
	// ErrDeliveryStuck is returned by Produce when franz-go reported neither
	// success nor failure for a record within DeliveryTimeout plus a grace
	// period. The record may or may not have been stored; the client was
	// replaced.
	ErrDeliveryStuck = errors.New("kafka: record neither delivered nor failed within the delivery timeout")
	// ErrAlreadyRunning is returned when Run is called a second time.
	ErrAlreadyRunning = errors.New("kafka: consumer already started")
	// ErrDrainTimeout is returned by Run when in-flight handlers had to be
	// cancelled because they did not finish within ShutdownTimeout. Their
	// records were not committed and will be redelivered.
	ErrDrainTimeout = errors.New("kafka: in-flight handlers did not drain before the shutdown timeout")
	// ErrPanic wraps a recovered handler or hook panic.
	ErrPanic = errors.New("kafka: handler panicked")
)

func errInvalid(msg string) error { return fmt.Errorf("%w: %s", ErrInvalidConfig, msg) }

func errNegative(what string) error { return errInvalid(what + " must not be negative") }

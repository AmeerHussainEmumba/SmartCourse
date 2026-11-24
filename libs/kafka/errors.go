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

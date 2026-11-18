package telemetry

import (
	"errors"
	"fmt"
	"math"
	"strings"
)

// Config configures tracing. It is a plain struct so that this module does not
// depend on libs/config; the caller binds environment variables to it.
//
// The zero value is the disabled configuration: Setup does nothing, opens no
// connection and starts no goroutine.
type Config struct {
	// Enabled turns tracing on. When false every other field is ignored.
	Enabled bool

	// ServiceName becomes the service.name resource attribute (required when
	// enabled), for example "course-api".
	ServiceName string
	// ServiceVersion and Environment are optional resource attributes.
	ServiceVersion string
	Environment    string

	// Endpoint is the OTLP/HTTP collector address, either host:port
	// ("jaeger:4318") or a full URL ("http://jaeger:4318"). Required when
	// enabled. Jaeger accepts OTLP/HTTP on 4318 (RFC §12.2).
	Endpoint string
	// Insecure sends plain HTTP when Endpoint is host:port. A URL endpoint
	// carries its own scheme and ignores this.
	Insecure bool

	// SampleRatio is the head-based sampling probability in [0, 1] (RFC §12.2:
	// 1 in development, configurable in deployment). Spans that end in error
	// are exported regardless; see the README for the exact semantics.
	SampleRatio float64
}

// Validate reports a configuration that cannot be used. A disabled
// configuration is always valid.
func (c Config) Validate() error {
	return c.validate(true)
}

func (c Config) validate(needEndpoint bool) error {
	if !c.Enabled {
		return nil
	}
	var errs []error
	if strings.TrimSpace(c.ServiceName) == "" {
		errs = append(errs, errors.New("telemetry: ServiceName is required"))
	}
	if needEndpoint && strings.TrimSpace(c.Endpoint) == "" {
		errs = append(errs, errors.New("telemetry: Endpoint is required"))
	}
	if math.IsNaN(c.SampleRatio) || c.SampleRatio < 0 || c.SampleRatio > 1 {
		errs = append(errs, fmt.Errorf("telemetry: SampleRatio %v is outside [0, 1]", c.SampleRatio))
	}
	return errors.Join(errs...)
}

package config

import (
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// TelemetryConfig configures tracing and the /metrics surface (ADR-0032,
// RFC §12). Every binary reads the same environment, so Load validates it for
// all of them. Nothing here is required: with the endpoint unset the service
// runs with tracing off, never with an error.
type TelemetryConfig struct {
	// OTLPEndpoint is OTEL_EXPORTER_OTLP_ENDPOINT, the OTLP/HTTP collector as
	// host:port ("jaeger:4318") or an http(s) URL. Empty means tracing is off.
	OTLPEndpoint string
	// TracingDisabled is OTEL_SDK_DISABLED=true, the standard switch: it turns
	// tracing off even when an endpoint is set.
	TracingDisabled bool
	// ServiceName is OTEL_SERVICE_NAME. Empty means the binary supplies its
	// own name ("course-api").
	ServiceName string
	// SampleRatio is OTEL_TRACES_SAMPLER_ARG in [0, 1] (default 1: sample
	// everything, the development setting, RFC §12.2).
	SampleRatio float64
	// MetricsEnabled is METRICS_ENABLED (default true). When false no
	// /metrics route, no collectors and no request metrics are installed.
	MetricsEnabled bool
	// MetricsAddr is METRICS_HTTP_ADDR, the worker's own metrics listener
	// (default ":9100"). The three HTTP services serve /metrics on their API
	// port instead and ignore it. It is unauthenticated: bind it to a private
	// interface (RFC §12.3).
	MetricsAddr string
}

// TracingEnabled reports whether spans should be recorded and exported.
func (t TelemetryConfig) TracingEnabled() bool {
	return t.OTLPEndpoint != "" && !t.TracingDisabled
}

const defaultMetricsAddr = ":9100"

func loadTelemetry(getenv func(string) string, optBool func(string, bool) bool) (TelemetryConfig, []string) {
	t := TelemetryConfig{
		OTLPEndpoint:    strings.TrimSpace(getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		TracingDisabled: optBool("OTEL_SDK_DISABLED", false),
		ServiceName:     strings.TrimSpace(getenv("OTEL_SERVICE_NAME")),
		SampleRatio:     1,
		MetricsEnabled:  optBool("METRICS_ENABLED", true),
		MetricsAddr:     strings.TrimSpace(getenv("METRICS_HTTP_ADDR")),
	}
	if t.MetricsAddr == "" {
		t.MetricsAddr = defaultMetricsAddr
	}
	var errs []string
	if v := strings.TrimSpace(getenv("OTEL_TRACES_SAMPLER_ARG")); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || math.IsNaN(f) || f < 0 || f > 1 {
			errs = append(errs, "OTEL_TRACES_SAMPLER_ARG must be a number from 0 to 1")
		} else {
			t.SampleRatio = f
		}
	}
	if t.OTLPEndpoint != "" {
		if msg := otlpEndpointError(t.OTLPEndpoint); msg != "" {
			errs = append(errs, msg)
		}
	}
	if strings.ContainsAny(t.ServiceName, " \t\r\n") {
		errs = append(errs, "OTEL_SERVICE_NAME must not contain whitespace")
	}
	if msg := listenAddrError("METRICS_HTTP_ADDR", t.MetricsAddr); msg != "" {
		errs = append(errs, msg)
	}
	return t, errs
}

// otlpEndpointError checks the shape only; the endpoint is never dialled at
// load time (an unreachable collector must not stop a service). The value is
// not echoed: a URL may carry a token.
func otlpEndpointError(ep string) string {
	const key = "OTEL_EXPORTER_OTLP_ENDPOINT"
	if strings.Contains(ep, "://") {
		u, err := url.Parse(ep)
		switch {
		case err != nil:
			return key + " is not a valid URL"
		case u.Scheme != "http" && u.Scheme != "https":
			return key + " must use http:// or https:// (OTLP/HTTP)"
		case u.Hostname() == "":
			return key + " must name a host"
		case u.User != nil:
			return key + " must not carry credentials in the URL"
		}
		return ""
	}
	host, port, err := net.SplitHostPort(ep)
	if err != nil || host == "" {
		return key + " must be host:port (for example jaeger:4318) or an http(s) URL"
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return key + " port must be a number from 1 to 65535"
	}
	return ""
}

func listenAddrError(key, addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Sprintf("%s must be host:port, for example :9100 (%v)", key, err)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return key + " port must be a number from 1 to 65535"
	}
	if host != "" && net.ParseIP(host) == nil && !validHostname(host) {
		return key + " host must be an IP address or a host name"
	}
	return ""
}

func validHostname(h string) bool {
	if len(h) > 253 {
		return false
	}
	for _, r := range h {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
		default:
			return false
		}
	}
	return true
}

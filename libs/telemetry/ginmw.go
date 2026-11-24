package telemetry

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// UnmatchedRoute is the route label for requests no route matched (404/405).
// It is a constant so scanners probing random paths cannot create series.
const UnmatchedRoute = "unmatched"

// untraced are the routes HTTPTracing skips (see its doc).
var untraced = map[string]bool{"/metrics": true, "/health/live": true, "/health/ready": true}

// otherMethod labels request methods outside the standard set.
const otherMethod = "OTHER"

// HTTPTracing returns Gin middleware that continues an incoming W3C trace
// (traceparent header) or starts a new one, and wraps the request in a SERVER
// span named "METHOD route-template". 5xx responses and handler errors mark
// the span failed. Mount it after request-ID and before logging so the logger
// can read the trace id from the request context.
//
// Probe and scrape routes (/metrics, /health/live, /health/ready) are not
// traced: Prometheus and the orchestrator hit them every few seconds forever,
// and each would be a one-span trace that buries the real ones.
func HTTPTracing() gin.HandlerFunc {
	return func(c *gin.Context) {
		if untraced[c.FullPath()] {
			c.Next()
			return
		}
		ctx := propagator.Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
		method := methodLabel(c.Request.Method)
		ctx, span := tracer().Start(ctx, method,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", method)))
		defer span.End()

		c.Request = c.Request.WithContext(ctx)
		c.Next()

		route := routeLabel(c)
		status := c.Writer.Status()
		if route != UnmatchedRoute {
			span.SetName(method + " " + route)
			span.SetAttributes(attribute.String("http.route", route))
		}
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		// Correlates the trace with the access log line and the X-Request-Id
		// the client saw (RFC §12.4); set by httpx.RequestID, which runs first.
		if id := c.GetString("request_id"); id != "" {
			span.SetAttributes(attribute.String("smartcourse.request_id", id))
		}
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, "")
		}
		for _, e := range c.Errors {
			span.RecordError(e.Err)
		}
	}
}

// HTTPMetrics returns Gin middleware recording http_requests_total and
// http_request_duration_seconds (RED). The route label is the registered route
// template (c.FullPath()), never the raw path, so identifiers in URLs cannot
// grow the series count; unmatched requests use UnmatchedRoute.
func HTTPMetrics(m *Metrics) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		route := routeLabel(c)
		method := methodLabel(c.Request.Method)
		m.HTTPRequests.WithLabelValues(route, method, statusLabel(c.Writer.Status())).Inc()
		m.HTTPDuration.WithLabelValues(route, method).Observe(time.Since(start).Seconds())
	}
}

func routeLabel(c *gin.Context) string {
	if r := c.FullPath(); r != "" {
		return r
	}
	return UnmatchedRoute
}

// methodLabel bounds the method label: Gin routes only registered methods, but
// an unmatched request can carry any token.
func methodLabel(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut,
		http.MethodPatch, http.MethodDelete, http.MethodOptions,
		http.MethodConnect, http.MethodTrace:
		return m
	default:
		return otherMethod
	}
}

func statusLabel(code int) string {
	if code < 100 || code > 599 {
		return "0"
	}
	return strconv.Itoa(code)
}

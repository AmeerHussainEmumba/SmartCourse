package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// apiError is a non-2xx answer from a service.
type apiError struct {
	Method, Path string
	Status       int
	Code         string // the error body's "code", when it has one
	Message      string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d %s %s", e.Method, e.Path, e.Status, e.Code, e.Message)
}

// statusOf returns the HTTP status of err when it is an *apiError, else 0.
func statusOf(err error) int {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Status
	}
	return 0
}

// codeOf returns the error code of err when it is an *apiError, else "".
func codeOf(err error) string {
	var ae *apiError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return ""
}

// client is a minimal JSON-over-HTTP client for one service.
type client struct {
	base   string
	http   *http.Client
	logger *slog.Logger
	// sleep waits between rate-limit retries; tests replace it.
	sleep func(context.Context, time.Duration) error
	// rateLimitWait is how long to wait after a 429 before the next try. The
	// services count per minute (DR-022), so a few waits cover a window.
	rateLimitWait time.Duration
	// maxRateLimitRetries bounds the waiting per request.
	maxRateLimitRetries int
}

// ipSeq numbers the client addresses sent as X-Forwarded-For. identity-api
// rate-limits register and login per client IP (DR-022: 5 and 20 a minute). A
// service that trusts its proxy (TRUSTED_PROXIES covers the peer address, as the
// E2E harness sets for loopback) reads this header, so every request claims its
// own address and the limits do not throttle the seed. A service that does not
// trust it ignores the header, and the seed falls back to waiting out a 429.
var ipSeq atomic.Uint32

func nextIP() string {
	n := ipSeq.Add(1)
	return fmt.Sprintf("10.%d.%d.%d", (n>>16)&255, (n>>8)&255, n&255)
}

func newClient(base string, logger *slog.Logger) *client {
	return &client{
		base:                base,
		http:                &http.Client{Timeout: 30 * time.Second},
		logger:              logger,
		sleep:               sleepCtx,
		rateLimitWait:       20 * time.Second,
		maxRateLimitRetries: 6,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// do sends one request, retrying while the service answers 429, and decodes a
// 2xx JSON body into out when out is not nil. The token and the request body
// are never logged.
func (c *client) do(ctx context.Context, method, path, token string, body, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return fmt.Errorf("encode %s %s: %w", method, path, err)
		}
	}
	for attempt := 0; ; attempt++ {
		err := c.once(ctx, method, path, token, payload, out)
		if statusOf(err) != http.StatusTooManyRequests || attempt >= c.maxRateLimitRetries {
			return err
		}
		c.logger.Warn("rate limited; waiting before retrying",
			slog.String("method", method), slog.String("path", path), slog.Duration("wait", c.rateLimitWait))
		if err := c.sleep(ctx, c.rateLimitWait); err != nil {
			return err
		}
	}
}

func (c *client) once(ctx context.Context, method, path, token string, payload []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Forwarded-For", nextIP())
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("%s %s: read body: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		ae := &apiError{Method: method, Path: path, Status: resp.StatusCode}
		var eb struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		if json.Unmarshal(raw, &eb) == nil {
			ae.Code, ae.Message = eb.Code, eb.Message
		}
		return ae
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("%s %s: decode response: %w", method, path, err)
		}
	}
	return nil
}

// ready polls GET /health/ready until it answers 200 or the wait passes.
func (c *client) ready(ctx context.Context, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		err := c.once(ctx, http.MethodGet, "/health/ready", "", nil, nil)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		if err := c.sleep(ctx, 500*time.Millisecond); err != nil {
			return err
		}
	}
}

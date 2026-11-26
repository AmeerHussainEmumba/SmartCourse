//go:build integration

// Regression tests for behaviour findings B-02 and B-03
// (docs/plans/week-3-findings-behaviour.md), fixed by storing the response
// byte for byte (migration course/000009).
package idempotency_test

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency"
)

// B-02 (was: a successful response with no JSON body (204, or 202 with an empty
// body) is "not storable", so the key is released and the retry runs the
// handler a second time. The contract is "a retry gets the stored response
// back instead of a second execution"; for a 2xx without a body it silently
// does not hold, and nothing tells the route author). Now stored and replayed.
func TestSameKey_EmptyBody2xx_ReplaysAndRunsOnce(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusAccepted} {
		h := newHarness(t)
		h.next = func(c *gin.Context, _ int32) { c.Status(status) }
		first := h.do(userA, "k-empty", `{}`)
		second := h.do(userA, "k-empty", `{}`)
		if second.Code != status || second.Body.Len() != 0 || second.Header().Get(idempotency.ReplayedHeader) != "true" {
			t.Errorf("status %d: replay = %d %q replayed=%q (first %d)", status, second.Code, second.Body.String(), second.Header().Get(idempotency.ReplayedHeader), first.Code)
		}
		if got := h.calls.Load(); got != 1 {
			t.Errorf("status %d: handler ran %d times for one Idempotency-Key, want 1", status, got)
		}
	}
}

// B-03 (was: the stored response is round-tripped through jsonb, so the replay is
// not the response the first call returned: key order, whitespace and number
// formatting change, and the original headers (Location, Content-Type
// charset) are not replayed at all. A client that compares or hashes the body
// sees two different answers for one key). Now replayed byte for byte.
func TestReplay_IsByteIdenticalToTheOriginal(t *testing.T) {
	h := newHarness(t)
	const body = `{"b":1,"a":2.50,"id":"x"}`
	h.next = func(c *gin.Context, _ int32) {
		c.Header("Location", "/things/1")
		c.Data(http.StatusCreated, "application/json; charset=utf-8", []byte(body))
	}
	first := h.do(userA, "k-bytes", `{}`)
	second := h.do(userA, "k-bytes", `{}`)
	if second.Header().Get(idempotency.ReplayedHeader) != "true" {
		t.Fatal("second call was not a replay")
	}
	if second.Body.String() != first.Body.String() {
		t.Errorf("replayed body %q != original %q", second.Body.String(), first.Body.String())
	}
	if second.Header().Get("Location") != first.Header().Get("Location") {
		t.Errorf("replay dropped the Location header: %q vs %q", second.Header().Get("Location"), first.Header().Get("Location"))
	}
}

// B-02 companion: a 2xx whose body is not JSON (plain text, XML) is stored
// and replayed with its Content-Type, and a 2xx with a body that Postgres
// jsonb cannot hold is stored too.
func TestReplay_NonJSONBody_IsStoredAndReplayedWithContentType(t *testing.T) {
	h := newHarness(t)
	h.next = func(c *gin.Context, _ int32) {
		c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte("ok  \n"))
	}
	first := h.do(userA, "k-text", `{}`)
	second := h.do(userA, "k-text", `{}`)
	if got := h.calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
	if second.Body.String() != first.Body.String() || second.Header().Get("Content-Type") != first.Header().Get("Content-Type") {
		t.Errorf("replay %q (%s) != original %q (%s)", second.Body.String(), second.Header().Get("Content-Type"), first.Body.String(), first.Header().Get("Content-Type"))
	}
}

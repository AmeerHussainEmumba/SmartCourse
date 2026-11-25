//go:build integration && bugrepro

// Reproducers for behaviour defects found by the second adversarial pass
// (docs/plans/week-3-findings-behaviour.md). They fail today by design.
//
//	cd libs/idempotency && go test -tags='integration bugrepro' -run Bugrepro .
package idempotency_test

import (
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency"
)

// B-02: a successful response with no JSON body (204, or 202 with an empty
// body) is "not storable", so the key is released and the retry runs the
// handler a second time. The contract is "a retry gets the stored response
// back instead of a second execution"; for a 2xx without a body it silently
// does not hold, and nothing tells the route author.
func TestBugrepro_SameKey_EmptyBody2xx_ExecutesAgain(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusAccepted} {
		h := newHarness(t)
		h.next = func(c *gin.Context, _ int32) { c.Status(status) }
		h.do(userA, "k-empty", `{}`)
		h.do(userA, "k-empty", `{}`)
		if got := h.calls.Load(); got != 1 {
			t.Errorf("status %d: handler ran %d times for one Idempotency-Key, want 1", status, got)
		}
	}
}

// B-03: the stored response is round-tripped through jsonb, so the replay is
// not the response the first call returned: key order, whitespace and number
// formatting change, and the original headers (Location, Content-Type
// charset) are not replayed at all. A client that compares or hashes the body
// sees two different answers for one key.
func TestBugrepro_Replay_IsNotByteIdenticalToTheOriginal(t *testing.T) {
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

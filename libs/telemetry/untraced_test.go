package telemetry

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHTTPTracingSkipsProbeAndScrapeRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	p, exp := newTestProvider(t, 1)
	r := gin.New()
	r.Use(HTTPTracing())
	for _, path := range []string{"/metrics", "/health/live", "/health/ready", "/real"} {
		r.GET(path, func(c *gin.Context) { c.Status(http.StatusNoContent) })
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}
	flush(t, p)

	spans := exp.GetSpans()
	if len(spans) != 1 || spans[0].Name != "GET /real" {
		var names []string
		for _, s := range spans {
			names = append(names, s.Name)
		}
		t.Fatalf("want only the span for /real, got %v", names)
	}
}

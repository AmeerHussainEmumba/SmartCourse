package telemetry

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHTTPTracingRecordsRequestID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	p, exp := newTestProvider(t, 1)
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set("request_id", "req-123"); c.Next() }, HTTPTracing())
	r.GET("/ok", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ok", nil))
	flush(t, p)

	if !hasAttr(spanByName(t, exp.GetSpans(), "GET /ok"), "smartcourse.request_id", "req-123") {
		t.Error("request id missing from the server span")
	}
}

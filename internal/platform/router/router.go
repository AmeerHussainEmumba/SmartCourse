// Package router assembles the Gin engine every module registers its routes
// on. It owns cross-cutting middleware (request ID, recovery, structured
// logging, CORS) so no individual module has to reimplement it.
package router

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/httpx"
)

// New builds a Gin engine with the standard middleware chain. allowedOrigins
// is deny-by-default (RFC §11.9): an empty slice means no cross-origin
// requests are permitted at all, which is the safe default until a real
// client origin is configured.
func New(env string, allowedOrigins []string) *gin.Engine {
	if env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(httpx.RequestID())
	r.Use(requestLogger())
	r.Use(gin.Recovery())
	r.Use(cors(allowedOrigins))
	return r
}

// requestLogger emits one structured line per request (RFC §12.4): method,
// path, status, latency, request_id. Detail beyond that lives in traces
// (§12.2, wired in M4), not in this log line.
func requestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		slog.Info("request",
			slog.String("method", c.Request.Method),
			slog.String("path", c.FullPath()),
			slog.Int("status", c.Writer.Status()),
			slog.Duration("latency", time.Since(start)),
			slog.String("request_id", httpx.RequestIDFromContext(c)),
		)
	}
}

// cors implements the deny-by-default policy from RFC §11.9: an explicit
// allowlist, never "*", and an unconfigured origin is refused rather than
// silently allowed.
func cors(allowedOrigins []string) gin.HandlerFunc {
	allowed := make(map[string]bool, len(allowedOrigins))
	for _, o := range allowedOrigins {
		allowed[o] = true
	}
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		if origin != "" && allowed[origin] {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Vary", "Origin")
			c.Header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key")
		}
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

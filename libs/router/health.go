package router

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Checker is a dependency readiness probe — Postgres and Redis both
// implement this shape — see libs/infra.Dependencies.HealthCheckers.
type Checker func(ctx context.Context) error

// RegisterHealth wires /health/live and /health/ready (RFC §12.5).
//
// The two are deliberately different: /health/live never checks
// dependencies, because a dependency failure must not cause a restart loop.
// /health/ready checks every dependency named in deps and returns 503 when
// one is down, so a load balancer stops routing without the process being
// killed. Conflating them means a brief Redis outage restarts every replica
// simultaneously, converting a degradation into an outage.
func RegisterHealth(r *gin.Engine, deps map[string]Checker) {
	r.GET("/health/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "live"})
	})

	r.GET("/health/ready", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()

		// Per-dependency result is a bare ok/failed: raw driver errors carry
		// host, port, user and database names and /health/ready is
		// unauthenticated (F-13). The detail goes to the log instead.
		checks := map[string]string{}
		failed := false
		for name, check := range deps {
			if err := check(ctx); err != nil {
				failed = true
				checks[name] = "failed"
				slog.Error("readiness check failed", slog.String("dependency", name), slog.Any("error", err))
				continue
			}
			checks[name] = "ok"
		}
		if failed {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "checks": checks})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})
}

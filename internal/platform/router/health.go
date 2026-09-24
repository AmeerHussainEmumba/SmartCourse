package router

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Checker is a dependency readiness probe — Postgres and Redis both
// implement this shape via small adapter funcs in cmd/smartcourse.
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

		failures := map[string]string{}
		for name, check := range deps {
			if err := check(ctx); err != nil {
				failures[name] = err.Error()
			}
		}
		if len(failures) > 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "not_ready", "failures": failures})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})
}

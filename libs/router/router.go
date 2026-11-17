// Package router assembles the Gin engine every module registers its routes
// on. It owns cross-cutting middleware (request ID, recovery, structured
// logging, CORS) so no individual module has to reimplement it.
package router

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/httpx"
)

// Options configures NewWithOptions. The zero value is the safe default:
// no trusted proxies, 1 MiB body cap, no HSTS.
type Options struct {
	Env            string
	AllowedOrigins []string
	// TrustedProxies are the IPs/CIDRs whose X-Forwarded-For / X-Real-IP
	// headers are believed. Empty (default) trusts none, so c.ClientIP() is
	// the TCP peer and per-IP rate limits cannot be bypassed by header
	// rotation (F-02).
	TrustedProxies []string
	// MaxBodyBytes caps request bodies; <= 0 means httpx.DefaultMaxBodyBytes.
	MaxBodyBytes int64
	// HSTS forces Strict-Transport-Security; it is always sent for requests
	// that arrive over TLS.
	HSTS bool
}

// New builds a Gin engine with the standard middleware chain and the safe
// defaults of Options (no trusted proxies). allowedOrigins is
// deny-by-default (RFC §11.9): an empty slice means no cross-origin requests
// are permitted at all, which is the safe default until a real client origin
// is configured.
func New(env string, allowedOrigins []string) *gin.Engine {
	r, err := NewWithOptions(Options{Env: env, AllowedOrigins: allowedOrigins})
	if err != nil {
		// Unreachable: with no trusted proxies configured nothing can fail.
		panic(err)
	}
	return r
}

// NewWithOptions is New with explicit trusted proxies, body cap and HSTS.
// It returns an error for an unparsable TrustedProxies entry.
func NewWithOptions(o Options) (*gin.Engine, error) {
	if o.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	// SetTrustedProxies(nil) means "trust no proxy"; gin's default is to
	// trust every peer.
	if err := r.SetTrustedProxies(o.TrustedProxies); err != nil {
		return nil, fmt.Errorf("invalid trusted proxies: %w", err)
	}
	r.Use(httpx.RequestID())
	r.Use(httpx.SecurityHeaders(o.HSTS))
	r.Use(requestLogger())
	r.Use(gin.Recovery())
	r.Use(httpx.MaxBodyBytes(o.MaxBodyBytes))
	r.Use(cors(o.AllowedOrigins))
	return r, nil
}

// ServerOptions are the http.Server limits applied by NewServer.
type ServerOptions struct {
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

// NewServer returns an http.Server with every timeout set (F-16): a server
// with only ReadHeaderTimeout still lets a client dribble a body or hold a
// keep-alive connection forever. Zero fields take defaults chosen so the
// 202-returning publish endpoint is unaffected.
func NewServer(addr string, h http.Handler, o ServerOptions) *http.Server {
	if o.ReadHeaderTimeout <= 0 {
		o.ReadHeaderTimeout = 5 * time.Second
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 15 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 30 * time.Second
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 120 * time.Second
	}
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: o.ReadHeaderTimeout,
		ReadTimeout:       o.ReadTimeout,
		WriteTimeout:      o.WriteTimeout,
		IdleTimeout:       o.IdleTimeout,
	}
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

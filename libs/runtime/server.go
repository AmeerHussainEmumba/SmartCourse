package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// DefaultInternalAddr is the internal listener's default, METRICS_HTTP_ADDR.
const DefaultInternalAddr = ":9100"

// NewInternalServer is the INTERNAL listener every binary serves (design §6):
// GET /metrics (when metrics is non-nil), GET /health/live and GET /health/ready,
// and nothing else. It is unauthenticated by design (Prometheus and the
// orchestrator's probes carry no credentials), so it must be bound to a private
// interface or published on 127.0.0.1 only; it is never the public API port.
// Timeouts are set so a slow scraper cannot hold connections open.
func NewInternalServer(addr string, metrics http.Handler, health *Health) *http.Server {
	mux := http.NewServeMux()
	if metrics != nil {
		mux.Handle("GET /metrics", metrics)
	}
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"status": "live"})
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		rep := health.run(r.Context())
		body := map[string]any{"status": rep.status}
		if len(rep.checks) > 0 {
			body["checks"] = rep.checks
		}
		writeJSON(w, rep.code, body)
	})
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// ServeInternal runs srv until ctx is cancelled, then shuts it down. A listen
// failure is logged and ends only this call: telemetry never stops a service,
// which then runs without /metrics and probes.
func ServeInternal(ctx context.Context, logger *slog.Logger, srv *http.Server) {
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("internal listener up", slog.String("addr", srv.Addr))
	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("internal listener failed; the process runs without /metrics and health probes", slog.Any("error", err))
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}
}

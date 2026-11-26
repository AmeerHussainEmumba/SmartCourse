// Package runtime is the process bootstrap every SmartCourse binary shares
// (docs/plans/microservices-split.md §6), so each main.go stays a few lines of
// identical shape:
//
//	func main() {
//		runtime.Main("course-worker", func(p *runtime.Process) error {
//			// wire from p.Config, p.Deps, p.Logger, p.Obs; then register components
//			p.Go("relay", relay.Run)                          // independent: its failure is logged, the rest keep running
//			p.Go("activities", runActivities, runtime.Critical()) // its failure stops the process
//			p.Ready("kafka", kafka.HealthCheck(brokers), runtime.NonFatal())
//			return nil
//		})
//	}
//
// Main loads the configuration, builds the logger, connects the dependencies the
// binary asked for (WithDependencies: Postgres and Redis), starts telemetry (tracing, metrics), serves the INTERNAL listener
// (METRICS_HTTP_ADDR, default :9100: /metrics, /health/live, /health/ready), runs
// the components, and on SIGTERM or SIGINT shuts down gracefully within a bounded
// time and flushes telemetry.
//
// Independence, stated once. Components are plain func(ctx) error goroutines.
// A panic is recovered and counts as an error. A component that fails or ends
// before shutdown:
//
//   - non-critical (the default): is logged loudly, shown as a non-fatal entry of
//     /health/ready, and does NOT stop the others. It is not restarted: a
//     component that should retry owns its own loop.
//   - Critical(): cancels the process, which then shuts down and exits non-zero.
//
// Dependencies are optional and chosen by the binary, so a binary that never
// connects a database does not link a database driver:
//
//	runtime.Main("course-api", wire, runtime.WithDependencies(infra.Connect))
//
// Inside the wiring function p.Deps is that value as a Dependencies interface;
// infra.Of(p.Deps) gives the *infra.Dependencies back. This package imports
// libs/infra/observe (telemetry) but never libs/infra, so pgx, GORM and go-redis
// stay out of a binary that does not pass a constructor.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra/observe"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/logging"
)

// DefaultShutdownTimeout bounds the drain of components and shutdown hooks. It
// stays under the 30 s most orchestrators wait between SIGTERM and SIGKILL.
const DefaultShutdownTimeout = 25 * time.Second

// flushTimeout bounds the final telemetry flush: an unreachable collector costs
// at most this long at exit.
const flushTimeout = 5 * time.Second

// Process is what a binary's wiring function receives: the loaded
// configuration, the logger, the connected dependencies and telemetry, and the
// registration points for components, readiness checks and shutdown hooks.
type Process struct {
	// Name is the binary's name ("course-worker"), also its default service.name.
	Name   string
	Config *config.Config
	Logger *slog.Logger
	// Deps is what WithDependencies connected, nil when the binary has none.
	// infra.Of(Deps) is the *infra.Dependencies behind it.
	Deps Dependencies
	// Obs is never nil; it is inert when tracing and metrics are both off.
	Obs *observe.Observability
	// Health is the readiness state; Ready adds to it.
	Health *Health

	ctx   context.Context
	comps []component
	hooks []hook
}

type component struct {
	name     string
	fn       func(ctx context.Context) error
	critical bool
}

type hook struct {
	name string
	fn   func(ctx context.Context) error
}

// ComponentOption tunes Process.Go.
type ComponentOption func(*component)

// Critical makes the component's failure, or its ending before shutdown, stop
// the whole process.
func Critical() ComponentOption { return func(c *component) { c.critical = true } }

// Context is the process context: cancelled on SIGTERM/SIGINT or when a critical
// component fails. Components receive the same context.
func (p *Process) Context() context.Context { return p.ctx }

// Go registers a component. Components start together once the wiring function
// has returned without error, so a wiring failure never leaves a half-started
// process.
func (p *Process) Go(name string, fn func(ctx context.Context) error, opts ...ComponentOption) {
	c := component{name: name, fn: fn}
	for _, o := range opts {
		o(&c)
	}
	p.comps = append(p.comps, c)
}

// Ready registers a readiness check (see Health.Add).
func (p *Process) Ready(name string, check Check, opts ...CheckOption) {
	p.Health.Add(name, check, opts...)
}

// OnShutdown registers a cleanup run after every component has stopped, last
// registered first, each bounded by the shutdown timeout (close a producer, flush
// a buffer). Its error is logged and does not stop the other hooks.
func (p *Process) OnShutdown(name string, fn func(ctx context.Context) error) {
	p.hooks = append(p.hooks, hook{name: name, fn: fn})
}

// Option tunes Main and Run.
type Option func(*options)

type options struct {
	load            func() (*config.Config, error)
	logger          *slog.Logger
	connect         func(*config.Config) (Dependencies, error)
	outbox          bool
	workflows       bool
	shutdownTimeout time.Duration
}

// WithOutbox exports the outbox gauges (outbox_backlog and
// outbox_oldest_unpublished_seconds): set it for a binary that owns an outbox.
func WithOutbox() Option { return func(o *options) { o.outbox = true } }

// WithWorkflows keeps the workflow_* counters: set it for the binary that runs
// the Temporal workflow worker. Registering temporal.Observe (libs/temporal, not
// imported here so the Temporal SDK stays out of other binaries) is the
// binary's own call, after Main has built p.Obs.
func WithWorkflows() Option { return func(o *options) { o.workflows = true } }

// Dependencies is what Main needs of a binary's connected clients (Postgres and
// Redis for libs/infra's *Dependencies): their telemetry, their readiness checks
// and their release. It is an interface so this package links no database code.
type Dependencies interface {
	// Observe builds the binary's tracing and metrics, adding the database
	// series and client hooks of its own.
	Observe(ctx context.Context, cfg *config.Config, o observe.Options) *observe.Observability
	// ReadinessChecks are added to /health/ready, one per dependency.
	ReadinessChecks() map[string]func(context.Context) error
	// Close releases the connections after the components have stopped.
	Close()
}

// WithDependencies connects the binary's dependencies before wiring, normally
// WithDependencies(infra.Connect). A constructor error stops start-up. Without
// this option Process.Deps is nil, no database readiness check is added and no
// database code is linked.
func WithDependencies[D Dependencies](connect func(*config.Config) (D, error)) Option {
	return func(o *options) {
		o.connect = func(cfg *config.Config) (Dependencies, error) {
			d, err := connect(cfg)
			if err != nil {
				return nil, err
			}
			return d, nil
		}
	}
}

// WithShutdownTimeout overrides DefaultShutdownTimeout.
func WithShutdownTimeout(d time.Duration) Option { return func(o *options) { o.shutdownTimeout = d } }

// WithConfig replaces config.Load (tests, and a binary that loads a narrower
// configuration).
func WithConfig(load func() (*config.Config, error)) Option {
	return func(o *options) { o.load = load }
}

// WithLogger replaces the logger Main builds from the configuration.
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

// Main runs a binary and never returns: it exits 0 after a clean shutdown (a
// signal) and 1 on a configuration or start-up error, a critical component's
// failure, or a shutdown that did not finish within the timeout. A second signal
// during shutdown kills the process at once.
func Main(name string, wire func(*Process) error, opts ...Option) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := Run(ctx, name, wire, opts...)
	stop()
	if err != nil {
		slog.Error("exiting", slog.String("service", name), slog.Any("error", err))
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
	os.Exit(0)
}

// Run is Main without the signal handling and the exit: it returns when ctx is
// cancelled and shutdown has finished, or earlier on a start-up error or a
// critical component's failure, with the reason. Tests call it.
func Run(ctx context.Context, name string, wire func(*Process) error, opts ...Option) error {
	o := options{load: config.Load, shutdownTimeout: DefaultShutdownTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	cfg, err := o.load()
	if err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}
	logger := o.logger
	if logger == nil {
		logger = logging.New(cfg.Env, logging.LevelFromEnv())
	}
	logger.Info("starting", slog.String("service", name), slog.String("env", cfg.Env))

	var deps Dependencies
	if o.connect != nil {
		if deps, err = o.connect(cfg); err != nil {
			return fmt.Errorf("connect to dependencies: %w", err)
		}
		defer deps.Close()
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	obsOpts := observe.Options{Service: name, Outbox: o.outbox, Workflows: o.workflows}
	var obs *observe.Observability
	if deps != nil {
		obs = deps.Observe(runCtx, cfg, obsOpts)
	} else {
		obs = observe.New(runCtx, cfg, obsOpts)
	}

	p := &Process{Name: name, Config: cfg, Logger: logger, Deps: deps, Obs: obs, Health: NewHealth(), ctx: runCtx}
	if deps != nil {
		for dep, check := range deps.ReadinessChecks() {
			p.Health.Add(dep, Check(check))
		}
	}

	// The internal listener has its own lifetime, longer than the components':
	// probes and scrapes keep working while they drain.
	listenCtx, stopListener := context.WithCancel(context.WithoutCancel(ctx))
	var listener sync.WaitGroup
	listener.Add(1)
	go func() {
		defer listener.Done()
		ServeInternal(listenCtx, logger, NewInternalServer(cfg.Telemetry.MetricsAddr, obs.MetricsHandler(), p.Health))
	}()
	defer func() {
		stopListener()
		listener.Wait()
	}()
	defer func() {
		flush, stop := context.WithTimeout(context.Background(), flushTimeout)
		defer stop()
		obs.Shutdown(flush)
	}()

	if err := wire(p); err != nil {
		cancel(nil)
		p.runHooks(o.shutdownTimeout)
		return fmt.Errorf("start-up: %w", err)
	}

	var wg sync.WaitGroup
	for _, c := range p.comps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.supervise(c, cancel)
		}()
	}
	p.Health.Started()
	logger.Info("running", slog.String("service", name), slog.Int("components", len(p.comps)))

	<-runCtx.Done()
	cause := context.Cause(runCtx)
	p.Health.Stopping()
	logger.Info("shutting down", slog.String("service", name), slog.Any("reason", cause))

	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	var shutdownErr error
	select {
	case <-drained:
	case <-time.After(o.shutdownTimeout):
		shutdownErr = fmt.Errorf("shutdown did not finish within %s", o.shutdownTimeout)
		logger.Error("components still running at the shutdown deadline; exiting anyway", slog.Duration("timeout", o.shutdownTimeout))
	}
	p.runHooks(o.shutdownTimeout)
	logger.Info("stopped", slog.String("service", name))

	var failure *criticalError
	if errors.As(cause, &failure) {
		return errors.Join(failure, shutdownErr)
	}
	return shutdownErr
}

// criticalError is the cause a critical component's failure cancels the process
// with.
type criticalError struct {
	component string
	err       error
}

func (e *criticalError) Error() string {
	if e.err == nil {
		return fmt.Sprintf("critical component %s exited before shutdown", e.component)
	}
	return fmt.Sprintf("critical component %s failed: %v", e.component, e.err)
}

func (e *criticalError) Unwrap() error { return e.err }

// supervise runs one component with panic recovery and applies the independence
// rules.
func (p *Process) supervise(c component, cancel context.CancelCauseFunc) {
	err := safely(p.ctx, c.fn)
	if p.ctx.Err() != nil && (err == nil || errors.Is(err, context.Canceled)) {
		p.Logger.Info("component stopped", slog.String("component", c.name))
		return
	}
	switch {
	case c.critical:
		p.Logger.Error("critical component stopped; stopping the process",
			slog.String("component", c.name), slog.Any("error", err))
		cancel(&criticalError{component: c.name, err: err})
	case err != nil:
		p.Logger.Error("component failed; the other components keep running",
			slog.String("component", c.name), slog.Any("error", err))
		p.Health.setComponent(c.name, "failed")
	default:
		p.Logger.Warn("component exited before shutdown; the other components keep running", slog.String("component", c.name))
		p.Health.setComponent(c.name, "stopped")
	}
}

// safely runs fn and turns a panic into an error carrying the stack.
func safely(ctx context.Context, fn func(context.Context) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()
	return fn(ctx)
}

// runHooks runs the shutdown hooks, last registered first, each under the
// timeout, recovering panics.
func (p *Process) runHooks(timeout time.Duration) {
	for i := len(p.hooks) - 1; i >= 0; i-- {
		h := p.hooks[i]
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		if err := safely(ctx, h.fn); err != nil {
			p.Logger.Error("shutdown hook failed", slog.String("hook", h.name), slog.Any("error", err))
		}
		cancel()
	}
}

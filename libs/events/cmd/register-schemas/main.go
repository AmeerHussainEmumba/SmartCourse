// Command register-schemas registers every event subject with the Schema
// Registry at BACKWARD compatibility. It is idempotent: re-running with
// unchanged schemas changes nothing, and a schema that is incompatible with
// the registered one fails the run (RFC §6.8, ADR-0030).
//
//	register-schemas [-registry http://localhost:8081]
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
)

func main() {
	url := flag.String("registry", envOr("SCHEMA_REGISTRY_URL", "http://localhost:8081"), "Schema Registry base URL")
	timeout := flag.Duration("timeout", 2*time.Minute, "overall timeout")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	subjects, err := events.Subjects()
	if err != nil {
		fmt.Fprintln(os.Stderr, "register-schemas:", err)
		os.Exit(1)
	}
	if err := run(ctx, os.Stdout, registry.New(*url), subjects); err != nil {
		fmt.Fprintln(os.Stderr, "register-schemas:", err)
		os.Exit(1)
	}
}

// run registers subjects in order and stops at the first failure.
func run(ctx context.Context, out io.Writer, c *registry.Client, subjects []events.Subject) error {
	for _, s := range subjects {
		if err := c.SetCompatibility(ctx, s.Name, registry.Backward); err != nil {
			return fmt.Errorf("%s: set compatibility: %w", s.Name, err)
		}
		level, ok, err := c.Compatibility(ctx, s.Name)
		if err != nil {
			return fmt.Errorf("%s: read compatibility: %w", s.Name, err)
		}
		if !ok || level != registry.Backward {
			return fmt.Errorf("%s: compatibility is %q after setting %s", s.Name, level, registry.Backward)
		}
		compatible, err := c.IsCompatible(ctx, s.Name, s.Schema)
		if err != nil {
			return fmt.Errorf("%s: compatibility check: %w", s.Name, err)
		}
		if !compatible {
			return fmt.Errorf("%s: schema is not %s-compatible with the registered version; make an additive change or review ADR-0008", s.Name, registry.Backward)
		}
		id, err := c.Register(ctx, s.Name, s.Schema)
		if err != nil {
			return fmt.Errorf("%s: register: %w", s.Name, err)
		}
		version, err := c.LatestVersion(ctx, s.Name)
		if err != nil {
			return fmt.Errorf("%s: read version: %w", s.Name, err)
		}
		if _, err := fmt.Fprintf(out, "%-45s id=%d version=%d %s\n", s.Name, id, version, registry.Backward); err != nil {
			return err
		}
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

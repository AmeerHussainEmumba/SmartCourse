// Package temporal builds the Temporal client. It is its own package, apart
// from libs/infra, so that identity-api and analytics-api —
// which never touch Temporal (ADR-0009: Temporal backs publishing and
// nothing else) — don't link the Temporal SDK. Only course-api, course-worker
// and publishing-worker (under services/) import it.
package temporal

import (
	"fmt"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

// Connect builds a Temporal client for course-api (to start the publish
// workflow) and worker (to run it). client.NewLazyClient, not client.Dial:
// Dial eagerly contacts the server and fails if it's unreachable, which
// would make course-api's boot depend on Temporal for catalogue/detail/CRUD
// routes that have nothing to do with it. A lazy client defers that
// connection to the first actual publish, matching ADR-0009's own stated
// consequence — "Temporal down means publishing unavailable — though
// nothing else is affected."
func Connect(cfg *config.Config) (temporalclient.Client, error) {
	ints, err := interceptors() // none unless Observe was called (observability.go)
	if err != nil {
		return nil, err
	}
	c, err := temporalclient.NewLazyClient(temporalclient.Options{
		HostPort:     cfg.Temporal.HostPort,
		Namespace:    cfg.Temporal.Namespace,
		Interceptors: ints,
	})
	if err != nil {
		return nil, fmt.Errorf("build temporal client: %w", err)
	}
	return c, nil
}

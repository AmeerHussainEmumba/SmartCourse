package main

import (
	"github.com/gin-gonic/gin"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

// registerRoutes wires services/identity/identity's HTTP routes onto r. Fully
// self-contained — no other module reads identity's tables directly;
// everyone else gets the caller's identity from the JWT claims
// (auth.FromContext), never a lookup into `users`. See ADR-0023 for why
// identity split out on its own first.
func registerRoutes(r *gin.Engine, deps *infra.Dependencies, cfg *config.Config) {
	api := r.Group("/api/v1")

	identityRepo := identity.NewRepository(deps.DB)
	identitySvc := identity.NewService(identityRepo, deps.RDB, cfg.JWT, cfg.Argon, identity.LoggingEmailSender{})
	identity.RegisterRoutes(api, identitySvc, cfg.JWT, deps.RDB)
}

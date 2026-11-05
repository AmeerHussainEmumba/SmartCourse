// Domain logic for analytics (RFC §4.3). Thin today — one read — but the
// layering exists so this module follows the same handler → service →
// repository shape as every other module, rather than the handler holding
// a *gorm.DB directly. That inconsistency was flagged in review: it's an
// easy pattern to copy by accident if left uncorrected, and the whole
// point of a uniform module shape (RFC §4.3) is that a contributor who
// knows one module already knows the rest.
package analytics

import "context"

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) GetPlatformSnapshot(ctx context.Context) (*PlatformSnapshot, error) {
	return s.repo.GetPlatformSnapshot(ctx)
}

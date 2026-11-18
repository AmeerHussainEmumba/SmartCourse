//go:build integration

package securitytest_test

import (
	"context"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// stubAssetStore stands in for object storage where a test is about routing
// and authorisation, not storage behaviour (assets_integration_test.go uses a
// real MinIO for that).
type stubAssetStore struct{}

func (stubAssetStore) PresignUpload(_ context.Context, key, contentType string, _ int64, expires time.Duration) (course.PresignedUpload, error) {
	return course.PresignedUpload{
		URL:       "http://storage.invalid/" + key,
		Headers:   map[string]string{"Content-Type": contentType},
		ExpiresAt: time.Now().Add(expires),
	}, nil
}

func (stubAssetStore) Stat(context.Context, string) (course.AssetInfo, error) {
	return course.AssetInfo{}, course.ErrAssetNotFound
}

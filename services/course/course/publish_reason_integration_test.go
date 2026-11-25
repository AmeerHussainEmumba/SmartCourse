//go:build integration

package course_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// S-08 (was TestBugrepro_VerifyAssets_StorageErrorTextLeaksIntoPublishReason):
// VerifyAssets' storage failure used to carry the raw SDK/driver error text
// (internal host, port, bucket path) in the error's Error() string; the publish
// workflow stored that text as course_versions.publish_error and in
// course.publish_failed's reason, which the worker emails to the instructor and
// analytics keeps. The error an activity returns now carries a fixed category
// only, and the full error is logged.
type failingStat struct{ err error }

func (f failingStat) PresignUpload(context.Context, string, string, int64, time.Duration) (course.PresignedUpload, error) {
	return course.PresignedUpload{}, f.err
}
func (f failingStat) Stat(context.Context, string) (course.AssetInfo, error) {
	return course.AssetInfo{}, f.err
}

func TestVerifyAssets_StorageErrorTextDoesNotLeakIntoPublishReason(t *testing.T) {
	w := newAssetWorld(t)
	key := "courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".pdf"
	w.lesson(t, 1, &key)
	internal := errors.New(`operation error S3: HeadObject, https response error StatusCode: 0, request send failed, Head "http://minio.internal.svc:9000/smartcourse-assets/` + key + `": dial tcp 10.42.7.19:9000: connect: connection refused`)
	svc := w.svc.WithAssetStore(failingStat{internal})
	err := svc.VerifyAssets(context.Background(), w.draftID)
	if err == nil {
		t.Fatal("want an error")
	}
	// This string is what Temporal's ApplicationError.Message() used to carry to failPublish.
	for _, leak := range []string{"10.42.7.19", "minio.internal", "9000", "smartcourse-assets", "HeadObject", "dial tcp", key} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("the activity error leaks %q: %q", leak, err.Error())
		}
	}
	if got, want := course.PublishFailureText(err), "temporarily_unavailable: storage unavailable"; got != want {
		t.Errorf("PublishFailureText = %q, want %q", got, want)
	}
}

// Every VerifyAssets failure category maps to a fixed, generic reason, and a
// lesson title (instructor-controlled) never appears in it.
func TestPublishFailureText_VerifyAssetsCategories(t *testing.T) {
	w := newAssetWorld(t)
	ctx := context.Background()

	missing := "courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".pdf"
	w.lesson(t, 1, &missing)
	err := w.svc.VerifyAssets(ctx, w.draftID)
	if got, want := course.PublishFailureText(err), "asset_invalid: asset not found"; got != want {
		t.Errorf("missing asset: %q, want %q", got, want)
	}
	if strings.Contains(err.Error(), "never uploaded") || strings.Contains(err.Error(), "Lx") {
		t.Errorf("the error carries a lesson title or prose: %q", err.Error())
	}
}

// An error that is not categorised (a database failure, say) is reported as
// internal_error, never as its own text.
func TestPublishFailureText_UncategorisedErrorsAreGeneric(t *testing.T) {
	for _, err := range []error{
		errors.New(`pq: connection to 10.1.2.3:5432 refused`),
		nil,
	} {
		if got := course.PublishFailureText(err); got != "internal_error" {
			t.Errorf("PublishFailureText(%v) = %q, want internal_error", err, got)
		}
	}
}

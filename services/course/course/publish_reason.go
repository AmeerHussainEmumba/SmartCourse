package course

import (
	"errors"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/contracts/publishing"
	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
)

// PublishFailureType is the Temporal ApplicationError type that marks a
// publish activity failure whose message is already a PublishFailureText: a
// bounded, fixed-vocabulary string that may be stored, emitted in
// course.publish_failed and emailed to the instructor. failPublish trusts no
// other message (S-08).
const PublishFailureType = publishing.FailureType

// publishReason is the fixed vocabulary of publish failures. A raw error text
// (a storage SDK message with a host, port and bucket path, a driver error, a
// lesson title) is never part of what an instructor reads: the publish-failure
// reason carries one of these categories, and the full error goes to the
// server log (see logPublishFailure).
type publishReason string

func (r publishReason) Error() string { return string(r) }

const (
	reasonTitleRequired      publishReason = "title is required"
	reasonNoModules          publishReason = "at least one module is required"
	reasonNoLessons          publishReason = "at least one lesson is required"
	reasonContentTooLarge    publishReason = "lesson content too large"
	reasonAssetNotFound      publishReason = "asset not found"
	reasonAssetEmpty         publishReason = "asset is empty"
	reasonAssetTooLarge      publishReason = "asset too large"
	reasonAssetWrongType     publishReason = "asset has the wrong content type"
	reasonAssetKeyInvalid    publishReason = "asset reference is not valid for this course"
	reasonStorageUnavailable publishReason = "storage unavailable"
	reasonStorageNotSetUp    publishReason = "storage not configured"
)

// PublishFailureText maps an activity error to the only text that may leave
// the service for it: "<code>: <category>" for a categorised failure (for
// example "asset_invalid: asset not found"), the bare stable error code for an
// uncategorised *AppError, and "internal_error" for anything else. It never
// returns any part of err's own message, so it is safe for
// course_versions.publish_error, the course.publish_failed reason and the
// instructor's email. The length is bounded by the vocabulary.
func PublishFailureText(err error) string {
	code := apperrors.ErrInternal.Code
	if ae, ok := apperrors.As(err); ok {
		code = ae.Code
	}
	var r publishReason
	if errors.As(err, &r) {
		return code + ": " + string(r)
	}
	return code
}

package course

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
)

// Asset upload and verification (RFC §7.4, §11.7, ADR-0019). The API never
// proxies asset bytes: it hands the instructor a short-lived pre-signed PUT
// URL for a key the SERVER generated, and the publish workflow's VerifyAssets
// later confirms each referenced key really exists in object storage with an
// allowed type and size. This package defines only the storage seam
// (AssetStore) so it stays free of an S3 SDK import, the same way it stays free
// of the Temporal SDK; services/course/assets implements it.

// uploadURLExpiry is how long a pre-signed upload URL stays valid (RFC §11.7:
// 15 minutes).
const uploadURLExpiry = 15 * time.Minute

// ErrAssetNotFound is what AssetStore.Stat returns when the object does not
// exist.
var ErrAssetNotFound = errors.New("asset not found")

// AssetInfo is what VerifyAssets needs to know about a stored object.
type AssetInfo struct {
	Size        int64
	ContentType string
}

// PresignedUpload is a pre-signed PUT request. The client must send Headers
// verbatim (they are part of the signature) and a body of exactly the size it
// declared.
type PresignedUpload struct {
	URL       string
	Headers   map[string]string
	ExpiresAt time.Time
}

// AssetStore is the object-storage seam. Implementations must sign the
// content type and content length into the URL so storage itself rejects an
// upload that differs from what was authorised.
type AssetStore interface {
	PresignUpload(ctx context.Context, key, contentType string, size int64, expires time.Duration) (PresignedUpload, error)
	// Stat returns ErrAssetNotFound (possibly wrapped) when the key does not exist.
	Stat(ctx context.Context, key string) (AssetInfo, error)
}

// WithAssetStore attaches object storage to the service. Without one,
// POST /assets/upload-url answers 503 and VerifyAssets fails any lesson that
// references an asset (it never silently passes an unverifiable asset).
func (s *Service) WithAssetStore(store AssetStore) *Service {
	s.assets = store
	return s
}

// assetType is one entry of the content-type allowlist.
type assetType struct {
	ext     string
	maxSize int64
}

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
)

// assetTypes is the allowlist: nothing else can be uploaded or published.
// Limits are per type; a single pre-signed PUT can carry at most 5 GiB.
var assetTypes = map[string]assetType{
	"video/mp4":       {ext: "mp4", maxSize: 2 * gib},
	"video/webm":      {ext: "webm", maxSize: 2 * gib},
	"application/pdf": {ext: "pdf", maxSize: 100 * mib},
	"image/png":       {ext: "png", maxSize: 10 * mib},
	"image/jpeg":      {ext: "jpg", maxSize: 10 * mib},
}

// assetTypeByExt is the reverse lookup used to check a stored object's type
// against the extension its server-generated key carries.
func assetTypeByExt(ext string) (string, assetType, bool) {
	for ct, t := range assetTypes {
		if t.ext == ext {
			return ct, t, true
		}
	}
	return "", assetType{}, false
}

// assetKeyPattern is the only shape of key the server ever generates:
//
//	courses/{course_id}/assets/{asset_id}.{ext}
//	courses/{course_id}/lessons/{lesson_id}/{asset_id}.{ext}
//
// Lowercase hex UUIDs only, no client-supplied segment, so a key can never
// contain a path traversal, another course's prefix or a surprise character.
var assetKeyPattern = regexp.MustCompile(`^courses/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/(?:assets|lessons/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.([a-z0-9]+)$`)

// parseAssetKey validates the key's shape and returns the course it is scoped
// to and its extension.
func parseAssetKey(key string) (courseID uuid.UUID, ext string, ok bool) {
	m := assetKeyPattern.FindStringSubmatch(key)
	if m == nil {
		return uuid.Nil, "", false
	}
	id, err := uuid.Parse(m[1])
	if err != nil {
		return uuid.Nil, "", false
	}
	return id, m[2], true
}

var (
	errUnsupportedAssetType = apperrors.New(http.StatusBadRequest, "unsupported_content_type", "This content type is not allowed for asset uploads.")
	errAssetTooLarge        = apperrors.New(http.StatusRequestEntityTooLarge, "asset_too_large", "The asset exceeds the maximum size allowed for its type.")
	errAssetStoreOff        = apperrors.New(http.StatusServiceUnavailable, "assets_unavailable", "Asset storage is not available right now.")
	errAssetInvalid         = apperrors.New(http.StatusBadRequest, "asset_invalid", "A lesson references an asset that is missing or not allowed.")
	errAssetKeyInvalid      = apperrors.New(http.StatusBadRequest, "invalid_asset_key", "asset_key must be a key issued by POST /assets/upload-url for this course.")
)

// CreateUploadURL backs POST /assets/upload-url. The instructor must own the
// course (a non-owner gets 404, like every other course route), the course
// must have a draft, and an optional lesson must belong to that draft. The
// object key is generated here from the course (and lesson) and a fresh UUID;
// nothing the client sends ends up in it.
func (s *Service) CreateUploadURL(ctx context.Context, instructorID uuid.UUID, req UploadURLRequest) (*UploadURLResponse, error) {
	if err := rejectNUL(req.ContentType); err != nil {
		return nil, err
	}
	courseID, err := uuid.Parse(req.CourseID)
	if err != nil {
		return nil, apperrors.ErrValidation.Wrap(err)
	}
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	if c.ArchivedAt != nil {
		return nil, errCourseArchived
	}
	if c.DraftVersionID == nil {
		return nil, errNoDraft
	}

	contentType := strings.ToLower(strings.TrimSpace(req.ContentType))
	at, ok := assetTypes[contentType]
	if !ok {
		return nil, errUnsupportedAssetType
	}
	if req.SizeBytes < 1 {
		return nil, apperrors.ErrValidation.Wrap(fmt.Errorf("size_bytes must be positive"))
	}
	if req.SizeBytes > at.maxSize {
		return nil, errAssetTooLarge
	}

	prefix := fmt.Sprintf("courses/%s/assets", courseID)
	if req.LessonID != nil {
		lessonID, err := uuid.Parse(*req.LessonID)
		if err != nil {
			return nil, apperrors.ErrValidation.Wrap(err)
		}
		lesson, err := s.repo.GetLessonByID(ctx, lessonID)
		if errors.Is(err, ErrNotFound) {
			return nil, apperrors.ErrNotFound
		}
		if err != nil {
			return nil, apperrors.ErrInternal.Wrap(err)
		}
		mod, err := s.repo.GetModuleByID(ctx, lesson.ModuleID)
		if errors.Is(err, ErrNotFound) {
			return nil, apperrors.ErrNotFound
		}
		if err != nil {
			return nil, apperrors.ErrInternal.Wrap(err)
		}
		if mod.CourseVersionID != *c.DraftVersionID {
			return nil, apperrors.ErrNotFound // a lesson of another course or a non-draft version looks like no lesson at all
		}
		prefix = fmt.Sprintf("courses/%s/lessons/%s", courseID, lessonID)
	}

	if s.assets == nil {
		return nil, errAssetStoreOff
	}
	key := fmt.Sprintf("%s/%s.%s", prefix, uuid.New(), at.ext)
	up, err := s.assets.PresignUpload(ctx, key, contentType, req.SizeBytes, uploadURLExpiry)
	if err != nil {
		return nil, apperrors.ErrUnavailable.Wrap(fmt.Errorf("presign upload: %w", err))
	}
	return &UploadURLResponse{
		UploadURL: up.URL, Method: http.MethodPut, Headers: up.Headers,
		AssetKey: key, ExpiresAt: up.ExpiresAt, MaxSizeBytes: at.maxSize,
	}, nil
}

// validateAssetKeyForCourse is the write-time check for a lesson's asset_key:
// it must be a key this service could have issued, scoped to this course.
func validateAssetKeyForCourse(key string, courseID uuid.UUID) error {
	keyCourse, ext, ok := parseAssetKey(key)
	if !ok || keyCourse != courseID {
		return errAssetKeyInvalid
	}
	if _, _, known := assetTypeByExt(ext); !known {
		return errAssetKeyInvalid
	}
	return nil
}

// verifyAssetConcurrency bounds the StatObject calls of one VerifyAssets run
// (RFC §7.4: "bounded concurrency").
const verifyAssetConcurrency = 8

// verifyLessonAssets checks every asset key the draft references, once per key.
func (s *Service) verifyLessonAssets(ctx context.Context, courseID uuid.UUID, refs map[string]string) error {
	if len(refs) == 0 {
		return nil
	}
	if s.assets == nil {
		slog.ErrorContext(ctx, "publish: lesson assets cannot be verified, no asset store configured", slog.Int("assets", len(refs)))
		return errAssetStoreOff.Wrap(reasonStorageNotSetUp)
	}
	// Shape and scope first: cheap, deterministic, and never touches storage
	// for a key that could not have been issued for this course.
	for key, lesson := range refs {
		if err := validateAssetKeyForCourse(key, courseID); err != nil {
			slog.WarnContext(ctx, "publish: asset key is not valid for this course", slog.String("lesson", lesson))
			return errAssetInvalid.Wrap(reasonAssetKeyInvalid)
		}
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(verifyAssetConcurrency)
	for key, lesson := range refs {
		g.Go(func() error { return s.verifyAsset(gctx, key, lesson) })
	}
	return g.Wait()
}

func (s *Service) verifyAsset(ctx context.Context, key, lesson string) error {
	_, ext, _ := parseAssetKey(key)
	wantType, at, _ := assetTypeByExt(ext)
	info, err := s.assets.Stat(ctx, key)
	if errors.Is(err, ErrAssetNotFound) {
		slog.WarnContext(ctx, "publish: the referenced asset was never uploaded", slog.String("lesson", lesson), slog.String("asset_key", key))
		return errAssetInvalid.Wrap(reasonAssetNotFound)
	}
	if err != nil {
		// Storage trouble is not the instructor's fault: surface it as a
		// transient error so Temporal retries the activity. The SDK error
		// (host, port, bucket path) is logged here and never carried in the
		// returned error: its text becomes the instructor-visible publish
		// reason (S-08).
		slog.ErrorContext(ctx, "publish: asset storage stat failed",
			slog.String("lesson", lesson), slog.String("asset_key", key), slog.Any("error", err))
		return apperrors.ErrUnavailable.Wrap(reasonStorageUnavailable)
	}
	if info.Size <= 0 {
		slog.WarnContext(ctx, "publish: the uploaded asset is empty", slog.String("lesson", lesson), slog.String("asset_key", key))
		return errAssetInvalid.Wrap(reasonAssetEmpty)
	}
	if info.Size > at.maxSize {
		slog.WarnContext(ctx, "publish: the uploaded asset is too large", slog.String("lesson", lesson), slog.String("asset_key", key), slog.Int64("size", info.Size), slog.Int64("max", at.maxSize))
		return errAssetInvalid.Wrap(reasonAssetTooLarge)
	}
	if got := strings.ToLower(strings.TrimSpace(info.ContentType)); got != wantType {
		slog.WarnContext(ctx, "publish: the uploaded asset has the wrong content type", slog.String("lesson", lesson), slog.String("asset_key", key), slog.String("got", info.ContentType), slog.String("want", wantType))
		return errAssetInvalid.Wrap(reasonAssetWrongType)
	}
	return nil
}

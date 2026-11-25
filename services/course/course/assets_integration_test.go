//go:build integration

package course_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/testenv"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/assets"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/internal/miniotest"
)

// What this file checks, against a real MinIO (RFC §7.4, §11.7, ADR-0019):
// upload-URL issuance rules, write-time validation of a lesson's asset_key,
// and VerifyAssets with present, missing, wrong-size, wrong-type and
// foreign-course assets.

type assetWorld struct {
	env      *testenv.Env
	minio    *miniotest.Env
	svc      *course.Service
	instr    uuid.UUID
	courseID uuid.UUID
	moduleID uuid.UUID
	draftID  uuid.UUID
}

func newAssetWorld(t *testing.T) *assetWorld {
	t.Helper()
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	m := miniotest.Setup(t)
	store, err := assets.New(m.Config)
	if err != nil {
		t.Fatal(err)
	}
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "").WithAssetStore(store)
	instr := seedInstructor(t, env)
	courseID, draftID := newCourse(t, svc, instr, "assets-"+uuid.NewString()[:8], "Assets")
	mod, err := svc.AddModule(context.Background(), courseID, instr, course.CreateModuleRequest{Title: "M", Position: 1})
	if err != nil {
		t.Fatal(err)
	}
	return &assetWorld{env: env, minio: m, svc: svc, instr: instr, courseID: courseID, moduleID: uuid.MustParse(mod.ID), draftID: draftID}
}

// lesson adds a lesson referencing key (nil: no asset) and returns its ID.
func (w *assetWorld) lesson(t *testing.T, pos int, key *string) uuid.UUID {
	t.Helper()
	l, err := w.svc.AddLesson(context.Background(), w.courseID, w.instr, w.moduleID, course.CreateLessonRequest{
		Title: "L" + strings.Repeat("x", pos), Position: pos, ContentType: "pdf", Content: map[string]any{}, AssetKey: key, DurationS: 1,
	})
	if err != nil {
		t.Fatalf("AddLesson: %v", err)
	}
	return uuid.MustParse(l.ID)
}

// uploaded issues an upload URL and performs the upload; returns the key.
func (w *assetWorld) uploaded(t *testing.T, contentType string, body []byte) string {
	t.Helper()
	resp, err := w.svc.CreateUploadURL(context.Background(), w.instr, course.UploadURLRequest{
		CourseID: w.courseID.String(), ContentType: contentType, SizeBytes: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("CreateUploadURL: %v", err)
	}
	if got := miniotest.Upload(t, resp.UploadURL, resp.Headers, body); got != 200 {
		t.Fatalf("upload status %d", got)
	}
	return resp.AssetKey
}

func isAppErr(err error, status int, code string) bool {
	ae, ok := apperrors.As(err)
	return ok && ae.HTTPStatus == status && ae.Code == code
}

func TestVerifyAssets_LessonsWithoutAssetsPass_EvenWithNoStore(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	instr := seedInstructor(t, env)
	courseID, draftID := newCourse(t, svc, instr, "noassets", "No assets")
	authorDraft(t, svc, courseID, instr, 1, 2)
	if err := svc.VerifyAssets(context.Background(), draftID); err != nil {
		t.Fatalf("VerifyAssets: %v", err)
	}
}

func TestVerifyAssets_PresentAssetPasses(t *testing.T) {
	w := newAssetWorld(t)
	key := w.uploaded(t, "application/pdf", []byte("%PDF-1.7 content"))
	w.lesson(t, 1, &key)
	if err := w.svc.VerifyAssets(context.Background(), w.draftID); err != nil {
		t.Fatalf("VerifyAssets with a present asset: %v", err)
	}
}

func TestVerifyAssets_MissingAssetFails(t *testing.T) {
	w := newAssetWorld(t)
	// A well-formed key this service could have issued, but nothing uploaded.
	key := "courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".pdf"
	w.lesson(t, 1, &key)
	err := w.svc.VerifyAssets(context.Background(), w.draftID)
	if !isAppErr(err, 400, "asset_invalid") || !strings.Contains(err.Error(), "asset not found") {
		t.Fatalf("err = %v, want asset_invalid (never uploaded)", err)
	}
}

func TestVerifyAssets_EmptyObjectFails(t *testing.T) {
	w := newAssetWorld(t)
	key := "courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".pdf"
	w.minio.Put(t, key, "application/pdf", nil)
	w.lesson(t, 1, &key)
	if err := w.svc.VerifyAssets(context.Background(), w.draftID); !isAppErr(err, 400, "asset_invalid") || !strings.Contains(err.Error(), "asset is empty") {
		t.Fatalf("err = %v, want asset_invalid (empty)", err)
	}
}

func TestVerifyAssets_OversizeObjectFails(t *testing.T) {
	w := newAssetWorld(t)
	// .png is capped at 10 MiB; write the object directly, past the pre-signed path.
	key := "courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".png"
	w.minio.Put(t, key, "image/png", make([]byte, 10*1024*1024+1))
	w.lesson(t, 1, &key)
	if err := w.svc.VerifyAssets(context.Background(), w.draftID); !isAppErr(err, 400, "asset_invalid") || !strings.Contains(err.Error(), "asset too large") {
		t.Fatalf("err = %v, want asset_invalid (too large)", err)
	}
}

func TestVerifyAssets_WrongContentTypeFails(t *testing.T) {
	w := newAssetWorld(t)
	key := "courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".pdf"
	w.minio.Put(t, key, "text/html", []byte("<script>alert(1)</script>"))
	w.lesson(t, 1, &key)
	if err := w.svc.VerifyAssets(context.Background(), w.draftID); !isAppErr(err, 400, "asset_invalid") || !strings.Contains(err.Error(), "asset has the wrong content type") {
		t.Fatalf("err = %v, want asset_invalid (content type)", err)
	}
}

// A lesson cannot reference an object that belongs to another course, even if
// it exists and is well-formed; the write-time check rejects it, and VerifyAssets
// still catches one that got into the database some other way.
func TestAssetKey_ForeignCourseRejectedAtWriteAndAtVerify(t *testing.T) {
	w := newAssetWorld(t)
	foreign := "courses/" + uuid.NewString() + "/assets/" + uuid.NewString() + ".pdf"
	w.minio.Put(t, foreign, "application/pdf", []byte("%PDF"))

	_, err := w.svc.AddLesson(context.Background(), w.courseID, w.instr, w.moduleID, course.CreateLessonRequest{
		Title: "F", Position: 1, ContentType: "pdf", Content: map[string]any{}, AssetKey: &foreign, DurationS: 1,
	})
	if !isAppErr(err, 400, "invalid_asset_key") {
		t.Fatalf("AddLesson with a foreign key: %v, want 400 invalid_asset_key", err)
	}

	id := w.lesson(t, 1, nil)
	if err := w.env.DB.Exec(`UPDATE lessons SET asset_key = ? WHERE id = ?`, foreign, id).Error; err != nil {
		t.Fatal(err)
	}
	if err := w.svc.VerifyAssets(context.Background(), w.draftID); !isAppErr(err, 400, "asset_invalid") {
		t.Fatalf("VerifyAssets with a foreign key: %v, want asset_invalid", err)
	}
}

func TestAssetKey_TraversalAndMalformedKeysRejectedAtWrite(t *testing.T) {
	w := newAssetWorld(t)
	good := "courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".pdf"
	bad := []string{
		"../../etc/passwd",
		"courses/" + w.courseID.String() + "/../" + uuid.NewString() + "/assets/" + uuid.NewString() + ".pdf",
		"courses/" + w.courseID.String() + "/assets/../../x.pdf",
		"/courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".pdf",
		good + "/",
		good + "\n",
		good + "?x=1",
		strings.ToUpper(good),
		"courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".exe",
		"courses/" + w.courseID.String() + "/assets/not-a-uuid.pdf",
		"s3://other-bucket/x.pdf",
	}
	for i, k := range bad {
		k := k
		_, err := w.svc.AddLesson(context.Background(), w.courseID, w.instr, w.moduleID, course.CreateLessonRequest{
			Title: "B", Position: i + 1, ContentType: "pdf", Content: map[string]any{}, AssetKey: &k, DurationS: 1,
		})
		if !isAppErr(err, 400, "invalid_asset_key") {
			t.Errorf("asset_key %q accepted or wrong error: %v", k, err)
		}
	}
	// The issued shapes (course-level and lesson-level) are accepted.
	lessonLevel := "courses/" + w.courseID.String() + "/lessons/" + uuid.NewString() + "/" + uuid.NewString() + ".mp4"
	for i, k := range []string{good, lessonLevel} {
		k := k
		if _, err := w.svc.AddLesson(context.Background(), w.courseID, w.instr, w.moduleID, course.CreateLessonRequest{
			Title: "G", Position: 100 + i, ContentType: "pdf", Content: map[string]any{}, AssetKey: &k, DurationS: 1,
		}); err != nil {
			t.Errorf("valid key %q rejected: %v", k, err)
		}
	}
}

// With a lesson that references an asset but no storage configured, VerifyAssets
// fails closed instead of waving the asset through.
func TestVerifyAssets_AssetReferencedButNoStore_FailsClosed(t *testing.T) {
	env := testenv.Setup(t)
	env.DB = env.ServiceDB(t, "course")
	svc := course.NewService(course.NewRepository(env.DB), env.RDB, nil, "")
	instr := seedInstructor(t, env)
	courseID, draftID := newCourse(t, svc, instr, "nostore", "No store")
	mod, err := svc.AddModule(context.Background(), courseID, instr, course.CreateModuleRequest{Title: "M", Position: 1})
	if err != nil {
		t.Fatal(err)
	}
	key := "courses/" + courseID.String() + "/assets/" + uuid.NewString() + ".pdf"
	if _, err := svc.AddLesson(context.Background(), courseID, instr, uuid.MustParse(mod.ID), course.CreateLessonRequest{
		Title: "L", Position: 1, ContentType: "pdf", Content: map[string]any{}, AssetKey: &key, DurationS: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := svc.VerifyAssets(context.Background(), draftID); !isAppErr(err, 503, "assets_unavailable") {
		t.Fatalf("err = %v, want 503 assets_unavailable", err)
	}
}

// Storage being unreachable is a transient error (Temporal retries it), not
// the instructor's asset being "invalid".
func TestVerifyAssets_StorageUnreachable_IsTransient(t *testing.T) {
	w := newAssetWorld(t)
	key := "courses/" + w.courseID.String() + "/assets/" + uuid.NewString() + ".pdf"
	w.lesson(t, 1, &key)
	cfg := w.minio.Config
	cfg.Endpoint = "http://127.0.0.1:1"
	dead, err := assets.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	svc := course.NewService(course.NewRepository(w.env.DB), w.env.RDB, nil, "").WithAssetStore(dead)
	if err := svc.VerifyAssets(context.Background(), w.draftID); !isAppErr(err, 503, "temporarily_unavailable") {
		t.Fatalf("err = %v, want 503 temporarily_unavailable", err)
	}
}

func TestCreateUploadURL_RulesAndKeyScheme(t *testing.T) {
	w := newAssetWorld(t)
	ctx := context.Background()
	lessonID := w.lesson(t, 1, nil)
	req := func(ct string, size int64) course.UploadURLRequest {
		return course.UploadURLRequest{CourseID: w.courseID.String(), ContentType: ct, SizeBytes: size}
	}

	// Allowed type, course scope: the key is server-generated and scoped.
	resp, err := w.svc.CreateUploadURL(ctx, w.instr, req("application/pdf", 1234))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(resp.AssetKey, "courses/"+w.courseID.String()+"/assets/") || !strings.HasSuffix(resp.AssetKey, ".pdf") {
		t.Fatalf("key = %q", resp.AssetKey)
	}
	if resp.Method != "PUT" || resp.Headers["Content-Type"] != "application/pdf" || resp.MaxSizeBytes != 100*1024*1024 {
		t.Fatalf("resp = %+v", resp)
	}
	// RFC §11.7: expires in 15 minutes, both as reported and as signed.
	if d := time.Until(resp.ExpiresAt); d < 14*time.Minute || d > 15*time.Minute {
		t.Fatalf("expires in %v, want about 15 minutes", d)
	}
	if !strings.Contains(resp.UploadURL, "X-Amz-Expires=900") {
		t.Fatalf("signed expiry is not 900s: %s", resp.UploadURL)
	}

	// Lesson scope.
	lr := req("video/mp4", 10)
	l := lessonID.String()
	lr.LessonID = &l
	resp, err = w.svc.CreateUploadURL(ctx, w.instr, lr)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(resp.AssetKey, "courses/"+w.courseID.String()+"/lessons/"+l+"/") || !strings.HasSuffix(resp.AssetKey, ".mp4") {
		t.Fatalf("lesson key = %q", resp.AssetKey)
	}

	// Two calls never share a key.
	a, _ := w.svc.CreateUploadURL(ctx, w.instr, req("image/png", 5))
	b, _ := w.svc.CreateUploadURL(ctx, w.instr, req("image/png", 5))
	if a.AssetKey == b.AssetKey {
		t.Fatal("two upload URLs share a key")
	}

	// Disallowed types, case/whitespace tricks that are not on the list.
	for _, ct := range []string{"text/html", "application/x-msdownload", "application/pdf; charset=binary", "application/pdf/../x", "video/mp4 x", "", "image/svg+xml", "application/octet-stream"} {
		if _, err := w.svc.CreateUploadURL(ctx, w.instr, req(ct, 10)); err == nil || !(isAppErr(err, 400, "unsupported_content_type") || isAppErr(err, 400, "validation_failed")) {
			t.Errorf("content type %q: err = %v, want a 400 rejection", ct, err)
		}
	}
	// An allowed type spelled in upper case is normalised, not mis-keyed.
	if r, err := w.svc.CreateUploadURL(ctx, w.instr, req(" APPLICATION/PDF ", 10)); err != nil || r.Headers["Content-Type"] != "application/pdf" {
		t.Errorf("normalised type: %+v %v", r, err)
	}

	// Size limits: per type maximum, zero and negative rejected.
	if _, err := w.svc.CreateUploadURL(ctx, w.instr, req("image/png", 10*1024*1024+1)); !isAppErr(err, 413, "asset_too_large") {
		t.Errorf("oversize png: %v", err)
	}
	if _, err := w.svc.CreateUploadURL(ctx, w.instr, req("image/png", 10*1024*1024)); err != nil {
		t.Errorf("png at the limit rejected: %v", err)
	}
	for _, sz := range []int64{0, -1} {
		if _, err := w.svc.CreateUploadURL(ctx, w.instr, req("image/png", sz)); !isAppErr(err, 400, "validation_failed") {
			t.Errorf("size %d: %v", sz, err)
		}
	}
}

func TestCreateUploadURL_OwnershipLessonScopeAndLifecycle(t *testing.T) {
	w := newAssetWorld(t)
	ctx := context.Background()
	other := seedInstructor(t, w.env)
	req := course.UploadURLRequest{CourseID: w.courseID.String(), ContentType: "application/pdf", SizeBytes: 10}

	// Another instructor: 404, like every other course route.
	if _, err := w.svc.CreateUploadURL(ctx, other, req); !isAppErr(err, 404, "not_found") {
		t.Errorf("non-owner: %v", err)
	}
	// Unknown course.
	unknown := req
	unknown.CourseID = uuid.NewString()
	if _, err := w.svc.CreateUploadURL(ctx, w.instr, unknown); !isAppErr(err, 404, "not_found") {
		t.Errorf("unknown course: %v", err)
	}
	// A lesson of another course cannot be used to scope a key under this one.
	oc, _ := newCourse(t, w.svc, other, "other-"+uuid.NewString()[:8], "Other")
	om, err := w.svc.AddModule(ctx, oc, other, course.CreateModuleRequest{Title: "M", Position: 1})
	if err != nil {
		t.Fatal(err)
	}
	ol, err := w.svc.AddLesson(ctx, oc, other, uuid.MustParse(om.ID), course.CreateLessonRequest{Title: "L", Position: 1, ContentType: "text", Content: map[string]any{}, DurationS: 1})
	if err != nil {
		t.Fatal(err)
	}
	foreignLesson := req
	foreignLesson.LessonID = &ol.ID
	if _, err := w.svc.CreateUploadURL(ctx, w.instr, foreignLesson); !isAppErr(err, 404, "not_found") {
		t.Errorf("lesson of another course: %v", err)
	}
	notUUID := req
	bad := "../../x"
	notUUID.LessonID = &bad
	if _, err := w.svc.CreateUploadURL(ctx, w.instr, notUUID); !isAppErr(err, 400, "validation_failed") {
		t.Errorf("lesson id not a uuid: %v", err)
	}
	badCourse := req
	badCourse.CourseID = "../" + w.courseID.String()
	if _, err := w.svc.CreateUploadURL(ctx, w.instr, badCourse); !isAppErr(err, 400, "validation_failed") {
		t.Errorf("course id not a uuid: %v", err)
	}

	// No storage configured: 503.
	bare := course.NewService(course.NewRepository(w.env.DB), w.env.RDB, nil, "")
	if _, err := bare.CreateUploadURL(ctx, w.instr, req); !isAppErr(err, 503, "assets_unavailable") {
		t.Errorf("no store: %v", err)
	}

	// Archived course: refused.
	if err := w.svc.ArchiveCourse(ctx, w.courseID, w.instr); err != nil {
		t.Fatal(err)
	}
	if _, err := w.svc.CreateUploadURL(ctx, w.instr, req); !isAppErr(err, 409, "course_archived") {
		t.Errorf("archived: %v", err)
	}
}

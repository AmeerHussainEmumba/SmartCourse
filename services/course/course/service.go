// Domain logic, transactions, and ownership checks for course (RFC §4.3).
package course

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	temporalclient "go.temporal.io/sdk/client"
	"gorm.io/gorm"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/libs/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/httpx"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/outbox"
)

// isUniqueViolation duplicates identity's helper of the same name —
// intentionally: a 3-line Postgres-error check isn't worth a shared
// platform package, and each module staying self-contained is the point
// (RFC §4.4).
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

var slugPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// publishClaimStaleAfter is how long a `publishing` claim is trusted before
// another Publish may reclaim it. The workflow's activities are capped at 30s
// each with at most 5 attempts, so a live publish finishes in minutes; this
// only matters when the process that took the claim died before the workflow
// ran to completion.
const publishClaimStaleAfter = 15 * time.Minute

var (
	errSlugFormat      = apperrors.New(400, "invalid_slug", "Slug must be lowercase letters, numbers, and hyphens only.")
	errCourseArchived  = apperrors.New(409, "course_archived", "This course is archived and cannot be published.")
	errNoDraft         = apperrors.New(409, "no_draft", "This course has no draft version to edit.")
	errPublishRunning  = apperrors.New(409, "publish_in_progress", "A publish is already running for this course. Poll GET /courses/{id}/publish-status for the result.")
	errContentTooLarge = apperrors.New(400, "content_too_large", "Lesson content exceeds the maximum allowed size.")
)

// cacheTTL bounds how long a warmed course-detail entry lives in Redis.
// Deliberately NOT tied to explicit invalidation (ADR-0025): each entry's
// key is already scoped to one specific version number, so a republish
// simply starts writing under a new key — the old key is never read again
// and just needs to eventually expire rather than be hunted down and
// deleted. A day is long enough to matter for a popular course's detail
// reads, short enough that a key nobody explicitly cleans up doesn't
// accumulate forever.
const cacheTTL = 24 * time.Hour

func cacheKeyForVersion(courseID uuid.UUID, versionNumber int) string {
	return fmt.Sprintf("course:detail:%s:v%d", courseID, versionNumber)
}

type Service struct {
	repo      *Repository
	rdb       *redis.Client
	temporal  temporalclient.Client
	taskQueue string
	assets    AssetStore // nil until WithAssetStore; see assets.go
}

// temporal and taskQueue are nil/empty-safe for callers that never publish
// (tools/seed's read paths, most of this package's own tests) — only
// Service.Publish dereferences temporal, and only when actually called.
func NewService(repo *Repository, rdb *redis.Client, temporal temporalclient.Client, taskQueue string) *Service {
	return &Service{repo: repo, rdb: rdb, temporal: temporal, taskQueue: taskQueue}
}

// --- course & draft lifecycle ------------------------------------------------

// CreateCourse creates a course and its first draft version atomically —
// a course can never exist without at least a draft to edit.
func (s *Service) CreateCourse(ctx context.Context, instructorID uuid.UUID, req CreateCourseRequest) (*CourseResponse, *VersionResponse, error) {
	if !slugPattern.MatchString(req.Slug) {
		return nil, nil, errSlugFormat
	}
	if err := rejectNUL(req.Title, req.Description, ptrStr(req.Category), ptrStr(req.Level)); err != nil {
		return nil, nil, err
	}

	courseID := uuid.Must(uuid.NewV7())
	versionID := uuid.Must(uuid.NewV7())

	course := &Course{
		ID: courseID, InstructorID: instructorID, Slug: req.Slug,
		EnrollmentLimit: req.EnrollmentLimit,
	}
	version := &CourseVersion{
		ID: versionID, CourseID: courseID, VersionNumber: 1, State: StateDraft,
		Title: req.Title, Description: req.Description, Category: req.Category,
		Level: req.Level, Tags: pq.StringArray{}, Language: "en",
	}

	// Order matters: course_versions.course_id FKs to courses(id), so the
	// course row must exist first — created here with draft_version_id
	// still NULL, then set once the version row exists. courses.
	// draft_version_id FKs to course_versions(id), so the reverse order is
	// impossible; this three-step sequence is the only valid one, and it's
	// exactly why both FK columns on courses are nullable.
	err := s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.CreateCourse(ctx, course); err != nil {
			return err
		}
		if err := txRepo.CreateVersion(ctx, version); err != nil {
			return err
		}
		return txRepo.SetDraftVersion(ctx, courseID, versionID)
	})
	if isUniqueViolation(err) {
		return nil, nil, apperrors.ErrConflict.Wrap(fmt.Errorf("slug already in use"))
	}
	if err != nil {
		return nil, nil, dbError(err)
	}
	course.DraftVersionID = &versionID

	cResp := toCourseResponse(course)
	vResp := toVersionResponse(version)
	return &cResp, &vResp, nil
}

func (s *Service) GetCourse(ctx context.Context, id uuid.UUID) (*Course, error) {
	c, err := s.repo.GetCourseByID(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	return c, nil
}

// requireOwner loads the course and asserts instructorID owns it —
// ownership is checked here, in the service layer, never inferred from a
// client-supplied ID (RFC §11.4). Every draft-mutating method below calls
// this first. A caller who does not own an existing course gets the same
// 404 as for an unknown id, so the response does not reveal whether a
// course id exists (F-36).
func (s *Service) requireOwner(ctx context.Context, courseID, instructorID uuid.UUID) (*Course, error) {
	c, err := s.repo.GetCourseByID(ctx, courseID)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	if c.InstructorID != instructorID {
		return nil, apperrors.ErrNotFound
	}
	return c, nil
}

func (s *Service) UpdateDraft(ctx context.Context, courseID, instructorID uuid.UUID, req UpdateDraftRequest) (*VersionResponse, error) {
	if err := rejectNUL(append([]string{req.Title, req.Description, ptrStr(req.Category), ptrStr(req.Level)}, req.Tags...)...); err != nil {
		return nil, err
	}
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	if c.DraftVersionID == nil {
		return nil, errNoDraft
	}
	if err := s.repo.UpdateVersionMetadata(ctx, *c.DraftVersionID, req.Title, req.Description, req.Category, req.Level, req.Tags); err != nil {
		return nil, dbError(err)
	}
	v, err := s.repo.GetVersionByID(ctx, *c.DraftVersionID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	resp := toVersionResponse(v)
	return &resp, nil
}

// ArchiveCourse is the "delete a course" endpoint in effect — a soft
// delete via archived_at, not a row removal: a course students have
// enrollment/progress/certificate history against can never actually
// disappear. ListCatalogue and enrollment.Enroll both already gate on
// archived_at IS NULL, so this takes effect everywhere a student-facing
// read or write touches the course, with no other change needed.
// ArchiveCourse writes course.course_archived in the same transaction as
// setting archived_at (RFC §4.4/ADR-0006: that's the only channel another
// module may react through — this module never imports enrollment's Go
// API). enrollment.Handlers' handleCourseArchived consumes it to withdraw
// every active enrollment: "if a course is archived, all active students
// of it should be outrolled."
func (s *Service) ArchiveCourse(ctx context.Context, courseID, instructorID uuid.UUID) error {
	if _, err := s.requireOwner(ctx, courseID, instructorID); err != nil {
		return err
	}
	err := s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.ArchiveCourse(ctx, courseID); err != nil {
			return err
		}
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "course",
			AggregateID:   courseID,
			EventType:     "course.course_archived",
			SchemaVersion: 1,
			Payload:       map[string]any{"course_id": courseID},
		})
	})
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// --- modules -----------------------------------------------------------------

func (s *Service) AddModule(ctx context.Context, courseID, instructorID uuid.UUID, req CreateModuleRequest) (*ModuleResponse, error) {
	if err := rejectNUL(req.Title); err != nil {
		return nil, err
	}
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	if c.DraftVersionID == nil {
		return nil, errNoDraft
	}
	m := &Module{
		ID: uuid.Must(uuid.NewV7()), CourseVersionID: *c.DraftVersionID,
		ModuleKey: uuid.New(), Position: req.Position, Title: req.Title,
	}
	if err := s.repo.CreateModule(ctx, m); err != nil {
		if isUniqueViolation(err) {
			return nil, apperrors.ErrConflict.Wrap(fmt.Errorf("a module already occupies this position"))
		}
		return nil, dbError(err)
	}
	resp := toModuleResponse(m)
	return &resp, nil
}

// UpdateModule, like AddModule, only ever touches the draft version — a
// published version is immutable (ADR-0010); editing it in place would let
// a student see a course change mid-session with no publish event, no
// outbox write, nothing. moduleID identifying a module on a non-draft
// version is rejected the same way AddLesson rejects it.
func (s *Service) UpdateModule(ctx context.Context, courseID, instructorID, moduleID uuid.UUID, req UpdateModuleRequest) (*ModuleResponse, error) {
	if err := rejectNUL(req.Title); err != nil {
		return nil, err
	}
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	mod, err := s.repo.GetModuleByID(ctx, moduleID)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	if c.DraftVersionID == nil || mod.CourseVersionID != *c.DraftVersionID {
		return nil, errNoDraft
	}
	if err := s.repo.UpdateModule(ctx, moduleID, req.Title); err != nil {
		return nil, dbError(err)
	}
	mod.Title = req.Title
	resp := toModuleResponse(mod)
	return &resp, nil
}

// DeleteModule, like UpdateModule, only touches the draft version: a
// published version is immutable (ADR-0010), and deleting its modules would
// cascade away live lessons and change enrolled students' completion
// denominators (F-10).
func (s *Service) DeleteModule(ctx context.Context, courseID, instructorID, moduleID uuid.UUID) error {
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return err
	}
	mod, err := s.repo.GetModuleByID(ctx, moduleID)
	if errors.Is(err, ErrNotFound) {
		return apperrors.ErrNotFound
	}
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if c.DraftVersionID == nil || mod.CourseVersionID != *c.DraftVersionID {
		return errNoDraft
	}
	if err := s.repo.DeleteModule(ctx, moduleID); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	_ = s.repo.RecomputeVersionTotals(ctx, *c.DraftVersionID)
	return nil
}

// --- lessons -------------------------------------------------------------

func (s *Service) AddLesson(ctx context.Context, courseID, instructorID, moduleID uuid.UUID, req CreateLessonRequest) (*LessonResponse, error) {
	if err := rejectNUL(req.Title, req.ContentType, ptrStr(req.AssetKey)); err != nil {
		return nil, err
	}
	if err := validateContent(req.Content); err != nil {
		return nil, err
	}
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	if c.DraftVersionID == nil {
		return nil, errNoDraft
	}
	if req.AssetKey != nil && *req.AssetKey != "" {
		if err := validateAssetKeyForCourse(*req.AssetKey, courseID); err != nil {
			return nil, err
		}
	}
	mod, err := s.repo.GetModuleByID(ctx, moduleID)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	if mod.CourseVersionID != *c.DraftVersionID {
		return nil, errNoDraft // module belongs to a different (non-draft) version — reject rather than silently editing history
	}

	contentJSON, err := json.Marshal(req.Content)
	if err != nil {
		return nil, apperrors.ErrValidation.Wrap(err)
	}
	if len(contentJSON) >= maxLessonContentBytes {
		return nil, errContentTooLarge
	}

	l := &Lesson{
		ID: uuid.Must(uuid.NewV7()), ModuleID: moduleID, LessonKey: uuid.New(),
		Position: req.Position, Title: req.Title, ContentType: req.ContentType,
		Content: contentJSON, AssetKey: req.AssetKey, DurationS: req.DurationS,
	}
	if err := s.repo.CreateLesson(ctx, l); err != nil {
		if isUniqueViolation(err) {
			return nil, apperrors.ErrConflict.Wrap(fmt.Errorf("a lesson already occupies this position"))
		}
		return nil, dbError(err)
	}
	_ = s.repo.RecomputeVersionTotals(ctx, *c.DraftVersionID)

	return &LessonResponse{
		ID: l.ID.String(), LessonKey: l.LessonKey.String(), Position: l.Position, Title: l.Title,
		ContentType: l.ContentType, Content: req.Content, DurationS: l.DurationS,
	}, nil
}

// UpdateLesson is UpdateModule's sibling: draft-only, same reasoning.
// RecomputeVersionTotals runs afterward because duration_s can change.
func (s *Service) UpdateLesson(ctx context.Context, courseID, instructorID, lessonID uuid.UUID, req UpdateLessonRequest) (*LessonResponse, error) {
	if err := rejectNUL(req.Title, req.ContentType); err != nil {
		return nil, err
	}
	if err := validateContent(req.Content); err != nil {
		return nil, err
	}
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	lesson, err := s.repo.GetLessonByID(ctx, lessonID)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	mod, err := s.repo.GetModuleByID(ctx, lesson.ModuleID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	if c.DraftVersionID == nil || mod.CourseVersionID != *c.DraftVersionID {
		return nil, errNoDraft
	}

	// req.Content is nil both when the client omits the field and when it
	// sends an explicit JSON null — neither means "clear the content," so
	// a nil req.Content preserves whatever is already stored. An explicit
	// "content": {} is a real, non-nil empty map and does overwrite, same
	// as any other value. Without this, a PATCH that only means to change
	// title/duration_s (the normal way to call PATCH) silently destroys
	// the lesson's real content — found by a live test against a seeded
	// lesson's content turning into literal SQL `null`.
	contentJSON := lesson.Content
	respContent := req.Content
	if req.Content != nil {
		cj, err := json.Marshal(req.Content)
		if err != nil {
			return nil, apperrors.ErrValidation.Wrap(err)
		}
		if len(cj) >= maxLessonContentBytes {
			return nil, errContentTooLarge
		}
		contentJSON = cj
	} else {
		_ = json.Unmarshal(lesson.Content, &respContent)
	}
	if err := s.repo.UpdateLesson(ctx, lessonID, req.Title, req.ContentType, contentJSON, req.DurationS); err != nil {
		return nil, dbError(err)
	}
	_ = s.repo.RecomputeVersionTotals(ctx, *c.DraftVersionID)

	return &LessonResponse{
		ID: lesson.ID.String(), LessonKey: lesson.LessonKey.String(), Position: lesson.Position,
		Title: req.Title, ContentType: req.ContentType, Content: respContent, DurationS: req.DurationS,
	}, nil
}

func (s *Service) DeleteLesson(ctx context.Context, courseID, instructorID, lessonID uuid.UUID) error {
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return err
	}
	lesson, err := s.repo.GetLessonByID(ctx, lessonID)
	if errors.Is(err, ErrNotFound) {
		return apperrors.ErrNotFound
	}
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	mod, err := s.repo.GetModuleByID(ctx, lesson.ModuleID)
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if c.DraftVersionID == nil || mod.CourseVersionID != *c.DraftVersionID {
		return errNoDraft // published versions are immutable (ADR-0010, F-10)
	}
	if err := s.repo.DeleteLesson(ctx, lessonID); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	_ = s.repo.RecomputeVersionTotals(ctx, *c.DraftVersionID)
	return nil
}

// --- publish -----------------------------------------------------------------
//
// M2 implements RFC §7.4's full activity graph as a Temporal workflow
// (services/course/workflows.PublishCourseWorkflow, ADR-0009/0025):
//
//	ValidateMetadata -> {VerifyAssets, ComputeMetadata} -> BuildOutline -> {IndexSearch, WarmCache} -> PromoteVersion
//
// Every exported method below this comment up to PromoteAndPublish is one
// node in that graph — plain, Temporal-agnostic Go methods on Service, each
// independently retryable because Temporal calls them as separate
// activities (services/course/workflows.Activities just forwards to these; the
// Temporal SDK import stays out of this package entirely, matching RFC
// §4.4's module-boundary discipline extended to the new package). This
// replaces M1's runPublishChecks/validateMetadata/validateLessonContent,
// which bundled the same checks into one errgroup inside a single
// synchronous call — real concurrency, but none of it survived a process
// crash mid-publish, which is exactly what Temporal buys.
//
// InitAnalytics isn't one of these methods: analytics already recomputes
// course_stats from the course.course_published outbox event
// PromoteAndPublish writes, via the existing relay path (ADR-0021). Adding
// a second, synchronous write here would race that idempotent recompute
// for no benefit — ADR-0025 documents this as a deliberate omission, not a
// gap.

// Publish starts the publish workflow and returns immediately (RFC §7.5:
// 202, not 200, as of M2) — it does not wait for ValidateMetadata or
// anything else to run.
//
// The version is claimed first: one conditional UPDATE moves it from
// draft/publish_failed to `publishing` (ClaimPublish) BEFORE the workflow is
// started. A second publish while that claim is live gets 409
// publish_in_progress rather than attaching to the running workflow, and the
// workflow's own state writes (publish_failed, ready) can never be overwritten
// by a late "publishing" write. If the workflow cannot be started the claim is
// released (ReleasePublish) so the course is not left wedged; a claim older
// than publishClaimStaleAfter is treated as abandoned and may be reclaimed.
// The workflow ID stays deterministic (publish-{course_id}-{version_number},
// RFC §7.5), on top of idx_one_draft_per_course's storage-layer guard against
// two different drafts racing. ADR-0028 records the change.
func (s *Service) Publish(ctx context.Context, courseID, instructorID uuid.UUID) (*PublishAcceptedResponse, error) {
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
	draft, err := s.repo.GetVersionByID(ctx, *c.DraftVersionID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	workflowID := fmt.Sprintf("publish-%s-%d", courseID, draft.VersionNumber)

	// Claim the version BEFORE starting the workflow. One conditional UPDATE
	// moves it to `publishing`; if another publish already holds it we say so
	// (409) instead of attaching to the running workflow. Writing the state
	// first also means the workflow's own transitions (publish_failed, ready)
	// can never be overwritten by a late "publishing" write.
	claim, claimed, err := s.repo.ClaimPublish(ctx, draft.ID, workflowID, publishClaimStaleAfter)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	if !claimed {
		return nil, errPublishRunning
	}

	_, err = s.temporal.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: s.taskQueue,
	}, PublishWorkflowName, PublishWorkflowInput{
		CourseID: courseID, VersionID: draft.ID, OldVersionID: c.PublishedVersionID, Title: draft.Title,
	})
	if err != nil {
		// The workflow did not start, so nothing will ever move this version
		// out of `publishing`: put it back, or the course would be wedged
		// until the claim goes stale. Use a fresh context, since ctx may be
		// the very thing that failed (a cancelled request).
		relCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if relErr := s.repo.ReleasePublish(relCtx, draft.ID, workflowID, claim); relErr != nil {
			return nil, apperrors.ErrUnavailable.Wrap(fmt.Errorf("start publish workflow: %w (and releasing the publish claim failed: %v)", err, relErr))
		}
		return nil, apperrors.ErrUnavailable.Wrap(fmt.Errorf("start publish workflow: %w", err))
	}
	return &PublishAcceptedResponse{WorkflowID: workflowID, State: string(StatePublishing)}, nil
}

// GetPublishStatus backs GET /courses/{id}/publish-status (RFC §7.5) — it
// reads course_versions directly, never Temporal's API: every state
// transition a client cares about (publishing/ready/publish_failed) is
// already written to Postgres by the activities below, which is also the
// system of record every other read in this module uses. Querying Temporal
// instead would mean this endpoint has a dependency GetPublishedDetail and
// ListCatalogue don't, for data that's already sitting in the same row.
func (s *Service) GetPublishStatus(ctx context.Context, courseID, instructorID uuid.UUID) (*PublishStatusResponse, error) {
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	// The draft, if one exists, is the version actually publishing or
	// having just failed to — once PromoteAndPublish commits, the draft
	// pointer is cleared and the published version itself holds the
	// terminal "ready" state instead.
	versionID := c.DraftVersionID
	if versionID == nil {
		versionID = c.PublishedVersionID
	}
	if versionID == nil {
		return nil, apperrors.ErrNotFound
	}
	v, err := s.repo.GetVersionByID(ctx, *versionID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	return &PublishStatusResponse{WorkflowID: v.WorkflowID, State: string(v.State), Error: v.PublishError}, nil
}

// ValidateMetadata is the ValidateMetadata activity (RFC §7.4 — "None" in
// the retry column: a failure here is the instructor's error, not a
// transient one, so the workflow marks the version failed immediately
// rather than retrying).
func (s *Service) ValidateMetadata(ctx context.Context, versionID uuid.UUID) error {
	v, err := s.repo.GetVersionByID(ctx, versionID)
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if v.Title == "" {
		return apperrors.ErrValidation.Wrap(fmt.Errorf("title is required to publish"))
	}
	mods, err := s.repo.ListModules(ctx, v.ID)
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if len(mods) == 0 {
		return apperrors.ErrValidation.Wrap(fmt.Errorf("at least one module is required to publish"))
	}
	lessons, err := s.repo.CountLessons(ctx, v.ID)
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if lessons == 0 {
		return apperrors.ErrValidation.Wrap(fmt.Errorf("at least one lesson is required to publish"))
	}
	return nil
}

// VerifyAssets is the RFC §7.4 activity of the same name. Two independent
// checks run over the draft's lessons, concurrent with ComputeMetadata:
//
//  1. Every lesson's actual content is scanned for size. Defense in depth
//     beyond the DB's own CHECK constraint: this fails as a clean 400-equivalent
//     before publish, rather than a raw constraint violation if the
//     application-level check at write time is ever bypassed.
//  2. Every asset key a lesson references (asset_key) must be a key this service
//     issued for THIS course, must exist in object storage (StatObject), and
//     must match the allowed type and size (RFC §7.4 A2: existence, content
//     type and size, never the content itself). A missing or invalid asset
//     fails the publish before PromoteAndPublish, so the live version is never
//     touched. Lessons that reference no asset are not affected.
func (s *Service) VerifyAssets(ctx context.Context, versionID uuid.UUID) error {
	v, err := s.repo.GetVersionByID(ctx, versionID)
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	mods, err := s.repo.ListModules(ctx, versionID)
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	refs := map[string]string{} // asset key -> title of a lesson referencing it
	for _, m := range mods {
		lessons, err := s.repo.ListLessons(ctx, m.ID)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		for _, l := range lessons {
			if len(l.Content) >= maxLessonContentBytes {
				return errContentTooLarge
			}
			if l.AssetKey != nil && *l.AssetKey != "" {
				if _, seen := refs[*l.AssetKey]; !seen {
					refs[*l.AssetKey] = l.Title
				}
			}
		}
	}
	return s.verifyLessonAssets(ctx, v.CourseID, refs)
}

// ComputeMetadata is the ComputeMetadata activity — lesson count and total
// duration, recomputed from the draft's actual content tree rather than
// trusted from incremental updates made while authoring.
func (s *Service) ComputeMetadata(ctx context.Context, versionID uuid.UUID) error {
	if err := s.repo.RecomputeVersionTotals(ctx, versionID); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// BuildOutline is the BuildOutline activity — assembles everything
// IndexSearch and WarmCache need in one pass (one set of DB reads, not two)
// and pre-marshals the course detail payload WarmCache writes to Redis
// verbatim, so a cache hit never re-serializes anything.
func (s *Service) BuildOutline(ctx context.Context, courseID, versionID uuid.UUID) (*PublishOutline, error) {
	c, err := s.repo.GetCourseByID(ctx, courseID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	v, err := s.repo.GetVersionByID(ctx, versionID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	// BuildOutline runs before PromoteAndPublish (RFC §7.4 — the cache
	// must already be warm the instant the pointer swap makes the course
	// live, not built afterward). At this point c and v still reflect the
	// DRAFT's pre-promotion state: c.DraftVersionID is still set,
	// c.PublishedVersionID may still point at the OLD version or be nil,
	// and v.State is "publishing", not "ready". Caching that snapshot
	// verbatim would mean GetPublishedDetail's first cache hit after a
	// publish serves a course that still looks unpublished — caught live
	// (`make dev-workflow` + a real publish), not by inspection. published
	// and readyVersion are what the row WILL read as the instant
	// PromoteAndPublish's transaction commits — deterministic, since
	// PromoteVersion (repository.go) sets exactly these three fields.
	published := *c
	published.PublishedVersionID = &v.ID
	published.DraftVersionID = nil
	readyVersion := *v
	readyVersion.State = StateReady

	detail, err := s.loadDetail(ctx, &published, &readyVersion)
	if err != nil {
		return nil, err
	}
	instructorName, err := s.repo.GetInstructorName(ctx, c.InstructorID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	return &PublishOutline{
		CourseID: c.ID, VersionID: v.ID, VersionNumber: v.VersionNumber, Slug: c.Slug,
		Title: v.Title, Description: v.Description, InstructorName: instructorName,
		Category: v.Category, Level: v.Level, Tags: []string(v.Tags),
		EnrollmentCount: c.EnrollmentCount, DetailJSON: detailJSON,
	}, nil
}

// IndexSearch is the IndexSearch activity (RFC §7.4, pulled forward from
// M3 per ADR-0025 rather than left as a no-op stand-in — GET
// /courses/search is live, not just this write side of it).
func (s *Service) IndexSearch(ctx context.Context, o *PublishOutline) error {
	if err := s.repo.UpsertSearchIndex(ctx, o); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// WarmCache is the WarmCache activity (RFC §7.4, same pull-forward as
// IndexSearch) — writes the version-scoped Redis key GetPublishedDetail
// reads through. See cacheTTL's comment for why there's no explicit
// invalidation step: each version gets its own key.
func (s *Service) WarmCache(ctx context.Context, o *PublishOutline) error {
	key := cacheKeyForVersion(o.CourseID, o.VersionNumber)
	if err := s.rdb.Set(ctx, key, o.DetailJSON, cacheTTL).Err(); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// PromoteAndPublish is the PromoteVersion activity — must run last (RFC
// §7.4: "the only one whose effect students can observe"). Promotion, the
// search-index refresh for the promoted version, and the
// course.course_published outbox write happen in one transaction, so they
// either all happen or none does. Temporal's at-least-once activity
// execution means this method must be safe to run twice, including two
// executions overlapping: the idempotency guard therefore runs INSIDE the
// transaction, after taking the course row lock, so exactly one execution
// promotes and emits the event (F-23). An archived course is never promoted
// (F-21).
func (s *Service) PromoteAndPublish(ctx context.Context, courseID, versionID uuid.UUID, oldVersionID *uuid.UUID, title string) error {
	// Read-only assembly of the index row (draft content does not change
	// under us); the write happens inside the transaction below.
	outline, err := s.BuildOutline(ctx, courseID, versionID)
	if err != nil {
		return err
	}
	err = s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		c, err := txRepo.GetCourseForUpdate(ctx, courseID)
		if err != nil {
			return err
		}
		if c.PublishedVersionID != nil && *c.PublishedVersionID == versionID {
			return nil // already promoted by a previous or concurrent attempt
		}
		if c.ArchivedAt != nil {
			return errCourseArchived
		}
		if err := txRepo.PromoteVersion(ctx, courseID, versionID, oldVersionID); err != nil {
			return err
		}
		if err := txRepo.UpsertSearchIndex(ctx, outline); err != nil {
			return err
		}
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "course",
			AggregateID:   courseID,
			EventType:     "course.course_published",
			SchemaVersion: 1,
			// instructor_id is additive: consumers decode only the fields
			// they need (analytics ignores it today). It lets a consumer
			// attribute the course to its owner without reading course's
			// schema (ADR-0024).
			Payload: map[string]any{"course_id": courseID, "version_id": versionID, "title": title, "instructor_id": c.InstructorID},
		})
	})
	if err != nil {
		var appErr *apperrors.AppError
		if errors.As(err, &appErr) {
			return err
		}
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// MarkPublishFailed is the workflow's failure-handling step (RFC §7.4: "no
// compensation of prior activities is needed, because none of them
// affected the live version — a property of the design, not an
// oversight"). Called by the workflow with the triggering activity's error
// message; never from the HTTP path, which only ever starts a publish, not
// fails one.
//
// It writes course.publish_failed in the same transaction as the transition,
// and only when the version was not already publish_failed: Temporal retries
// this activity (retry5x), and a retry after a committed first attempt must
// not emit a second event. failedActivity names the workflow activity whose
// final failure ended the publish.
func (s *Service) MarkPublishFailed(ctx context.Context, versionID uuid.UUID, reason, failedActivity string) error {
	// Postgres text rejects invalid UTF-8 and NUL; an activity error that
	// carried either would otherwise fail this step on every retry.
	reason = strings.ReplaceAll(strings.ToValidUTF8(reason, ""), "\x00", "")
	err := s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		changed, err := s.repo.WithTx(tx).FailVersion(ctx, versionID, reason)
		if err != nil || !changed {
			return err
		}
		return s.writePublishFailed(ctx, tx, versionID, failedActivity, reason)
	})
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// --- catalogue & detail ----------------------------------------------------

func (s *Service) ListCatalogue(ctx context.Context, limit int, cursor, category, level, search string) (*CatalogueResponse, error) {
	if err := rejectNUL(cursor, category, level, search); err != nil {
		return nil, err
	}
	var afterCreatedAt *time.Time
	var afterID *uuid.UUID
	if cursor != "" {
		c, err := httpx.DecodeCursor(cursor)
		if err != nil {
			return nil, apperrors.ErrValidation.Wrap(err)
		}
		id, err := uuid.Parse(c.ID)
		if err != nil {
			return nil, apperrors.ErrValidation.Wrap(err)
		}
		afterCreatedAt = &c.CreatedAt
		afterID = &id
	}

	filter := CatalogueFilter{Search: search}
	if category != "" {
		filter.Category = &category
	}
	if level != "" {
		filter.Level = &level
	}

	rows, err := s.repo.ListCatalogue(ctx, limit+1, afterCreatedAt, afterID, filter)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	resp := &CatalogueResponse{}
	hasMore := len(rows) > limit
	if hasMore {
		rows = rows[:limit]
	}
	for _, row := range rows {
		resp.Items = append(resp.Items, CatalogueCardResponse{
			CourseID: row.CourseID.String(), Slug: row.Slug, Title: row.Title,
			InstructorName: row.InstructorName, Category: row.Category, Level: row.Level,
			EnrollmentCount: row.EnrollmentCount,
		})
	}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		next, err := httpx.EncodeCursor(httpx.Cursor{CreatedAt: last.CreatedAt, ID: last.CourseID.String()})
		if err != nil {
			return nil, apperrors.ErrInternal.Wrap(err)
		}
		resp.NextCursor = &next
	}
	return resp, nil
}

// GetPublishedDetail loads a course's currently published version, its
// modules, and their lessons by slug — the "course detail" read (RFC
// §5.5). Progress is deliberately NOT joined here: that join happens in
// services/course/progress against this same published version, keeping this
// method usable by both authenticated and (future) anonymous callers.
//
// Read-through cache (ADR-0025, pulled forward from M3): WarmCache already
// writes this exact payload to Redis on every publish, keyed by version
// number, so a hit here skips the module/lesson reads below entirely. A
// miss falls through to Postgres and fills the cache itself — not just
// relying on WarmCache — so a cold key (eviction, or a read landing before
// WarmCache's activity finishes) still gets a correct answer, just a
// slower one.
func (s *Service) GetPublishedDetail(ctx context.Context, slug string) (*CourseDetailResponse, error) {
	if strings.ContainsRune(slug, 0) {
		return nil, apperrors.ErrNotFound // no slug can contain NUL
	}
	c, err := s.repo.GetCourseBySlug(ctx, slug)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	if c.PublishedVersionID == nil || c.ArchivedAt != nil {
		return nil, apperrors.ErrNotFound
	}

	v, err := s.repo.GetVersionByID(ctx, *c.PublishedVersionID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	key := cacheKeyForVersion(c.ID, v.VersionNumber)
	if cached, err := s.rdb.Get(ctx, key).Bytes(); err == nil {
		var resp CourseDetailResponse
		if json.Unmarshal(cached, &resp) == nil {
			// enrollment_count/enrollment_limit change on every
			// enrollment and are not version data: never serve the
			// cached copy of them (F-20). c was read live above.
			resp.Course = toCourseResponse(c)
			return &resp, nil
		}
	}

	resp, err := s.loadDetail(ctx, c, v)
	if err != nil {
		return nil, err
	}
	if respJSON, err := json.Marshal(resp); err == nil {
		_ = s.rdb.Set(ctx, key, respJSON, cacheTTL).Err() // best-effort fill-on-miss; a Redis error here must not fail a read Postgres already answered correctly
	}
	return resp, nil
}

// loadDetail is GetPublishedDetail and BuildOutline's shared read: both
// assemble the identical Course/Version/Modules/Lessons shape, one against
// the published version, the other against the draft mid-publish — the
// query shape (RFC §13.2's N+1 discipline) shouldn't exist twice just
// because the caller differs.
func (s *Service) loadDetail(ctx context.Context, c *Course, v *CourseVersion) (*CourseDetailResponse, error) {
	mods, err := s.repo.ListModules(ctx, v.ID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	resp := &CourseDetailResponse{Course: toCourseResponse(c), Version: toVersionResponse(v)}
	for _, m := range mods {
		lessons, err := s.repo.ListLessons(ctx, m.ID)
		if err != nil {
			return nil, apperrors.ErrInternal.Wrap(err)
		}
		mwl := ModuleWithLessons{ModuleResponse: toModuleResponse(&m)}
		for _, l := range lessons {
			var content map[string]any
			_ = json.Unmarshal(l.Content, &content)
			mwl.Lessons = append(mwl.Lessons, LessonResponse{
				ID: l.ID.String(), LessonKey: l.LessonKey.String(), Position: l.Position, Title: l.Title,
				ContentType: l.ContentType, Content: content, DurationS: l.DurationS,
			})
		}
		resp.Modules = append(resp.Modules, mwl)
	}
	return resp, nil
}

// SearchCourses backs GET /courses/search (RFC §7.4's IndexSearch, read
// side) — ranked full-text search over course_search_index, distinct from
// ListCatalogue's plain ILIKE substring filter.
func (s *Service) SearchCourses(ctx context.Context, query string, limit int) ([]SearchResultResponse, error) {
	if err := rejectNUL(query); err != nil {
		return nil, err
	}
	rows, err := s.repo.SearchCourses(ctx, query, limit)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	out := make([]SearchResultResponse, 0, len(rows))
	for _, row := range rows {
		out = append(out, SearchResultResponse{
			CourseID: row.CourseID.String(), Slug: row.Slug, Title: row.Title,
			InstructorName: row.InstructorName, Category: row.Category, Level: row.Level,
			EnrollmentCount: row.EnrollmentCount,
		})
	}
	return out, nil
}

// ResolveModuleCourseID and ResolveLessonCourseID exist so handler.go never
// reaches into the repository directly (RFC §4.3 — handler.go is HTTP
// only). Deleting a module/lesson needs the owning course's ID for the
// ownership check, and the client only supplies the module/lesson ID.
func (s *Service) ResolveModuleCourseID(ctx context.Context, moduleID uuid.UUID) (uuid.UUID, error) {
	mod, err := s.repo.GetModuleByID(ctx, moduleID)
	if errors.Is(err, ErrNotFound) {
		return uuid.Nil, apperrors.ErrNotFound
	}
	if err != nil {
		return uuid.Nil, apperrors.ErrInternal.Wrap(err)
	}
	ver, err := s.repo.GetVersionByID(ctx, mod.CourseVersionID)
	if err != nil {
		return uuid.Nil, apperrors.ErrInternal.Wrap(err)
	}
	return ver.CourseID, nil
}

func (s *Service) ResolveLessonCourseID(ctx context.Context, lessonID uuid.UUID) (uuid.UUID, error) {
	lesson, err := s.repo.GetLessonByID(ctx, lessonID)
	if errors.Is(err, ErrNotFound) {
		return uuid.Nil, apperrors.ErrNotFound
	}
	if err != nil {
		return uuid.Nil, apperrors.ErrInternal.Wrap(err)
	}
	return s.ResolveModuleCourseID(ctx, lesson.ModuleID)
}

// --- input validation helpers (F-17) -----------------------------------------

// maxContentDepth bounds nesting of lesson content. Deeply nested JSON can
// pass the text-size check yet blow the jsonb size CHECK, and costs a lot to
// decode; real lesson content is nowhere near this deep.
const maxContentDepth = 32

var errNUL = apperrors.ErrValidation.Wrap(fmt.Errorf("NUL characters are not allowed"))

// rejectNUL fails with a 400 when any value contains a NUL byte, which
// Postgres text/jsonb cannot store (SQLSTATE 22021).
func rejectNUL(vals ...string) error {
	for _, v := range vals {
		if strings.ContainsRune(v, 0) {
			return errNUL
		}
	}
	return nil
}

func ptrStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// validateContent rejects NUL in any key or string value and nesting deeper
// than maxContentDepth.
func validateContent(content map[string]any) error {
	return walkContent(content, 1)
}

func walkContent(v any, depth int) error {
	if depth > maxContentDepth {
		return apperrors.ErrValidation.Wrap(fmt.Errorf("content is nested too deeply (max %d levels)", maxContentDepth))
	}
	switch t := v.(type) {
	case string:
		return rejectNUL(t)
	case map[string]any:
		for k, e := range t {
			if err := rejectNUL(k); err != nil {
				return err
			}
			if err := walkContent(e, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, e := range t {
			if err := walkContent(e, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// dbError maps Postgres data errors that bad input can still provoke
// (22021 invalid byte sequence, 22003 numeric out of range, 23514 check
// violation) to a 400 instead of a 500.
func dbError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "22021", "22003", "23514":
			return apperrors.ErrValidation.Wrap(err)
		}
	}
	return apperrors.ErrInternal.Wrap(err)
}

// escapeLike escapes LIKE/ILIKE metacharacters so user input matches
// literally (Postgres' default escape character is backslash).
func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

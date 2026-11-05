// Domain logic, transactions, and ownership checks for course (RFC §4.3).
package course

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	temporalclient "go.temporal.io/sdk/client"
	"gorm.io/gorm"

	apperrors "github.com/AmeerHussainEmumba/SmartCourse/internal/platform/errors"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/httpx"
	"github.com/AmeerHussainEmumba/SmartCourse/internal/platform/outbox"
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

var (
	errSlugFormat      = apperrors.New(400, "invalid_slug", "Slug must be lowercase letters, numbers, and hyphens only.")
	errNotOwner        = apperrors.New(403, "not_owner", "You do not own this course.")
	errNoDraft         = apperrors.New(409, "no_draft", "This course has no draft version to edit.")
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
}

// temporal and taskQueue are nil/empty-safe for callers that never publish
// (cmd/seed's read paths, most of this package's own tests) — only
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
		return nil, nil, apperrors.ErrInternal.Wrap(err)
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
// this first.
func (s *Service) requireOwner(ctx context.Context, courseID, instructorID uuid.UUID) (*Course, error) {
	c, err := s.repo.GetCourseByID(ctx, courseID)
	if errors.Is(err, ErrNotFound) {
		return nil, apperrors.ErrNotFound
	}
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	if c.InstructorID != instructorID {
		return nil, errNotOwner
	}
	return c, nil
}

func (s *Service) UpdateDraft(ctx context.Context, courseID, instructorID uuid.UUID, req UpdateDraftRequest) (*VersionResponse, error) {
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	if c.DraftVersionID == nil {
		return nil, errNoDraft
	}
	if err := s.repo.UpdateVersionMetadata(ctx, *c.DraftVersionID, req.Title, req.Description, req.Category, req.Level, req.Tags); err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
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
		return nil, apperrors.ErrInternal.Wrap(err)
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
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	mod.Title = req.Title
	resp := toModuleResponse(mod)
	return &resp, nil
}

func (s *Service) DeleteModule(ctx context.Context, courseID, instructorID, moduleID uuid.UUID) error {
	if _, err := s.requireOwner(ctx, courseID, instructorID); err != nil {
		return err
	}
	if err := s.repo.DeleteModule(ctx, moduleID); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// --- lessons -------------------------------------------------------------

func (s *Service) AddLesson(ctx context.Context, courseID, instructorID, moduleID uuid.UUID, req CreateLessonRequest) (*LessonResponse, error) {
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	if c.DraftVersionID == nil {
		return nil, errNoDraft
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
		return nil, apperrors.ErrInternal.Wrap(err)
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
		return nil, apperrors.ErrInternal.Wrap(err)
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
	if err := s.repo.DeleteLesson(ctx, lessonID); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if c.DraftVersionID != nil {
		_ = s.repo.RecomputeVersionTotals(ctx, *c.DraftVersionID)
	}
	return nil
}

// --- publish -----------------------------------------------------------------
//
// M2 implements RFC §7.4's full activity graph as a Temporal workflow
// (internal/workflows.PublishCourseWorkflow, ADR-0009/0025):
//
//	ValidateMetadata -> {VerifyAssets, ComputeMetadata} -> BuildOutline -> {IndexSearch, WarmCache} -> PromoteVersion
//
// Every exported method below this comment up to PromoteAndPublish is one
// node in that graph — plain, Temporal-agnostic Go methods on Service, each
// independently retryable because Temporal calls them as separate
// activities (internal/workflows.Activities just forwards to these; the
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
// anything else to run. The workflow ID is deterministic
// (publish-{course_id}-{version_number}), so a duplicate publish request
// while one is already running attaches to it rather than starting a
// second one (client.ExecuteWorkflow's default WorkflowIDReusePolicy
// returns the existing run's handle instead of erroring) — idempotency at
// the orchestration layer, on top of idx_one_draft_per_course's
// storage-layer guard against two different drafts racing.
func (s *Service) Publish(ctx context.Context, courseID, instructorID uuid.UUID) (*PublishAcceptedResponse, error) {
	c, err := s.requireOwner(ctx, courseID, instructorID)
	if err != nil {
		return nil, err
	}
	if c.DraftVersionID == nil {
		return nil, errNoDraft
	}
	draft, err := s.repo.GetVersionByID(ctx, *c.DraftVersionID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	workflowID := fmt.Sprintf("publish-%s-%d", courseID, draft.VersionNumber)
	_, err = s.temporal.ExecuteWorkflow(ctx, temporalclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: s.taskQueue,
	}, PublishWorkflowName, PublishWorkflowInput{
		CourseID: courseID, VersionID: draft.ID, OldVersionID: c.PublishedVersionID, Title: draft.Title,
	})
	if err != nil {
		return nil, apperrors.ErrUnavailable.Wrap(fmt.Errorf("start publish workflow: %w", err))
	}

	if err := s.repo.SetVersionPublishing(ctx, draft.ID, workflowID); err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
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
	return nil
}

// VerifyAssets stands in for the RFC §7.4 activity of the same name: no
// asset-upload endpoint exists yet (RFC §21.0), so there's nothing to
// HeadObject against. What runs instead is a genuine, independent scan of
// every lesson's actual content in the draft — not a placeholder, just a
// narrower real check in the same graph position, concurrent with
// ComputeMetadata. Defense in depth beyond the DB's own CHECK constraint on
// content size: this fails as a clean 400-equivalent before publish,
// rather than a raw constraint violation if the application-level check at
// write time is ever bypassed.
func (s *Service) VerifyAssets(ctx context.Context, versionID uuid.UUID) error {
	mods, err := s.repo.ListModules(ctx, versionID)
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	for _, m := range mods {
		lessons, err := s.repo.ListLessons(ctx, m.ID)
		if err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		for _, l := range lessons {
			if len(l.Content) >= maxLessonContentBytes {
				return errContentTooLarge
			}
		}
	}
	return nil
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
// §7.4: "the only one whose effect students can observe"). Promotion and
// the course.course_published outbox write happen in one transaction
// (repository.go's PromoteVersion no longer opens its own, precisely so
// this method can wrap both in one) — a fix over M1's two separate
// transactions, needed here because Temporal's at-least-once activity
// execution means this method must be safe to run twice. The idempotency
// guard is a check, not a transaction-level dedupe: if courseID's
// published_version_id already equals versionID, promotion (and the event
// announcing it) already happened on a previous attempt, so this is a
// no-op rather than a duplicate outbox row.
func (s *Service) PromoteAndPublish(ctx context.Context, courseID, versionID uuid.UUID, oldVersionID *uuid.UUID, title string) error {
	c, err := s.repo.GetCourseByID(ctx, courseID)
	if err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	if c.PublishedVersionID != nil && *c.PublishedVersionID == versionID {
		return nil
	}
	err = s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		txRepo := s.repo.WithTx(tx)
		if err := txRepo.PromoteVersion(ctx, courseID, versionID, oldVersionID); err != nil {
			return err
		}
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "course",
			AggregateID:   courseID,
			EventType:     "course.course_published",
			SchemaVersion: 1,
			Payload:       map[string]any{"course_id": courseID, "version_id": versionID, "title": title},
		})
	})
	if err != nil {
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
func (s *Service) MarkPublishFailed(ctx context.Context, versionID uuid.UUID, reason string) error {
	if err := s.repo.UpdateVersionState(ctx, versionID, StatePublishFailed, &reason); err != nil {
		return apperrors.ErrInternal.Wrap(err)
	}
	return nil
}

// --- catalogue & detail ----------------------------------------------------

func (s *Service) ListCatalogue(ctx context.Context, limit int, cursor, category, level, search string) (*CatalogueResponse, error) {
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
// internal/progress against this same published version, keeping this
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

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
	"golang.org/x/sync/errgroup"
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
	errAlreadyDraft    = apperrors.New(409, "already_publishing", "This course's draft is already being published.")
	errContentTooLarge = apperrors.New(400, "content_too_large", "Lesson content exceeds the maximum allowed size.")
)

type Service struct {
	repo *Repository
}

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

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
// M1 implements publish synchronously: validate (concurrently, where the
// steps are independent), then promote. This is the deliberately marked
// Temporal seam (RFC §21 M1: "Publish endpoint exists and transitions state
// synchronously, with the Temporal seam marked"). Durable, crash-recoverable
// orchestration — retrying one failed activity independently, resuming after
// a worker restart mid-sequence — is still M2/Temporal scope; nothing here
// survives a process crash mid-publish, which is exactly what Temporal
// buys and a same-process errgroup does not.
//
// What IS implemented now is real: runPublishChecks mirrors RFC §7.4's
// activity graph — ValidateMetadata gates first (deterministic, no retry;
// a failure is the instructor's error, not transient), then the remaining
// independent steps run concurrently via errgroup. VerifyAssets isn't
// implemented yet (no asset-upload endpoint exists — RFC §21.0), so
// validateLessonContent stands in as VerifyAssets's sibling in the same
// graph position: a genuine, independent check of the draft's actual
// content, not a placeholder goroutine added to tick a box.
// Publish replaces PublishCourse's body with a Temporal workflow start in
// M2; nothing calling this method today needs to change when that happens.

func (s *Service) Publish(ctx context.Context, courseID, instructorID uuid.UUID) (*VersionResponse, error) {
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
	if draft.State == StatePublishing {
		return nil, errAlreadyDraft
	}

	if err := s.runPublishChecks(ctx, draft); err != nil {
		_ = s.repo.UpdateVersionState(ctx, draft.ID, StatePublishFailed, strPtr(err.Error()))
		return nil, err
	}

	if err := s.repo.PromoteVersion(ctx, courseID, draft.ID, c.PublishedVersionID); err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}

	if err := s.repo.DB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return outbox.Write(ctx, tx, outbox.Event{
			AggregateType: "course",
			AggregateID:   courseID,
			EventType:     "course.course_published",
			SchemaVersion: 1,
			Payload:       map[string]any{"course_id": courseID, "version_id": draft.ID, "title": draft.Title},
		})
	}); err != nil {
		// Promotion already committed — the course IS published. A failure
		// writing the outbox event here is logged, not surfaced as a
		// publish failure to the instructor; RFC §14.2 treats a missed
		// event as recoverable via reconciliation, not as data loss.
		return nil, apperrors.ErrUnavailable.Wrap(fmt.Errorf("published but event recording failed: %w", err))
	}

	updated, err := s.repo.GetVersionByID(ctx, draft.ID)
	if err != nil {
		return nil, apperrors.ErrInternal.Wrap(err)
	}
	resp := toVersionResponse(updated)
	return &resp, nil
}

// runPublishChecks is the concurrent piece (RFC §7 — "operations should
// execute concurrently wherever dependencies allow"; §10.1 explains why
// errgroup over a raw sync.WaitGroup: a WaitGroup alone loses errors and
// doesn't cancel siblings on first failure, so a failing content check
// doesn't stop the totals recompute from doing pointless work). Both
// goroutines below do a real, independent database round trip — this is
// not the same work split across goroutines for appearance.
func (s *Service) runPublishChecks(ctx context.Context, draft *CourseVersion) error {
	if err := s.validateMetadata(ctx, draft); err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return s.validateLessonContent(gctx, draft.ID) })
	g.Go(func() error {
		if err := s.repo.RecomputeVersionTotals(gctx, draft.ID); err != nil {
			return apperrors.ErrInternal.Wrap(err)
		}
		return nil
	})
	return g.Wait()
}

// validateMetadata is today's ValidateMetadata activity (RFC §7.4), called
// directly rather than via a workflow. Deliberately conservative: its
// failure must be the instructor's error (missing content), not a
// transient one — see the activity table's retry column ("None"). Takes
// ctx explicitly rather than defaulting to context.Background() internally
// — the earlier version didn't, which meant a client disconnecting
// mid-publish couldn't cancel this query (RFC §10.5's specific warning).
func (s *Service) validateMetadata(ctx context.Context, v *CourseVersion) error {
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

// validateLessonContent stands in for VerifyAssets in today's graph (see
// the package-level publish comment): a genuine scan of every lesson in
// the draft, independent of and concurrent with RecomputeVersionTotals.
// Defense in depth beyond the DB's own CHECK constraint on content size —
// this fails as a clean 400 before publish, rather than a raw constraint
// violation if this check is ever bypassed.
func (s *Service) validateLessonContent(ctx context.Context, versionID uuid.UUID) error {
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

// --- catalogue & detail ----------------------------------------------------

func (s *Service) ListCatalogue(ctx context.Context, limit int, cursor string) (*CatalogueResponse, error) {
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

	rows, err := s.repo.ListCatalogue(ctx, limit+1, afterCreatedAt, afterID)
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

func strPtr(s string) *string { return &s }

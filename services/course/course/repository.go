// The only file in this module that issues GORM/raw-SQL queries directly
// (RFC §5.9). GORM handles single-entity CRUD; the catalogue query uses
// raw SQL specifically to join instructor names in one query instead of
// N+1 (RFC §13.2) — joined against instructor_mirror, this module's own
// denormalized copy of display names (ADR-0024), not identity.users
// directly: identity and course are different Postgres schemas now, and
// no foreign key or join crosses that boundary. See handlers.go for how
// the mirror is kept current.
package course

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

func NewRepository(db *gorm.DB) *Repository { return &Repository{db: db} }

func (r *Repository) WithTx(tx *gorm.DB) *Repository { return &Repository{db: tx} }

// UpsertInstructorMirror records (or updates) one user's display name,
// fed by identity.user_registered and identity.user_profile_updated
// (handlers.go). eventID (the outbox row's own id,
// relay.DecodedEvent.OutboxID) guards against the relay's worker pool
// processing those two events out of order for the same user — same
// mechanism and same reason as analytics.UpsertUserRoleMirror.
func UpsertInstructorMirror(ctx context.Context, tx *gorm.DB, userID uuid.UUID, fullName string, eventID int64) error {
	return tx.WithContext(ctx).Exec(`
		INSERT INTO instructor_mirror (user_id, full_name, last_event_id, updated_at) VALUES (?, ?, ?, now())
		ON CONFLICT (user_id) DO UPDATE SET
			full_name = EXCLUDED.full_name,
			last_event_id = EXCLUDED.last_event_id,
			updated_at = EXCLUDED.updated_at
		WHERE EXCLUDED.last_event_id > instructor_mirror.last_event_id
	`, userID, fullName, eventID).Error
}
func (r *Repository) DB() *gorm.DB { return r.db }

var ErrNotFound = errors.New("course: not found")

func wrapNotFound(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	}
	return err
}

// --- courses ---------------------------------------------------------------

func (r *Repository) CreateCourse(ctx context.Context, c *Course) error {
	return r.db.WithContext(ctx).Create(c).Error
}

func (r *Repository) GetCourseByID(ctx context.Context, id uuid.UUID) (*Course, error) {
	var c Course
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&c).Error
	if err != nil {
		return nil, wrapNotFound(err)
	}
	return &c, nil
}

func (r *Repository) GetCourseBySlug(ctx context.Context, slug string) (*Course, error) {
	var c Course
	err := r.db.WithContext(ctx).Where("slug = ?", slug).First(&c).Error
	if err != nil {
		return nil, wrapNotFound(err)
	}
	return &c, nil
}

// ArchiveCourse sets archived_at rather than deleting the row (RFC's
// documented soft-delete design) — enrollment and progress history for
// students who took the course must survive, and ListCatalogue /
// enrollment.Enroll both already gate on archived_at IS NULL, so archiving
// takes effect everywhere that matters without a schema change.
func (r *Repository) ArchiveCourse(ctx context.Context, id uuid.UUID) error {
	return r.db.WithContext(ctx).Model(&Course{}).Where("id = ?", id).
		Update("archived_at", time.Now().UTC()).Error
}

func (r *Repository) SetDraftVersion(ctx context.Context, courseID, versionID uuid.UUID) error {
	return r.db.WithContext(ctx).Model(&Course{}).Where("id = ?", courseID).
		Update("draft_version_id", versionID).Error
}

// PromoteVersion is what makes a publish visible (ADR-0010) — swap
// published_version_id, mark the old version superseded, clear
// draft_version_id. It does NOT open its own transaction (unlike this
// file's other multi-step operations): it's called via r.WithTx from
// Service.PromoteAndPublish, in the same transaction as the
// course.course_published outbox write, so promotion and the event that
// announces it either both happen or neither does (M1 ran these as two
// separate transactions — a gap closed when this became a Temporal
// activity, which must be atomic to retry safely; see ADR-0025).
// Everything before this call operates on the draft only; nothing a
// student can observe changes until this commits.
func (r *Repository) PromoteVersion(ctx context.Context, courseID, newVersionID uuid.UUID, oldVersionID *uuid.UUID) error {
	if err := r.db.WithContext(ctx).Model(&Course{}).Where("id = ?", courseID).
		Updates(map[string]any{
			"published_version_id": newVersionID,
			"draft_version_id":     nil,
			"updated_at":           time.Now().UTC(),
		}).Error; err != nil {
		return err
	}
	if err := r.db.WithContext(ctx).Model(&CourseVersion{}).Where("id = ?", newVersionID).
		Updates(map[string]any{"state": StateReady, "published_at": time.Now().UTC()}).Error; err != nil {
		return err
	}
	if oldVersionID != nil {
		if err := r.db.WithContext(ctx).Model(&CourseVersion{}).Where("id = ?", *oldVersionID).
			Update("state", StateSuperseded).Error; err != nil {
			return err
		}
	}
	return nil
}

// SetVersionPublishing records that a publish workflow has started —
// transitions the draft to `publishing` and stores its Temporal workflow
// ID, so GetPublishStatus (and idx_one_draft_per_course, already in place
// since M0) both have something real to read. Called once, right after
// Service.Publish starts the workflow; never from inside the workflow
// itself.
func (r *Repository) SetVersionPublishing(ctx context.Context, id uuid.UUID, workflowID string) error {
	return r.db.WithContext(ctx).Model(&CourseVersion{}).Where("id = ?", id).
		Updates(map[string]any{"state": StatePublishing, "workflow_id": workflowID, "publish_error": nil}).Error
}

// GetInstructorName reads instructor_mirror the same way ListCatalogue's
// LEFT JOIN does — a miss (mirror not caught up yet, ADR-0024) returns ""
// rather than an error, same tradeoff ListCatalogue makes and for the same
// reason: an instructor's own very first publish racing their own
// just-registered event shouldn't fail the publish.
func (r *Repository) GetInstructorName(ctx context.Context, userID uuid.UUID) (string, error) {
	var name string
	err := r.db.WithContext(ctx).Raw(`SELECT full_name FROM instructor_mirror WHERE user_id = ?`, userID).Row().Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return name, err
}

// UpsertSearchIndex is IndexSearch's write (RFC §7.4, pulled forward from
// M3 per ADR-0025) — one row per course, replaced wholesale on every
// publish rather than diffed, since a full republish already recomputes
// every field BuildOutline collected. search_vector is built here, in SQL,
// not in Go: to_tsvector needs Postgres's text search configuration, and
// building it application-side would mean shipping that logic twice.
// Title is weighted 'A' (ranks above a description match) — RFC §21's D11
// defers final relevance tuning, so this is a reasonable starting weight,
// not a tuned one.
func (r *Repository) UpsertSearchIndex(ctx context.Context, o *PublishOutline) error {
	tags := o.Tags
	if tags == nil {
		tags = []string{}
	}
	return r.db.WithContext(ctx).Exec(`
		INSERT INTO course_search_index
			(course_id, version_id, title, description, instructor_name, category, level, tags, enrollment_count, search_vector, indexed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
			setweight(to_tsvector('english', ?), 'A') || setweight(to_tsvector('english', ?), 'B'), now())
		ON CONFLICT (course_id) DO UPDATE SET
			version_id = EXCLUDED.version_id, title = EXCLUDED.title, description = EXCLUDED.description,
			instructor_name = EXCLUDED.instructor_name, category = EXCLUDED.category, level = EXCLUDED.level,
			tags = EXCLUDED.tags, enrollment_count = EXCLUDED.enrollment_count,
			search_vector = EXCLUDED.search_vector, indexed_at = EXCLUDED.indexed_at
	`, o.CourseID, o.VersionID, o.Title, o.Description, o.InstructorName, o.Category, o.Level,
		pq.StringArray(tags), o.EnrollmentCount, o.Title, o.Description).Error
}

// SearchRow is SearchCourses' result row.
type SearchRow struct {
	CourseID        uuid.UUID
	Slug            string
	Title           string
	InstructorName  string
	Category        *string
	Level           *string
	EnrollmentCount int
}

// SearchCourses ranks by full-text relevance first, enrollment count as a
// tiebreaker — a real ranking, not just a filter, which is the difference
// between this and ListCatalogue's plain ILIKE search (RFC §21's D11:
// relevance weighting itself is still provisional, tuned later against
// real query logs). Joined against courses for the slug a search result
// actually needs to link to; course_search_index itself doesn't carry it.
func (r *Repository) SearchCourses(ctx context.Context, query string, limit int) ([]SearchRow, error) {
	rows, err := r.db.WithContext(ctx).Raw(`
		SELECT c.id, c.slug, csi.title, csi.instructor_name, csi.category, csi.level, csi.enrollment_count
		FROM course_search_index csi
		JOIN courses c ON c.id = csi.course_id
		WHERE csi.search_vector @@ plainto_tsquery('english', ?) AND c.archived_at IS NULL
		ORDER BY ts_rank(csi.search_vector, plainto_tsquery('english', ?)) DESC, csi.enrollment_count DESC
		LIMIT ?
	`, query, query, limit).Rows()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []SearchRow
	for rows.Next() {
		var row SearchRow
		if err := rows.Scan(&row.CourseID, &row.Slug, &row.Title, &row.InstructorName, &row.Category, &row.Level, &row.EnrollmentCount); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// --- versions ----------------------------------------------------------------

func (r *Repository) CreateVersion(ctx context.Context, v *CourseVersion) error {
	return r.db.WithContext(ctx).Create(v).Error
}

func (r *Repository) GetVersionByID(ctx context.Context, id uuid.UUID) (*CourseVersion, error) {
	var v CourseVersion
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&v).Error
	if err != nil {
		return nil, wrapNotFound(err)
	}
	return &v, nil
}

func (r *Repository) UpdateVersionState(ctx context.Context, id uuid.UUID, state VersionState, publishErr *string) error {
	return r.db.WithContext(ctx).Model(&CourseVersion{}).Where("id = ?", id).
		Updates(map[string]any{"state": state, "publish_error": publishErr}).Error
}

func (r *Repository) UpdateVersionMetadata(ctx context.Context, id uuid.UUID, title, description string, category, level *string, tags []string) error {
	if tags == nil {
		tags = []string{}
	}
	return r.db.WithContext(ctx).Model(&CourseVersion{}).Where("id = ?", id).
		Updates(map[string]any{
			"title": title, "description": description,
			"category": category, "level": level, "tags": pq.StringArray(tags),
		}).Error
}

// RecomputeVersionTotals keeps total_lessons/total_duration_s in sync after
// a module/lesson edit — a small denormalisation that keeps catalogue and
// detail reads from having to aggregate the content tree on every request.
func (r *Repository) RecomputeVersionTotals(ctx context.Context, versionID uuid.UUID) error {
	return r.db.WithContext(ctx).Exec(`
		UPDATE course_versions SET
			total_lessons = (SELECT count(*) FROM lessons l JOIN modules m ON m.id = l.module_id WHERE m.course_version_id = ?),
			total_duration_s = (SELECT coalesce(sum(l.duration_s), 0) FROM lessons l JOIN modules m ON m.id = l.module_id WHERE m.course_version_id = ?)
		WHERE id = ?`, versionID, versionID, versionID).Error
}

// --- modules & lessons ---------------------------------------------------

func (r *Repository) CreateModule(ctx context.Context, m *Module) error {
	return r.db.WithContext(ctx).Create(m).Error
}

func (r *Repository) GetModuleByID(ctx context.Context, id uuid.UUID) (*Module, error) {
	var m Module
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&m).Error
	if err != nil {
		return nil, wrapNotFound(err)
	}
	return &m, nil
}

func (r *Repository) UpdateModule(ctx context.Context, id uuid.UUID, title string) error {
	return r.db.WithContext(ctx).Model(&Module{}).Where("id = ?", id).Update("title", title).Error
}

func (r *Repository) DeleteModule(ctx context.Context, id uuid.UUID) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&Module{}).Error
}

func (r *Repository) ListModules(ctx context.Context, versionID uuid.UUID) ([]Module, error) {
	var mods []Module
	err := r.db.WithContext(ctx).Where("course_version_id = ?", versionID).Order("position").Find(&mods).Error
	return mods, err
}

func (r *Repository) CreateLesson(ctx context.Context, l *Lesson) error {
	return r.db.WithContext(ctx).Create(l).Error
}

func (r *Repository) GetLessonByID(ctx context.Context, id uuid.UUID) (*Lesson, error) {
	var l Lesson
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&l).Error
	if err != nil {
		return nil, wrapNotFound(err)
	}
	return &l, nil
}

func (r *Repository) UpdateLesson(ctx context.Context, id uuid.UUID, title, contentType string, content []byte, durationS int) error {
	return r.db.WithContext(ctx).Model(&Lesson{}).Where("id = ?", id).
		Updates(map[string]any{
			"title": title, "content_type": contentType, "content": content, "duration_s": durationS,
		}).Error
}

func (r *Repository) DeleteLesson(ctx context.Context, id uuid.UUID) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&Lesson{}).Error
}

func (r *Repository) ListLessons(ctx context.Context, moduleID uuid.UUID) ([]Lesson, error) {
	var lessons []Lesson
	err := r.db.WithContext(ctx).Where("module_id = ?", moduleID).Order("position").Find(&lessons).Error
	return lessons, err
}

// --- catalogue (keyset pagination, RFC §13.2) -------------------------------

type CatalogueRow struct {
	CourseID        uuid.UUID
	Slug            string
	Title           string
	InstructorName  string
	Category        *string
	Level           *string
	EnrollmentCount int
	CreatedAt       time.Time
}

// CatalogueFilter narrows ListCatalogue — every field is optional (zero
// value means "don't filter on this"). Search matches course_versions.title
// only; filtering by instructor name would need a second ILIKE against
// instructor_mirror.full_name, deliberately left out until a real need
// for it shows up.
type CatalogueFilter struct {
	Category *string
	Level    *string
	Search   string
}

// ListCatalogue returns published, non-archived courses ordered newest
// first, joined against the instructor's mirrored name in one query (RFC
// §13.2 — "no N+1"). LEFT JOIN, not JOIN: instructor_mirror is populated
// asynchronously from identity's events (handlers.go), so a course
// created in the same instant as its instructor's very first request
// could theoretically outrace the mirror — COALESCE falls back to an
// empty string rather than silently dropping the course from the
// catalogue over a timing gap that closes itself within one relay poll
// interval. Keyset, never OFFSET: afterCreatedAt/afterID identify the
// last row of the previous page; a nil afterID means "first page."
// filter's conditions are appended before the keyset condition, so args
// stay in the same order they appear in the query string regardless of
// which filters are set.
func (r *Repository) ListCatalogue(ctx context.Context, limit int, afterCreatedAt *time.Time, afterID *uuid.UUID, filter CatalogueFilter) ([]CatalogueRow, error) {
	q := `
		SELECT c.id, c.slug, cv.title, COALESCE(im.full_name, ''), cv.category, cv.level, c.enrollment_count, c.created_at
		FROM courses c
		JOIN course_versions cv ON cv.id = c.published_version_id
		LEFT JOIN instructor_mirror im ON im.user_id = c.instructor_id
		WHERE c.published_version_id IS NOT NULL AND c.archived_at IS NULL`
	args := []any{}
	if filter.Category != nil {
		q += ` AND cv.category = ?`
		args = append(args, *filter.Category)
	}
	if filter.Level != nil {
		q += ` AND cv.level = ?`
		args = append(args, *filter.Level)
	}
	if filter.Search != "" {
		q += ` AND cv.title ILIKE ?`
		args = append(args, "%"+filter.Search+"%")
	}
	if afterID != nil && afterCreatedAt != nil {
		q += ` AND (c.created_at, c.id) < (?, ?)`
		args = append(args, *afterCreatedAt, *afterID)
	}
	q += ` ORDER BY c.created_at DESC, c.id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := r.db.WithContext(ctx).Raw(q, args...).Rows()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []CatalogueRow
	for rows.Next() {
		var row CatalogueRow
		if err := rows.Scan(&row.CourseID, &row.Slug, &row.Title, &row.InstructorName, &row.Category, &row.Level, &row.EnrollmentCount, &row.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

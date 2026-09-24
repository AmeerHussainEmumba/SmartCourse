// The only file in this module that issues GORM/raw-SQL queries directly
// (RFC §5.9). GORM handles single-entity CRUD; the catalogue query uses
// raw SQL specifically to join instructor names in one query instead of
// N+1 (RFC §13.2) — a raw-SQL join against `users` is reading the same
// database, not importing internal/identity's Go types, so it doesn't
// violate the module-boundary rule (RFC §4.4).
package course

import (
	"context"
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
func (r *Repository) DB() *gorm.DB                   { return r.db }

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

func (r *Repository) SetDraftVersion(ctx context.Context, courseID, versionID uuid.UUID) error {
	return r.db.WithContext(ctx).Model(&Course{}).Where("id = ?", courseID).
		Update("draft_version_id", versionID).Error
}

// PromoteVersion is the single transaction that makes a publish visible
// (ADR-0010) — swap published_version_id, mark the old version superseded,
// clear draft_version_id. Everything before this call operates on the
// draft only; nothing a student can observe changes until this commits.
func (r *Repository) PromoteVersion(ctx context.Context, courseID, newVersionID uuid.UUID, oldVersionID *uuid.UUID) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&Course{}).Where("id = ?", courseID).
			Updates(map[string]any{
				"published_version_id": newVersionID,
				"draft_version_id":     nil,
				"updated_at":           time.Now().UTC(),
			}).Error; err != nil {
			return err
		}
		if err := tx.Model(&CourseVersion{}).Where("id = ?", newVersionID).
			Updates(map[string]any{"state": StateReady, "published_at": time.Now().UTC()}).Error; err != nil {
			return err
		}
		if oldVersionID != nil {
			if err := tx.Model(&CourseVersion{}).Where("id = ?", *oldVersionID).
				Update("state", StateSuperseded).Error; err != nil {
				return err
			}
		}
		return nil
	})
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

// ListCatalogue returns published, non-archived courses ordered newest
// first, joined against the instructor's name in one query (RFC §13.2 — "no
// N+1"). Keyset, never OFFSET: afterCreatedAt/afterID identify the last row
// of the previous page; a nil afterID means "first page."
func (r *Repository) ListCatalogue(ctx context.Context, limit int, afterCreatedAt *time.Time, afterID *uuid.UUID) ([]CatalogueRow, error) {
	q := `
		SELECT c.id, c.slug, cv.title, u.full_name, cv.category, cv.level, c.enrollment_count, c.created_at
		FROM courses c
		JOIN course_versions cv ON cv.id = c.published_version_id
		JOIN users u ON u.id = c.instructor_id
		WHERE c.published_version_id IS NOT NULL AND c.archived_at IS NULL`
	args := []any{}
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

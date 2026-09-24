// Explicit DTOs (RFC §11.5) — ORM models never bound or returned directly.
package course

type CreateCourseRequest struct {
	Title           string  `json:"title" binding:"required,min=1,max=200"`
	Slug            string  `json:"slug" binding:"required,min=1,max=200"` // format (lowercase, digits, hyphens) validated in service.go — see slugPattern
	Description     string  `json:"description" binding:"max=10000"`
	Category        *string `json:"category"`
	Level           *string `json:"level" binding:"omitempty,oneof=beginner intermediate advanced"`
	EnrollmentLimit *int    `json:"enrollment_limit" binding:"omitempty,min=1"`
}

type UpdateDraftRequest struct {
	Title       string   `json:"title" binding:"required,min=1,max=200"`
	Description string   `json:"description" binding:"max=10000"`
	Category    *string  `json:"category"`
	Level       *string  `json:"level" binding:"omitempty,oneof=beginner intermediate advanced"`
	Tags        []string `json:"tags" binding:"max=20"`
}

type CreateModuleRequest struct {
	Title    string `json:"title" binding:"required,min=1,max=200"`
	Position int    `json:"position" binding:"required,min=1"`
}

type CreateLessonRequest struct {
	Title       string         `json:"title" binding:"required,min=1,max=200"`
	Position    int            `json:"position" binding:"required,min=1"`
	ContentType string         `json:"content_type" binding:"required,oneof=video text pdf link"`
	Content     map[string]any `json:"content"`
	AssetKey    *string        `json:"asset_key"`
	DurationS   int            `json:"duration_s" binding:"min=0"`
}

type CourseResponse struct {
	ID                 string  `json:"id"`
	Slug               string  `json:"slug"`
	InstructorID       string  `json:"instructor_id"`
	PublishedVersionID *string `json:"published_version_id,omitempty"`
	DraftVersionID     *string `json:"draft_version_id,omitempty"`
	EnrollmentLimit    *int    `json:"enrollment_limit,omitempty"`
	EnrollmentCount    int     `json:"enrollment_count"`
}

type VersionResponse struct {
	ID          string   `json:"id"`
	State       string   `json:"state"`
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Category    *string  `json:"category,omitempty"`
	Level       *string  `json:"level,omitempty"`
	Tags        []string `json:"tags"`
}

type ModuleResponse struct {
	ID       string `json:"id"`
	Position int    `json:"position"`
	Title    string `json:"title"`
}

type LessonResponse struct {
	ID string `json:"id"`
	// LessonKey — not ID — is what internal/progress's completion endpoint
	// is keyed on (ADR-0011: stable across versions, unlike ID). It must be
	// exposed here or a client has no way to call
	// PUT /enrollments/{id}/lessons/{lessonKey}/complete at all — a gap
	// caught by an end-to-end smoke test, not by inspection.
	LessonKey   string         `json:"lesson_key"`
	Position    int            `json:"position"`
	Title       string         `json:"title"`
	ContentType string         `json:"content_type"`
	Content     map[string]any `json:"content"`
	DurationS   int            `json:"duration_s"`
}

type CatalogueCardResponse struct {
	CourseID        string  `json:"course_id"`
	Slug            string  `json:"slug"`
	Title           string  `json:"title"`
	InstructorName  string  `json:"instructor_name"`
	Category        *string `json:"category,omitempty"`
	Level           *string `json:"level,omitempty"`
	EnrollmentCount int     `json:"enrollment_count"`
}

type CatalogueResponse struct {
	Items      []CatalogueCardResponse `json:"items"`
	NextCursor *string                 `json:"next_cursor,omitempty"`
}

type ModuleWithLessons struct {
	ModuleResponse
	Lessons []LessonResponse `json:"lessons"`
}

// CourseDetailResponse deliberately nests Course/Version under named keys
// rather than embedding both structs — they'd otherwise collide on the
// promoted "ID" field (both have one) and Go silently drops ambiguously
// promoted fields from JSON output, which is exactly the kind of bug that
// only shows up by reading the response, not by reading the struct.
type CourseDetailResponse struct {
	Course  CourseResponse      `json:"course"`
	Version VersionResponse     `json:"version"`
	Modules []ModuleWithLessons `json:"modules"`
}

func toCourseResponse(c *Course) CourseResponse {
	resp := CourseResponse{
		ID: c.ID.String(), Slug: c.Slug, InstructorID: c.InstructorID.String(),
		EnrollmentLimit: c.EnrollmentLimit, EnrollmentCount: c.EnrollmentCount,
	}
	if c.PublishedVersionID != nil {
		s := c.PublishedVersionID.String()
		resp.PublishedVersionID = &s
	}
	if c.DraftVersionID != nil {
		s := c.DraftVersionID.String()
		resp.DraftVersionID = &s
	}
	return resp
}

func toVersionResponse(v *CourseVersion) VersionResponse {
	return VersionResponse{
		ID: v.ID.String(), State: string(v.State), Title: v.Title,
		Description: v.Description, Category: v.Category, Level: v.Level, Tags: []string(v.Tags),
	}
}

func toModuleResponse(m *Module) ModuleResponse {
	return ModuleResponse{ID: m.ID.String(), Position: m.Position, Title: m.Title}
}

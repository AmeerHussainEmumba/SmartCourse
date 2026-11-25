package main

import "fmt"

// seedPassword is shared by every account this tool creates except the admin
// (whose credentials come from SEED_ADMIN_EMAIL / SEED_ADMIN_PASSWORD and are
// created by identity-admin before the seed runs). It is printed at the end
// precisely so nobody has to go looking for it: this data never leaves a
// developer's own machine (ADR-0018's local-only compose profiles).
const seedPassword = "SeedPass123!"

const (
	roleStudent    = "student"
	roleInstructor = "instructor"

	statePublished = "published"
	stateDraft     = "draft"
	stateArchived  = "archived"
)

type moduleSpec struct {
	title   string
	lessons []string
}

// progressPlan enrols one student and marks completeFraction of the lessons
// complete, in lesson order (0 = enrolled, not started; 1 = course finished).
type progressPlan struct {
	student          int // index into students
	completeFraction float64
}

type courseSpec struct {
	title, slug, category, level string
	instructor                   int // index into instructors
	limit                        int // enrollment limit; 0 = none
	state                        string
	modules                      []moduleSpec
	plans                        []progressPlan
}

var instructorNames = []string{"Irfan Instructor", "Iman Instructor", "Idris Instructor"}

const studentCount = 12

func studentName(i int) string  { return fmt.Sprintf("Student %d", i+1) }
func studentEmail(i int) string { return fmt.Sprintf("student%d@smartcourse.dev", i+1) }

// instructorEmail turns "Irfan Instructor" into irfan-instructor@smartcourse.dev.
func instructorEmail(name string) string {
	out := make([]byte, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z':
			out = append(out, byte(r-'A'+'a'))
		case r >= 'a' && r <= 'z':
			out = append(out, byte(r))
		case r == ' ':
			out = append(out, '-')
		}
	}
	return string(out) + "@smartcourse.dev"
}

// dataset is the same data the former service-layer seed built, plus one
// archived course: every lifecycle state the platform has.
var dataset = []courseSpec{
	// Published, open enrollment, students at every progress stage (not
	// started / partial / complete): the main demo course.
	{
		title: "Introduction to Go", slug: "introduction-to-go", category: "backend", level: "beginner",
		instructor: 0, state: statePublished,
		modules: []moduleSpec{
			{title: "Getting Started", lessons: []string{"Installing Go", "Your First Program"}},
			{title: "Core Concepts", lessons: []string{"Structs and Methods", "Interfaces", "Goroutines and Channels"}},
		},
		plans: []progressPlan{{0, 1.0}, {1, 0.4}, {2, 0.0}},
	},
	// Published with a tight enrollment limit, filled to capacity: the seat
	// limit (ADR-0012) with real data.
	{
		title: "Advanced PostgreSQL", slug: "advanced-postgresql", category: "backend", level: "advanced",
		instructor: 0, limit: 2, state: statePublished,
		modules: []moduleSpec{
			{title: "Indexing", lessons: []string{"B-tree vs GIN", "Partial Indexes"}},
		},
		plans: []progressPlan{{3, 0.5}, {4, 0.0}},
	},
	// Published, nobody enrolled yet: a freshly live course.
	{
		title: "Distributed Systems Essentials", slug: "distributed-systems-essentials", category: "backend", level: "intermediate",
		instructor: 1, state: statePublished,
		modules: []moduleSpec{
			{title: "Consensus", lessons: []string{"Why Consensus Is Hard", "Raft, In Practice"}},
		},
	},
	// Draft, never published: the instructor is still working on it.
	{
		title: "Kubernetes in Production", slug: "kubernetes-in-production", category: "devops", level: "advanced",
		instructor: 2, state: stateDraft,
		modules: []moduleSpec{
			{title: "Scheduling", lessons: []string{"Pods and Deployments"}},
		},
	},
	// Published, one small module: the first course a new student takes.
	{
		title: "Security Fundamentals", slug: "security-fundamentals", category: "security", level: "beginner",
		instructor: 2, state: statePublished,
		modules: []moduleSpec{
			{title: "Basics", lessons: []string{"Threat Modeling 101"}},
		},
		plans: []progressPlan{{5, 1.0}},
	},
	// Published, then archived: hidden from the catalogue, no enrollments.
	{
		title: "Legacy Shell Scripting", slug: "legacy-shell-scripting", category: "devops", level: "beginner",
		instructor: 1, state: stateArchived,
		modules: []moduleSpec{
			{title: "Bash", lessons: []string{"Quoting Rules"}},
		},
	},
}

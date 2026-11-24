// Package harness is the container-tier stack of the Week 3 end-to-end
// scenarios (docs/plans/week-3.md section 5, test/README.md): real Postgres,
// Redis, Kafka, Schema Registry, MongoDB, Mailpit and Temporal through
// testcontainers, and the real worker, identity-api, course-api and
// analytics-api binaries run as subprocesses with the environment the compose
// file gives them.
//
// A scenario is written against Stack:
//
//	s := harness.Start(t, harness.Options{})   // containers + binaries, ready
//	inst := s.NewInstructor(t)                 // register, promote, re-login
//	course := s.PublishedCourse(t, inst, 2)    // create + publish through Temporal
//	student := s.NewStudent(t)
//	enr := s.Enroll(t, student, course)
//	harness.Eventually(t, time.Minute, "one mail", func() error { ... })
//
// Where things live: stack.go (what runs and how it is wired), infra.go
// (containers shared by the whole test binary: Schema Registry and Temporal),
// proc.go and build.go (binaries as subprocesses), api.go (HTTP helpers),
// mailpit.go, mongo.go, kafka.go, metrics.go and workeradmin.go (readers and
// fault controls for each dependency), poll.go (Eventually).
//
// The package imports no service internals beyond the module graph allows
// (test/e2e may require libs/* and services/identity and services/course
// only, scripts/check-module-graph.sh), so the worker and analytics-api are
// built with `go build` and run as processes, never imported.
package harness

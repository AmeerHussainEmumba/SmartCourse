// Package harness is the container-tier stack of the Week 3 end-to-end
// scenarios (docs/plans/week-3.md section 5, test/README.md): real Postgres
// (the real roles script and every migration), Redis, Kafka, a Confluent
// Schema Registry, MongoDB, Mailpit and Temporal through testcontainers, and
// the ten service binaries (identity-api, identity-worker, course-api,
// course-worker, analytics-api, analytics-worker, notification-worker,
// publishing-worker, audit-api, audit-worker) run as subprocesses, each with
// exactly the environment of its configuration table and its own Postgres
// role. The first admin is made by the built identity-admin CLI.
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
// Where things live: stack.go (Start, per-binary Start/Stop/Kill, readiness,
// SQL readers), topology.go (what each binary reads: the environment tables),
// infra.go (containers shared by the whole test binary: Schema Registry and
// Temporal), proc.go and build.go (binaries as subprocesses), api.go (HTTP
// helpers), audit.go (failed events through audit-api), mailpit.go, mongo.go,
// kafka.go and metrics.go (readers and fault controls for each dependency),
// poll.go (Eventually).
//
// The package imports no service module (test/e2e requires libs/* only,
// scripts/check-module-graph.sh): the binaries are built with `go build` and
// run as processes, never imported.
package harness

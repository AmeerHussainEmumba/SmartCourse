package main

// Wiring guard: buildRelay and buildPublishWorker are the ONLY place that
// connects the services' handlers and workflow to the worker process. If
// someone drops a Register loop or the RegisterWorkflow call, every other test
// still passes (each module's handlers are tested in isolation) and the
// feature silently stops working in production. These tests compare what the
// worker registers against (a) what each module exports and (b) what the
// producers actually emit.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	temporalclient "go.temporal.io/sdk/client"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/infra"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/relay"
	"github.com/AmeerHussainEmumba/SmartCourse/services/analytics/analytics"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/certificate"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/enrollment"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/workflows"
	"github.com/AmeerHussainEmumba/SmartCourse/services/worker/notification"
)

// registeredPairs reads relay.Relay's unexported handler list via reflection
// (the production type offers no accessor) and returns sorted
// "consumer|eventType" pairs.
func registeredPairs(t *testing.T, rl *relay.Relay) []string {
	t.Helper()
	hs := reflect.ValueOf(rl).Elem().FieldByName("handlers")
	if !hs.IsValid() {
		t.Fatal("relay.Relay no longer has a `handlers` field; update this guard")
	}
	var out []string
	for i := 0; i < hs.Len(); i++ {
		h := hs.Index(i)
		name := h.FieldByName("ConsumerName").String()
		if h.FieldByName("Handle").IsNil() {
			t.Errorf("handler %d (%s) has a nil Handle func", i, name)
		}
		keys := h.FieldByName("EventTypes").MapKeys()
		if len(keys) == 0 {
			t.Errorf("consumer %s registered with zero event types", name)
		}
		for _, k := range keys {
			out = append(out, name+"|"+k.String())
		}
	}
	sort.Strings(out)
	return out
}

func pairsOf(hs []relay.Handler) []string {
	var out []string
	for _, h := range hs {
		for et := range h.EventTypes {
			out = append(out, h.ConsumerName+"|"+et)
		}
	}
	sort.Strings(out)
	return out
}

func TestBuildRelay_RegistersExactlyTheModulesHandlers(t *testing.T) {
	rl := buildRelay(&infra.Dependencies{}, relayTestConfig())
	got := registeredPairs(t, rl)

	var want []string
	want = append(want, pairsOf(analytics.Handlers())...)
	want = append(want, pairsOf(course.Handlers())...)
	want = append(want, pairsOf(enrollment.Handlers())...)
	want = append(want, pairsOf(certificate.Handlers())...)
	want = append(want, pairsOf(notification.Handlers())...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildRelay does not register exactly analytics+course+enrollment+certificate+notification handlers\n got: %v\nwant: %v", got, want)
	}

	// Literal expectation, so deleting a handler from a module (and so from
	// both sides of the comparison above) is also caught.
	literal := []string{
		"analytics-aggregator|course.course_published",
		"analytics-aggregator|course.publish_failed",
		"analytics-aggregator|enrollment.student_enrolled",
		"analytics-aggregator|enrollment.student_withdrawn",
		"analytics-aggregator|identity.user_registered",
		"analytics-aggregator|identity.user_role_changed",
		"analytics-aggregator|progress.course_completed",
		"certificate-issuer|progress.course_completed",
		"course-instructor-mirror|identity.user_profile_updated",
		"course-instructor-mirror|identity.user_registered",
		"enrollment-course-archival|course.course_archived",
		"notification-dispatcher|course.course_published",
		"notification-dispatcher|course.publish_failed",
		"notification-dispatcher|enrollment.student_enrolled",
		"notification-dispatcher|identity.user_profile_updated",
		"notification-dispatcher|identity.user_registered",
		"notification-dispatcher|progress.certificate_issued",
		"notification-dispatcher|progress.course_completed",
	}
	if !reflect.DeepEqual(got, literal) {
		t.Fatalf("registered consumers/event types changed.\n got: %v\nwant: %v\nIf this is intentional, update this guard and the docs.", got, literal)
	}
}

func TestBuildRelay_NoDuplicateConsumerEventPairs(t *testing.T) {
	// processed_events is keyed (consumer_name, event_id): two handlers that
	// share a consumer name AND an event type would make the second one a
	// silent no-op.
	seen := map[string]bool{}
	for _, p := range registeredPairs(t, buildRelay(&infra.Dependencies{}, relayTestConfig())) {
		if seen[p] {
			t.Errorf("duplicate registration %s", p)
		}
		seen[p] = true
	}
}

// Every event type any service emits (outbox.Event{EventType: "..."}) must
// have at least one registered consumer, or the relay marks it published and
// nobody ever acts on it (relay.process treats "no handler" as success).
func TestEveryEmittedEventTypeHasAConsumer(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	servicesDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	emitted := map[string]string{}
	// Producers name their event types either with a string literal or with an
	// exported Event* constant (services/course/{progress,certificate,course}).
	// The constants are collected first so both forms are seen.
	consts := map[string]string{}
	type usage struct{ name, file string }
	var viaConst []usage
	fset := token.NewFileSet()
	err := filepath.Walk(servicesDir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return err
		}
		f, perr := parser.ParseFile(fset, p, nil, 0)
		if perr != nil {
			return perr
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.ValueSpec:
				for i, name := range n.Names {
					if i < len(n.Values) && strings.HasPrefix(name.Name, "Event") {
						if bl, ok := n.Values[i].(*ast.BasicLit); ok && bl.Kind == token.STRING {
							consts[name.Name], _ = strconv.Unquote(bl.Value)
						}
					}
				}
			case *ast.KeyValueExpr:
				if id, ok := n.Key.(*ast.Ident); ok && id.Name == "EventType" {
					switch v := n.Value.(type) {
					case *ast.BasicLit:
						if v.Kind == token.STRING {
							s, _ := strconv.Unquote(v.Value)
							emitted[s] = p
						}
					case *ast.Ident:
						viaConst = append(viaConst, usage{v.Name, p})
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range viaConst {
		val, ok := consts[u.name]
		if !ok {
			t.Errorf("EventType: %s in %s is not an Event* string constant this guard can resolve", u.name, filepath.Base(u.file))
			continue
		}
		emitted[val] = u.file
	}
	if len(emitted) < 8 {
		t.Fatalf("expected at least 8 emitted event types, found %d: %v", len(emitted), emitted)
	}
	consumed := map[string]bool{}
	for _, p := range registeredPairs(t, buildRelay(&infra.Dependencies{}, relayTestConfig())) {
		consumed[strings.SplitN(p, "|", 2)[1]] = true
	}
	// Events no relay handler consumes by design. progress.lesson_completed is
	// archived (the audit-archiver subscribes to every events.Types() entry) but
	// no RFC consumer needs it yet: RFC §4.6 lists "progress metrics" for it, and
	// the completion figures analytics reports derive from course_completed.
	archiveOnly := map[string]bool{"progress.lesson_completed": true}
	for et := range archiveOnly {
		if _, ok := events.Lookup(et); !ok {
			t.Errorf("%s is listed as archive-only but is not in events.Types(), so nothing consumes it", et)
		}
		if consumed[et] {
			t.Errorf("%s is listed as archive-only but has a consumer: remove it from the list", et)
		}
	}
	for et, file := range emitted {
		if !consumed[et] && !archiveOnly[et] {
			t.Errorf("event type %q (emitted in %s) has no registered consumer in the worker", et, filepath.Base(file))
		}
	}
	for et := range consumed {
		if _, ok := emitted[et]; !ok {
			t.Errorf("worker consumes %q but no production code emits it (typo in an event name?)", et)
		}
	}
}

// buildPublishWorker must register the workflow under the exact name
// course.Service.Publish starts it by, and every Activities method.
func TestBuildPublishWorker_RegistersWorkflowNameAndAllActivities(t *testing.T) {
	cl, err := temporalclient.NewLazyClient(temporalclient.Options{}) // no connection is made
	if err != nil {
		t.Fatalf("lazy client: %v", err)
	}
	defer cl.Close()
	cfg := &config.Config{Temporal: config.TemporalConfig{TaskQueue: "wiring-test-queue"}}
	w := buildPublishWorker(&infra.Dependencies{}, cfg, cl)

	reg := reflect.ValueOf(w).Elem().FieldByName("registry")
	if !reg.IsValid() {
		t.Fatal("temporal AggregatedWorker no longer has a `registry` field; update this guard")
	}
	reg = reg.Elem()
	var wfNames, actNames []string
	for _, k := range reg.FieldByName("workflowFuncMap").MapKeys() {
		wfNames = append(wfNames, k.String())
	}
	for _, k := range reg.FieldByName("activityFuncMap").MapKeys() {
		actNames = append(actNames, k.String())
	}
	sort.Strings(wfNames)
	sort.Strings(actNames)

	if course.PublishWorkflowName == "" {
		t.Fatal("course.PublishWorkflowName is empty")
	}
	if !reflect.DeepEqual(wfNames, []string{course.PublishWorkflowName}) {
		t.Fatalf("registered workflows = %v, want exactly [%s]", wfNames, course.PublishWorkflowName)
	}

	wantActs := []string{"BuildOutline", "ComputeMetadata", "IndexSearch", "MarkPublishFailed", "PromoteAndPublish", "ValidateMetadata", "VerifyAssets", "WarmCache"}
	if !reflect.DeepEqual(actNames, wantActs) {
		t.Fatalf("registered activities = %v\nwant %v", actNames, wantActs)
	}
	// And the literal list must track the Activities type itself.
	at := reflect.TypeOf(&workflows.Activities{})
	var methods []string
	for i := 0; i < at.NumMethod(); i++ {
		methods = append(methods, at.Method(i).Name)
	}
	sort.Strings(methods)
	if !reflect.DeepEqual(methods, wantActs) {
		t.Fatalf("workflows.Activities methods %v differ from the guard's list %v: a new activity must be registered AND added here", methods, wantActs)
	}
}

// The worker runs with POSTGRES_SEARCH_PATH=platform,course,analytics,public
// (Makefile run-worker, docker-compose). Bare table names resolve in that
// order, so a table name that exists in two schemas would silently be read
// from the wrong one. No two services may share a table name.
func TestNoTableNameExistsInTwoSchemas(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "..", "migrations")
	re := regexp.MustCompile(`(?im)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_][a-z0-9_]*)`)
	owner := map[string]string{}
	for _, schema := range []string{"platform", "identity", "course", "analytics"} {
		files, _ := filepath.Glob(filepath.Join(root, schema, "*.up.sql"))
		if len(files) == 0 {
			t.Fatalf("no migrations for %s in %s", schema, root)
		}
		for _, f := range files {
			b, _ := os.ReadFile(f)
			for _, m := range re.FindAllStringSubmatch(string(b), -1) {
				if prev, dup := owner[m[1]]; dup && prev != schema {
					t.Errorf("table %q is created in both %s and %s", m[1], prev, schema)
				}
				owner[m[1]] = schema
			}
		}
	}
	if len(owner) < 20 {
		t.Fatalf("only %d tables parsed; migration scan is broken", len(owner))
	}
}

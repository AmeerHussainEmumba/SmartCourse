//go:build e2e_legacy

package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/events"
	enrollmentv1 "github.com/AmeerHussainEmumba/SmartCourse/libs/events/gen/smartcourse/enrollment/v1"
	"github.com/AmeerHussainEmumba/SmartCourse/libs/events/registry"
	"github.com/AmeerHussainEmumba/SmartCourse/test/e2e/harness"
)

// TestE2E_E11_SchemaIncompatibility is E11, matrix row E11 (Phase 2).
//
// Steps and assertions, each a subtest against the shared REAL Confluent
// Schema Registry the harness starts (every registration below that must not
// change shared state is a read-only check or a rejected write):
//  1. The bootstrap tool (libs/events/cmd/register-schemas, the command a
//     deploy runs) is run twice: both runs exit 0 and print the same id and
//     version for every subject; every subject of events.Subjects() exists with
//     compatibility BACKWARD; the latest version of no subject moved.
//  2. The StudentEnrolled payload schema is taken from the registry. A
//     breaking variant (student_id changes from string to int64) is not
//     compatible (/compatibility/subjects/<subject>/versions/latest answers
//     is_compatible false) and registering it is refused with 409, the
//     subject's latest version number unchanged.
//  3. The CI check: the same bootstrap tool built from a temporary copy of
//     libs/events with that breaking edit applied to the .proto exits non-zero
//     and names the subject; the unedited tool exited 0 in step 1. (The tool
//     reports that the schema is not BACKWARD-compatible and names the
//     subject; it does not name the field that broke: noted, not asserted.)
//  4. A backward-compatible change (an added field) is compatible; on a
//     scratch subject seeded with the same schema it is accepted and the
//     version increments to 2 (a real subject is never given a new version by
//     this test).
//  5. Old-version payloads still decode with the current schema: the golden
//     frames committed under libs/events/testdata (written by an earlier
//     version) and a payload written before enrolled_at existed (fields 1 to 3
//     only) decode with the real codec and the generated message.
//  6. The worker refuses to start in kafka mode when a subject is missing: a
//     second worker process is pointed at the real registry through a proxy
//     that answers 404 (subject not found, error_code 40401) for the
//     StudentEnrolled subject only; it exits non-zero and says which subject
//     is missing. (A proxy, not a deletion, so the shared registry is not
//     changed for the scenarios that follow.)
func TestE2E_E11_SchemaIncompatibility(t *testing.T) {
	regURL := harness.Shared(t).RegistryURL
	reg := registry.New(regURL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	subjects, err := events.Subjects()
	if err != nil {
		t.Fatal(err)
	}
	enrolled, ok := events.Lookup("enrollment.student_enrolled")
	if !ok {
		t.Fatal("events catalogue has no enrollment.student_enrolled")
	}
	subject := enrolled.Subject()
	var source string // the .proto text the bootstrap tool registers for StudentEnrolled
	for _, s := range subjects {
		if s.Name == subject {
			source = s.Schema
		}
	}
	if source == "" {
		t.Fatalf("events.Subjects() has no %s", subject)
	}

	t.Run("bootstrap is idempotent and every subject is BACKWARD", func(t *testing.T) {
		versions := map[string]int{}
		for _, s := range subjects {
			v, err := reg.LatestVersion(ctx, s.Name)
			if err != nil {
				t.Fatalf("subject %s is not in the registry: %v", s.Name, err)
			}
			versions[s.Name] = v
			if level, ok, err := reg.Compatibility(ctx, s.Name); err != nil || !ok || level != registry.Backward {
				t.Errorf("subject %s compatibility %q (own config %v, err %v), want BACKWARD", s.Name, level, ok, err)
			}
		}
		first, second := runRegisterSchemas(t, harness.Binary(t, "register-schemas"), regURL), runRegisterSchemas(t, harness.Binary(t, "register-schemas"), regURL)
		if first.err != nil || second.err != nil {
			t.Fatalf("register-schemas exits: first %v, second %v\n%s\n%s", first.err, second.err, first.out, second.out)
		}
		if first.out != second.out {
			t.Errorf("two runs printed different results:\n%s\n---\n%s", first.out, second.out)
		}
		for _, s := range subjects {
			if !strings.Contains(first.out, s.Name) {
				t.Errorf("register-schemas did not report subject %s", s.Name)
			}
			if v, err := reg.LatestVersion(ctx, s.Name); err != nil || v != versions[s.Name] {
				t.Errorf("subject %s is at version %d after the bootstrap (err %v), was %d: not idempotent", s.Name, v, err, versions[s.Name])
			}
		}
	})

	breaking := strings.Replace(source, "string student_id = 2;", "int64 student_id = 2;", 1)
	if breaking == source {
		t.Fatal("the breaking edit did not apply: the StudentEnrolled schema no longer has `string student_id = 2;`")
	}
	t.Run("a breaking schema is incompatible and refused with 409", func(t *testing.T) {
		before, err := reg.LatestVersion(ctx, subject)
		if err != nil {
			t.Fatal(err)
		}
		if compatible, err := reg.IsCompatible(ctx, subject, breaking); err != nil || compatible {
			t.Errorf("breaking variant: is_compatible %v (err %v), want false", compatible, err)
		}
		_, err = reg.Register(ctx, subject, breaking)
		var apiErr *registry.APIError
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusConflict {
			t.Errorf("registering the breaking variant: %v, want an HTTP 409 incompatible-schema error", err)
		}
		if after, err := reg.LatestVersion(ctx, subject); err != nil || after != before {
			t.Errorf("latest version of %s is %d after the refused registration (err %v), was %d", subject, after, err, before)
		}
		// Observation, not an assertion: what the registry thinks of a rename
		// plus a renumbering (wire-compatible by field number).
		renamed := strings.Replace(source, "string course_id = 3;", "string course_ref = 3;", 1)
		renumbered := strings.Replace(source, "string student_id = 2;", "string student_id = 9;", 1)
		for name, v := range map[string]string{"rename course_id": renamed, "renumber student_id 2 to 9": renumbered} {
			ok, err := reg.IsCompatible(ctx, subject, v)
			t.Logf("registry verdict for %q: is_compatible=%v (err %v)", name, ok, err)
		}
	})

	t.Run("the CI check fails on the breaking edit", func(t *testing.T) {
		tmp := t.TempDir()
		copyDir := filepath.Join(tmp, "events")
		if err := os.CopyFS(copyDir, os.DirFS(filepath.Join(harness.RepoRoot(), "libs", "events"))); err != nil {
			t.Fatalf("copy libs/events: %v", err)
		}
		protoFiles, _ := filepath.Glob(filepath.Join(copyDir, "proto", "smartcourse", "enrollment", "v1", "*.proto"))
		edited := 0
		for _, f := range protoFiles {
			b, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			if out := strings.Replace(string(b), "string student_id = 2;", "int64 student_id = 2;", 1); out != string(b) {
				if err := os.WriteFile(f, []byte(out), 0o644); err != nil {
					t.Fatal(err)
				}
				edited++
			}
		}
		if edited != 1 {
			t.Fatalf("applied the breaking edit to %d proto files in the temporary copy, want 1", edited)
		}
		bin := filepath.Join(tmp, "register-schemas")
		build := exec.Command("go", "build", "-o", bin, "./cmd/register-schemas")
		build.Dir = copyDir
		build.Env = append(os.Environ(), "GOWORK=off")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build register-schemas from the edited copy: %v\n%s", err, out)
		}
		res := runRegisterSchemas(t, bin, regURL)
		if res.err == nil {
			t.Fatalf("register-schemas accepted the breaking edit (exit 0):\n%s", res.out)
		}
		if !strings.Contains(res.out, subject) {
			t.Errorf("the failure does not name the subject %s:\n%s", subject, res.out)
		}
		if !strings.Contains(strings.ToLower(res.out), "not backward-compatible") && !strings.Contains(strings.ToLower(res.out), "incompatible") {
			t.Errorf("the failure does not say the schema is incompatible:\n%s", res.out)
		}
		t.Logf("register-schemas on the edited copy: %v: %s", res.err, strings.TrimSpace(res.out))
		if v, err := reg.LatestVersion(ctx, subject); err != nil || v < 1 {
			t.Errorf("latest version of %s after the failed check: %d (err %v)", subject, v, err)
		}
	})

	t.Run("an added field is backward compatible", func(t *testing.T) {
		additive := strings.Replace(source, "google.protobuf.Timestamp enrolled_at = 4;", "google.protobuf.Timestamp enrolled_at = 4;\n  string source = 5;", 1)
		if additive == source {
			t.Fatal("the additive edit did not apply")
		}
		if compatible, err := reg.IsCompatible(ctx, subject, additive); err != nil || !compatible {
			t.Errorf("added optional field on the real subject: is_compatible %v (err %v), want true", compatible, err)
		}
		scratch := "e2e-e11-" + uuid.NewString()[:8] + "-StudentEnrolled"
		if err := reg.SetCompatibility(ctx, scratch, registry.Backward); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.Register(ctx, scratch, source); err != nil {
			t.Fatalf("seed the scratch subject: %v", err)
		}
		if v, err := reg.LatestVersion(ctx, scratch); err != nil || v != 1 {
			t.Fatalf("scratch subject at version %d (err %v), want 1", v, err)
		}
		if _, err := reg.Register(ctx, scratch, additive); err != nil {
			t.Fatalf("registering the added field on the scratch subject: %v", err)
		}
		if v, err := reg.LatestVersion(ctx, scratch); err != nil || v != 2 {
			t.Errorf("scratch subject at version %d (err %v) after the compatible change, want 2", v, err)
		}
		if _, err := reg.Register(ctx, scratch, breaking); err == nil {
			t.Errorf("the breaking variant was accepted on the scratch subject")
		}
	})

	t.Run("old-version payloads still decode", func(t *testing.T) {
		golden := filepath.Join(harness.RepoRoot(), "libs", "events", "testdata", "golden")
		bins, _ := filepath.Glob(filepath.Join(golden, "*.bin"))
		if len(bins) == 0 {
			t.Fatalf("no golden frames under %s", golden)
		}
		for _, f := range bins {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			var env events.Envelope
			if _, err := events.DecodeInto(raw, &env); err != nil {
				t.Errorf("%s no longer decodes: %v", filepath.Base(f), err)
				continue
			}
			if _, err := events.OpenPayload(&env); err != nil {
				t.Errorf("%s: payload does not open: %v", filepath.Base(f), err)
			}
		}
		// A StudentEnrolled written before enrolled_at (field 4) existed.
		var old []byte
		for i, v := range []string{"enrollment-1", "student-1", "course-1"} {
			old = protowire.AppendTag(old, protowire.Number(i+1), protowire.BytesType)
			old = protowire.AppendString(old, v)
		}
		var msg enrollmentv1.StudentEnrolled
		if err := proto.Unmarshal(old, &msg); err != nil {
			t.Fatalf("an old payload no longer decodes: %v", err)
		}
		if msg.GetEnrollmentId() != "enrollment-1" || msg.GetStudentId() != "student-1" || msg.GetCourseId() != "course-1" || msg.GetEnrolledAt() != nil {
			t.Errorf("old payload decoded as %v", &msg)
		}
		// And a payload from a newer writer (an unknown field 5) decodes too.
		newer := protowire.AppendString(protowire.AppendTag(append([]byte(nil), old...), 5, protowire.BytesType), "web")
		var fromNewer enrollmentv1.StudentEnrolled
		if err := proto.Unmarshal(newer, &fromNewer); err != nil || fromNewer.GetCourseId() != "course-1" {
			t.Errorf("a payload with an added field does not decode with the current schema: %v", err)
		}
	})

	t.Run("the worker refuses to start when a subject is missing", func(t *testing.T) {
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && r.URL.Path == "/subjects/"+subject+"/versions/latest" {
				w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
				w.WriteHeader(http.StatusNotFound)
				_, _ = fmt.Fprintf(w, `{"error_code":40401,"message":"Subject '%s' not found."}`, subject)
				return
			}
			up, err := http.NewRequestWithContext(r.Context(), r.Method, regURL+r.URL.RequestURI(), r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			up.Header = r.Header.Clone()
			resp, err := http.DefaultClient.Do(up)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			for k, vs := range resp.Header {
				for _, v := range vs {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
		}))
		defer proxy.Close()

		s := harness.Start(t, harness.Options{})
		p := s.NewWorkerProc(t, map[string]string{"SCHEMA_REGISTRY_URL": proxy.URL})
		if err := p.Start(); err != nil {
			t.Fatal(err)
		}
		if !p.Exited(90 * time.Second) {
			t.Fatalf("a worker with a missing subject is still running after 90 s; it must refuse to start. Output:\n%s", p.Logs())
		}
		logs := p.Logs()
		if !strings.Contains(logs, subject) || !strings.Contains(logs, "missing") {
			t.Errorf("the worker's exit does not say which subject is missing:\n%s", tail(logs, 2000))
		}
		if p.LogContains("kafka consumer started") || p.LogContains("publish worker started") {
			t.Errorf("the worker started consumers before it refused to start")
		}
	})
}

type toolRun struct {
	out string
	err error
}

func runRegisterSchemas(t *testing.T, bin, registryURL string) toolRun {
	t.Helper()
	var out bytes.Buffer
	cmd := exec.Command(bin, "-registry", registryURL)
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return toolRun{out: out.String(), err: err}
}

func tail(s string, n int) string {
	if len(s) > n {
		return "..." + s[len(s)-n:]
	}
	return s
}

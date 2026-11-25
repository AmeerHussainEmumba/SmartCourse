package harness

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

// binary is a program the harness builds from another module of the repo.
type binary struct {
	name string // output file name
	dir  string // module directory, relative to the repo root
	pkg  string // package inside that module
}

// binaries are the ten service binaries, the first-admin CLI (identity-admin,
// run once per stack, never a long-running process) and the schema tool the
// harness runs against the shared registry.
var binaries = []binary{
	{"identity-api", "services/identity", "./cmd/identity-api"},
	{"identity-worker", "services/identity", "./cmd/identity-worker"},
	{"identity-admin", "services/identity", "./cmd/identity-admin"},
	{"course-api", "services/course", "./cmd/course-api"},
	{"course-worker", "services/course", "./cmd/course-worker"},
	{"analytics-api", "services/analytics", "./cmd/analytics-api"},
	{"analytics-worker", "services/analytics", "./cmd/analytics-worker"},
	{"notification-worker", "services/notification", "./cmd/notification-worker"},
	{"publishing-worker", "services/publishing", "./cmd/publishing-worker"},
	{"audit-api", "services/audit", "./cmd/audit-api"},
	{"audit-worker", "services/audit", "./cmd/audit-worker"},
	{"register-schemas", "libs/events", "./cmd/register-schemas"},
}

var (
	buildOnce sync.Once
	buildDir  string
	buildErr  error
)

// RepoRoot is the repository root, found from this file's location.
func RepoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	// test/e2e/harness/build.go -> repo root
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

// Binary returns the path of a built binary, building all of them once per
// test process. `go build` runs inside each module directory, so it works
// with or without go.work (CI uses GOWORK=off; the modules' replace
// directives resolve the sibling libs). The go build cache makes a repeat run
// cheap; a cold build of the services takes a minute or two.
func Binary(t testing.TB, name string) string {
	t.Helper()
	buildOnce.Do(func() { buildDir, buildErr = buildAll() })
	if buildErr != nil {
		t.Fatalf("build binaries: %v", buildErr)
	}
	return filepath.Join(buildDir, name)
}

func buildAll() (string, error) {
	// Stable directory, so a second `go test` run reuses the output when the
	// build cache says nothing changed. It is keyed by the checkout it was
	// built from: two worktrees running the suite at once must not overwrite
	// (and then run) each other's binaries.
	root := RepoRoot()
	sum := sha256.Sum256([]byte(root))
	dir := filepath.Join(os.TempDir(), "smartcourse-e2e-bin-"+hex.EncodeToString(sum[:4]))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	var wg sync.WaitGroup
	errs := make([]error, len(binaries))
	// Bound the parallel compiles: a dozen linkers at once is more than a
	// laptop that is also running Docker needs.
	sem := make(chan struct{}, 4)
	for i, b := range binaries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			var out bytes.Buffer
			cmd := exec.Command("go", "build", "-o", filepath.Join(dir, b.name), b.pkg)
			cmd.Dir = filepath.Join(root, b.dir)
			cmd.Stdout, cmd.Stderr = &out, &out
			if err := cmd.Run(); err != nil {
				errs[i] = fmt.Errorf("go build %s/%s: %w\n%s", b.dir, b.pkg, err, out.String())
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return "", err
		}
	}
	return dir, nil
}

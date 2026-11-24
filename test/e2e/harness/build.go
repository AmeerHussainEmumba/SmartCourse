package harness

import (
	"bytes"
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

var binaries = []binary{
	{"worker", "services/worker", "./cmd/worker"},
	{"identity-api", "services/identity", "./cmd/identity-api"},
	{"course-api", "services/course", "./cmd/course-api"},
	{"analytics-api", "services/analytics", "./cmd/analytics-api"},
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
// cheap; a cold build of the four services takes a minute or two.
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
	// build cache says nothing changed.
	dir := filepath.Join(os.TempDir(), "smartcourse-e2e-bin")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	root := RepoRoot()
	var wg sync.WaitGroup
	errs := make([]error, len(binaries))
	for i, b := range binaries {
		wg.Add(1)
		go func() {
			defer wg.Done()
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

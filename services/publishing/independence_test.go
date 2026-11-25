// Guard: services/publishing imports no other service (docs/plans/microservices-split.md
// §1: services share only libs/*). Checked two ways: the go.mod require list
// names no other services/* module, and no package in the build graph (tests
// included) lives under services/ except this service's own.
package publishing_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

const (
	modulePrefix = "github.com/AmeerHussainEmumba/SmartCourse/"
	self         = modulePrefix + "services/publishing"
)

// goOutput runs go with GOWORK=off: the module graph is then exactly this
// module's go.mod (what CI and the Dockerfile see), not the whole workspace.
func goOutput(t *testing.T, args ...string) string {
	t.Helper()
	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go %s: %v", strings.Join(args, " "), err)
	}
	return string(out)
}

func isOtherService(path string) bool {
	return strings.HasPrefix(path, modulePrefix+"services/") && !strings.HasPrefix(path, self)
}

func TestNoOtherServiceInGoMod(t *testing.T) {
	for _, line := range strings.Split(goOutput(t, "list", "-m", "all"), "\n") {
		if path := strings.Fields(line + " x")[0]; isOtherService(path) {
			t.Errorf("module graph contains another service: %s", path)
		}
	}
}

func TestNoOtherServicePackageInBuildGraph(t *testing.T) {
	deps := goOutput(t, "list", "-deps", "-test", "-f", "{{.ImportPath}}", "./...")
	if !strings.Contains(deps, self+"/publish") {
		t.Fatalf("guard is not looking at this module's packages:\n%s", deps)
	}
	for _, pkg := range strings.Fields(deps) {
		if isOtherService(pkg) {
			t.Errorf("services/publishing imports another service's package: %s", pkg)
		}
	}
}

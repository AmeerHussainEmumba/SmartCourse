package runtime_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/runtime"
)

// TestMainHelper is not a test: the tests below re-run this binary with
// RUNTIME_HELPER set, so Main (which exits the process) can be observed from
// outside.
func TestMainHelper(t *testing.T) {
	mode := os.Getenv("RUNTIME_HELPER")
	if mode == "" {
		t.Skip("helper process only")
	}
	addr := os.Getenv("RUNTIME_ADDR")
	opts := []runtime.Option{runtime.WithConfig(testConfig(addr, true)), runtime.WithoutDependencies(), runtime.WithLogger(quiet),
		runtime.WithShutdownTimeout(2 * time.Second)}
	runtime.Main("helper", func(p *runtime.Process) error {
		switch mode {
		case "wire-error":
			return errors.New("bad wiring")
		case "critical":
			p.Go("core", func(context.Context) error { return errors.New("gone") }, runtime.Critical())
		case "serve":
			p.Go("work", func(ctx context.Context) error {
				fmt.Println("READY")
				<-ctx.Done()
				fmt.Println("DRAINED")
				return nil
			})
		}
		return nil
	}, opts...)
}

func helper(t *testing.T, mode string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMainHelper$")
	cmd.Env = append(os.Environ(), "RUNTIME_HELPER="+mode, "RUNTIME_ADDR="+freeAddr(t))
	return cmd
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return -1
}

func TestMain_ExitsNonZeroOnStartupAndCriticalFailures(t *testing.T) {
	for _, mode := range []string{"wire-error", "critical"} {
		out, err := helper(t, mode).CombinedOutput()
		if exitCode(err) != 1 {
			t.Errorf("%s: exit code %d (%v), want 1\n%s", mode, exitCode(err), err, out)
		}
	}
}

// SIGTERM is a graceful shutdown: the components are cancelled, drain, and the
// process exits 0.
func TestMain_SIGTERMShutsDownGracefully(t *testing.T) {
	cmd := helper(t, "serve")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(stdout)
	var lines []string
	readUntil := func(want string) {
		for sc.Scan() {
			lines = append(lines, sc.Text())
			if strings.TrimSpace(sc.Text()) == want {
				return
			}
		}
		t.Fatalf("never saw %q in %v", want, lines)
	}
	readUntil("READY")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	readUntil("DRAINED")
	select {
	case err := <-done:
		if exitCode(err) != 0 {
			t.Fatalf("exit code %d (%v), want 0", exitCode(err), err)
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the process did not exit after SIGTERM")
	}
}

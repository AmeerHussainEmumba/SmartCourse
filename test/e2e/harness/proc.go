package harness

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// logBuf keeps the tail of a process's output (the head is rarely what a
// failure needs) and is safe to read while the process still writes.
type logBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

const logCap = 512 << 10

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf.Write(p)
	if over := l.buf.Len() - logCap; over > 0 {
		l.buf.Next(over)
	}
	return len(p), nil
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// Proc is one service binary running as a subprocess. It can be killed and
// started again (the same environment, so the same ports), which is what the
// crash scenarios do.
type Proc struct {
	Name string
	bin  string
	env  []string

	logs *logBuf

	mu   sync.Mutex
	cmd  *exec.Cmd
	done chan struct{} // closed when the current process has exited
}

func newProc(name, bin string, env []string) *Proc {
	return &Proc{Name: name, bin: bin, env: env, logs: &logBuf{}}
}

// Start launches the process. Its stdout and stderr go to the log buffer that
// Logs returns and that Stack dumps into the test output when the test fails.
func (p *Proc) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd != nil && p.alive() {
		return fmt.Errorf("%s is already running", p.Name)
	}
	cmd := exec.Command(p.bin)
	// A clean environment: only what the service reads, so nothing from the
	// developer's shell (POSTGRES_DSN, EVENT_TRANSPORT) leaks into a scenario.
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}, p.env...)
	cmd.Stdout, cmd.Stderr = p.logs, p.logs
	// Own process group, so a kill reaches everything the binary started.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	fmt.Fprintf(p.logs, "--- harness: starting %s at %s ---\n", p.Name, time.Now().Format(time.RFC3339))
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", p.Name, err)
	}
	done := make(chan struct{})
	p.cmd, p.done = cmd, done
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	return nil
}

// alive reports whether the current process has not exited; callers hold mu.
func (p *Proc) alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

// Running reports whether the process is alive.
func (p *Proc) Running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cmd != nil && p.alive()
}

// Kill sends SIGKILL (kill -9): no shutdown hooks, no drain, no commit of
// anything in flight. It returns once the process is gone.
func (p *Proc) Kill() {
	p.signal(syscall.SIGKILL, 10*time.Second)
}

// Stop asks for a graceful shutdown (SIGTERM) and kills the process if it has
// not exited after grace.
func (p *Proc) Stop(grace time.Duration) {
	if !p.signal(syscall.SIGTERM, grace) {
		p.Kill()
	}
}

// signal sends sig to the process group and waits for the exit; false when the
// process was still alive at the deadline.
func (p *Proc) signal(sig syscall.Signal, wait time.Duration) bool {
	p.mu.Lock()
	cmd, done := p.cmd, p.done
	p.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return true
	}
	select {
	case <-done:
		return true
	default:
	}
	_ = syscall.Kill(-cmd.Process.Pid, sig)
	select {
	case <-done:
		return true
	case <-time.After(wait):
		return false
	}
}

// Logs is the captured output so far.
func (p *Proc) Logs() string { return p.logs.String() }

// LogCount counts occurrences of substr in the output so far.
func (p *Proc) LogCount(substr string) int { return strings.Count(p.logs.String(), substr) }

// LogContains reports whether substr has been logged.
func (p *Proc) LogContains(substr string) bool { return strings.Contains(p.logs.String(), substr) }

// dumpLogs writes the tail of the output to the test log.
func (p *Proc) dumpLogs(t testing.TB) {
	out := p.logs.String()
	const tail = 60 << 10
	if len(out) > tail {
		out = "...(truncated)...\n" + out[len(out)-tail:]
	}
	t.Logf("===== %s output =====\n%s", p.Name, out)
}

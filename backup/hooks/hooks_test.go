package hooks

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func commands() (echo, fail, hang string) {
	if runtime.GOOS == "windows" {
		return "echo hello", "exit /b 3", "ping -n 60 127.0.0.1 > nul"
	}

	return "echo hello", "exit 3", "sleep 60"
}

func TestRunPreCapturesOutput(t *testing.T) {
	echo, _, _ := commands()

	var stdout, logged bytes.Buffer
	r := &Runner{Pre: echo, Stdout: &stdout, Logger: slog.New(slog.NewTextHandler(&logged, nil))}

	out, err := r.RunPre(context.Background())
	require.NoError(t, err)
	require.Contains(t, out, "hello")
	require.Contains(t, stdout.String(), "hello", "output is echoed as well as captured")
	require.Contains(t, logged.String(), "hook=pre")
}

func TestRunPreReportsFailure(t *testing.T) {
	_, fail, _ := commands()
	r := &Runner{Pre: fail}

	_, err := r.RunPre(context.Background())
	require.Error(t, err)

	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 3, exit.ExitCode())
}

func TestTimeoutKillsHook(t *testing.T) {
	_, _, hang := commands()
	r := &Runner{Pre: hang, Timeout: 300 * time.Millisecond}

	start := time.Now()
	_, err := r.RunPre(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 20*time.Second)
}

func TestPostRunsAfterCancellation(t *testing.T) {
	echo, _, _ := commands()

	var stdout bytes.Buffer
	r := &Runner{Post: echo, Stdout: &stdout, Timeout: time.Minute}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, r.RunPost(ctx))
	require.Contains(t, stdout.String(), "hello")
}

func TestRedirect(t *testing.T) {
	require.Equal(t, "", Redirect(""))
	require.Equal(t, "", Redirect("saving off\nsaved\n"))
	require.Equal(t, "/snap/world", Redirect("saving off\n  GOBACK_ROOT=/snap/world  \n"))
	require.Equal(t, "/snap/b", Redirect("GOBACK_ROOT=/snap/a\r\nGOBACK_ROOT=/snap/b\r\n"))
}

func TestAPreHookThatLeavesAProcessHoldingItsOutputSucceeds(t *testing.T) {
	command := "sleep 20 & echo started"
	if runtime.GOOS == "windows" {
		command = "start /b ping -n 20 127.0.0.1 & echo started"
	}

	out, err := (&Runner{Pre: command, Stdout: io.Discard, Stderr: io.Discard}).RunPre(context.Background())
	require.NoError(t, err)
	require.Contains(t, out, "started")
}

func TestACaptureKeepsItsHeadAndTheLastRootLineAfterIt(t *testing.T) {
	var c capture

	line := strings.Repeat("x", 999) + "\n"
	for range 100 {
		_, _ = c.Write([]byte(line))
	}

	_, _ = c.Write([]byte("GOBACK_ROOT=/snap/a\n" + line + "GOBACK_ROOT=/sn"))
	_, _ = c.Write([]byte("ap/b\r\n" + line + "no newline"))

	out := c.String()
	require.LessOrEqual(t, len(out), captureLimit+len("GOBACK_ROOT=/snap/b\r\n"))
	require.Equal(t, "/snap/b", Redirect(out))

	var early capture
	_, _ = early.Write([]byte("GOBACK_ROOT=/snap/c\n"))
	for range 100 {
		_, _ = early.Write([]byte(line))
	}
	require.Equal(t, "/snap/c", Redirect(early.String()))
}

func TestEmptyHooksAreNoOps(t *testing.T) {
	r := &Runner{}

	out, err := r.RunPre(context.Background())
	require.NoError(t, err)
	require.Empty(t, out)
	require.NoError(t, r.RunPost(context.Background()))
}

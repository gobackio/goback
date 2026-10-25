// Package hooks runs the shell commands a backup is wrapped in.
package hooks

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/gobackio/goback/backup/childenv"
)

// Runner runs a backup's pre and post hooks through the system shell.
// Post runs under its own deadline even when the context is cancelled, so
// a quiesced server is released after an interrupted walk.
type Runner struct {
	Pre  string
	Post string
	// Timeout bounds each hook; 0 means none.
	Timeout time.Duration
	// Stdout and Stderr receive the hooks' output; nil means the process's.
	Stdout io.Writer
	Stderr io.Writer
	// Logger is where the runner reports; nil means slog.Default.
	Logger *slog.Logger
}

// RunPre runs the pre hook and returns what it printed to stdout: its
// first 64 KiB, and the last GOBACK_ROOT line after them.
func (r *Runner) RunPre(ctx context.Context) (string, error) {
	return r.run(ctx, "pre", r.Pre)
}

// RunPost runs the post hook, ignoring the context's cancellation.
func (r *Runner) RunPost(ctx context.Context) error {
	_, err := r.run(context.WithoutCancel(ctx), "post", r.Post)

	return err
}

func (r *Runner) run(ctx context.Context, name, command string) (string, error) {
	if command == "" {
		return "", nil
	}

	if r.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.Timeout)
		defer cancel()
	}

	r.logger().Info("running hook", "hook", name, "command", command)

	var out capture

	cmd := shell(ctx, command)
	cmd.Env = childenv.Environ()
	cmd.Stdout = io.MultiWriter(&out, r.writer(r.Stdout, os.Stdout))
	cmd.Stderr = r.writer(r.Stderr, os.Stderr)
	cmd.WaitDelay = 5 * time.Second

	err := cmd.Run()
	if ctx.Err() != nil {
		return out.String(), fmt.Errorf("%s hook: %w", name, ctx.Err())
	}

	if err != nil {
		return out.String(), fmt.Errorf("%s hook: %w", name, err)
	}

	return out.String(), nil
}

// Redirect returns the directory a pre hook asked the walk to read instead
// of the configured root, given as a GOBACK_ROOT=<dir> line on its stdout.
// The last such line wins; "" means no redirect.
func Redirect(output string) string {
	dir := ""
	for _, line := range strings.Split(output, "\n") {
		if value, ok := rootLine(line); ok {
			dir = value
		}
	}

	return dir
}

func rootLine(line string) (string, bool) {
	value, ok := strings.CutPrefix(strings.TrimSpace(line), "GOBACK_ROOT=")

	return strings.TrimSpace(value), ok
}

// captureLimit bounds what a capture keeps besides its last GOBACK_ROOT
// line.
const captureLimit = 64 << 10

// capture keeps the lines written to it while they fit in captureLimit, and
// the last GOBACK_ROOT line once they no longer do.
type capture struct {
	kept []byte
	full bool
	line []byte
	root []byte
}

func (c *capture) Write(p []byte) (int, error) {
	n := len(p)

	for len(p) > 0 {
		chunk := p
		end := bytes.IndexByte(p, '\n')
		if end >= 0 {
			chunk = p[:end+1]
		}

		if room := captureLimit - len(c.line); room > 0 {
			c.line = append(c.line, chunk[:min(room, len(chunk))]...)
		}

		p = p[len(chunk):]

		if end >= 0 {
			c.endLine()
		}
	}

	return n, nil
}

func (c *capture) endLine() {
	switch {
	case !c.full && len(c.kept)+len(c.line) <= captureLimit:
		c.kept = append(c.kept, c.line...)
	default:
		c.full = true

		if _, ok := rootLine(string(c.line)); ok {
			c.root = append(c.root[:0], c.line...)
		}
	}

	c.line = c.line[:0]
}

// String is what the capture kept, the last line included whether or not
// it ended.
func (c *capture) String() string {
	if len(c.line) > 0 {
		c.endLine()
	}

	return string(c.kept) + string(c.root)
}

func (r *Runner) writer(w io.Writer, fallback io.Writer) io.Writer {
	if w != nil {
		return w
	}

	return fallback
}

func (r *Runner) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}

	return slog.Default()
}

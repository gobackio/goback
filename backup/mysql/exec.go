package mysql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/gobackio/goback/backup/childenv"
)

// Exec is a Client that runs the installed client programs. They connect as
// MySQL's option files say: ~/.my.cnf, or DefaultsFile.
type Exec struct {
	Mysql       string
	Mysqldump   string
	Mysqlbinlog string
	// DefaultsFile, when set, goes to every program as
	// --defaults-extra-file.
	DefaultsFile string
	// Stderr receives the programs' errors; nil means os.Stderr.
	Stderr io.Writer
}

func (e *Exec) args(args ...string) []string {
	if e.DefaultsFile == "" {
		return args
	}

	return append([]string{"--defaults-extra-file=" + e.DefaultsFile}, args...)
}

func (e *Exec) stderr() io.Writer {
	if e.Stderr != nil {
		return e.Stderr
	}

	return os.Stderr
}

func (e *Exec) command(ctx context.Context, program string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, program, e.args(args...)...)
	cmd.Stderr = e.stderr()
	cmd.Env = childenv.Environ()

	return cmd
}

func (e *Exec) queryCommand(ctx context.Context, sql string) *exec.Cmd {
	return e.command(ctx, e.Mysql, "--batch", "--skip-column-names", "-e", sql)
}

// Query implements Client.
func (e *Exec) Query(ctx context.Context, sql string) ([][]string, error) {
	cmd := e.queryCommand(ctx, sql)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("mysql -e %q: %w: %s", sql, err, strings.TrimSpace(stderr.String()))
	}

	return parseBatch(out), nil
}

// parseBatch reads the rows mysql --batch prints: tab-separated, with tabs,
// newlines, backslashes and NULs escaped.
func parseBatch(out []byte) [][]string {
	var rows [][]string

	for _, line := range strings.Split(strings.TrimSuffix(string(out), "\n"), "\n") {
		if line == "" {
			continue
		}

		fields := strings.Split(line, "\t")
		for i, field := range fields {
			fields[i] = unescapeBatch(field)
		}

		rows = append(rows, fields)
	}

	return rows
}

var batchEscapes = strings.NewReplacer(`\\`, `\`, `\t`, "\t", `\n`, "\n", `\0`, "\x00")

func unescapeBatch(field string) string {
	if !strings.Contains(field, `\`) {
		return field
	}

	return batchEscapes.Replace(field)
}

// DumpVersion implements Client.
func (e *Exec) DumpVersion(ctx context.Context) (string, error) {
	out, err := e.command(ctx, e.Mysqldump, "--version").Output()
	if err != nil {
		return "", fmt.Errorf("mysqldump --version: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

// Dump implements Client.
func (e *Exec) Dump(ctx context.Context, args []string) (io.ReadCloser, error) {
	cmd := e.command(ctx, e.Mysqldump, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting mysqldump: %w", err)
	}

	return &running{cmd: cmd, out: stdout, name: "mysqldump"}, nil
}

// running is a program's output; Close waits for the program.
type running struct {
	cmd  *exec.Cmd
	out  io.Reader
	name string
	eof  bool
}

func (r *running) Read(p []byte) (int, error) {
	n, err := r.out.Read(p)
	if errors.Is(err, io.EOF) {
		r.eof = true
	}

	return n, err
}

func (r *running) Close() error {
	if !r.eof {
		_ = r.cmd.Process.Kill()
	}

	if err := r.cmd.Wait(); err != nil {
		return fmt.Errorf("%s: %w", r.name, err)
	}

	if !r.eof {
		return fmt.Errorf("%s: stopped before its output ended", r.name)
	}

	return nil
}

func (e *Exec) loadCommand(ctx context.Context) *exec.Cmd {
	return e.command(ctx, e.Mysql)
}

// Load implements Client.
func (e *Exec) Load(ctx context.Context, r io.Reader) error {
	cmd := e.loadCommand(ctx)
	cmd.Stdin = r
	cmd.Stdout = e.stderr()

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("mysql: %w", err)
	}

	return nil
}

// BinlogVersion implements Client.
func (e *Exec) BinlogVersion(ctx context.Context) (string, error) {
	out, err := e.command(ctx, e.Mysqlbinlog, "--version").Output()
	if err != nil {
		return "", fmt.Errorf("mysqlbinlog --version: %w", err)
	}

	return strings.TrimSpace(string(out)), nil
}

func (e *Exec) replayCommands(ctx context.Context, args []string) (*exec.Cmd, *exec.Cmd) {
	binlog := e.command(ctx, e.Mysqlbinlog, args...)
	binlog.Env = append(binlog.Env, "TZ=UTC")

	return binlog, e.loadCommand(ctx)
}

// Replay implements Client. Every file goes through one mysqlbinlog and one
// mysql, so a temporary table lives across files.
func (e *Exec) Replay(ctx context.Context, args []string) error {
	binlog, load := e.replayCommands(ctx, args)
	load.Stdout = e.stderr()

	pipe, err := binlog.StdoutPipe()
	if err != nil {
		return err
	}

	load.Stdin = pipe

	if err := binlog.Start(); err != nil {
		return fmt.Errorf("starting mysqlbinlog: %w", err)
	}

	loadErr := load.Run()
	if loadErr != nil {
		_ = binlog.Process.Kill()
	}

	binlogErr := binlog.Wait()

	switch {
	case loadErr != nil:
		return fmt.Errorf("mysql: %w", loadErr)
	case binlogErr != nil:
		return fmt.Errorf("mysqlbinlog: %w", binlogErr)
	}

	return nil
}

// LoadCommand is the shell command that loads the dump in file.
func (e *Exec) LoadCommand(file string) string {
	return shellJoin(append([]string{e.Mysql}, e.args()...)) + " < " + shellQuote(file)
}

// ReplayCommand is the shell command that replays binlogs as Replay does.
func (e *Exec) ReplayCommand(args []string) string {
	return "TZ=UTC " + shellJoin(append([]string{e.Mysqlbinlog}, e.args(args...)...)) + " | " + shellJoin(append([]string{e.Mysql}, e.args()...))
}

func shellJoin(words []string) string {
	quoted := make([]string, len(words))
	for i, word := range words {
		quoted[i] = shellQuote(word)
	}

	return strings.Join(quoted, " ")
}

func shellQuote(s string) string {
	if s != "" && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_./=:,+@") == "" {
		return s
	}

	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

package mysql

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestARoundTripThroughMySQL(t *testing.T) {
	// mysql:8.0 has no mysqlbinlog; Percona Server's of the same version
	// replays its binlogs
	for _, tc := range []struct{ version, replayImage string }{
		{"5.5", ""},
		{"8.0", "percona/percona-server:8.0"},
	} {
		t.Run(tc.version, func(t *testing.T) { roundTrip(t, tc.version, tc.replayImage) })
	}
}

// roundTrip dumps a live server, commits the binlogs written after it,
// restores to a moment between them, to the end and the dump alone into a
// new server, and checks the rows each time. It then purges and resets the
// source's binlogs.
func roundTrip(t *testing.T, version, replayImage string) {
	if testing.Short() {
		t.Skip("short mode")
	}

	ctx := context.Background()

	net, err := network.New(ctx)
	if err != nil {
		skipWithoutDocker(t, err)
	}

	t.Cleanup(func() { _ = net.Remove(ctx) })

	image := "mysql:" + version
	source := startMySQL(t, net, image, "source", "--log-bin=mysql-bin", "--server-id=7", "--binlog-format=ROW")
	source.binlogs = t.TempDir()

	source.sql("CREATE DATABASE app; CREATE TABLE app.rows (phase varchar(10), n int, PRIMARY KEY (phase, n)) ENGINE=InnoDB; " +
		"CREATE TABLE app.notes (n int PRIMARY KEY) ENGINE=MyISAM; INSERT INTO app.notes VALUES (1)")
	source.insert("before")

	f := newFixture(t)
	f.srv = nil

	dump, err := (&Dump{Walker: f.walker("db-dump"), Client: source, BinlogSet: "db-binlog"}).Run(ctx)
	require.NoError(t, err)
	require.Equal(t, "all-tables", dump.Commit.Metadata[MetaLock], "the MyISAM table locks all tables")

	source.insert("after")
	source.sql("INSERT INTO app.notes VALUES (2)")
	binlog := runBinlog(t, f, source)
	require.NotNil(t, binlog)

	at := tick()
	tick()
	source.insert("late")
	require.NotNil(t, runBinlog(t, f, source))

	target := startMySQL(t, net, image, "target")
	target.replayer = target
	if replayImage != "" {
		target.replayer = startReplayer(t, net, replayImage)
		target.replayHost = "target"
	}

	restore := func(binlogSet string, at time.Time) *RestoreResult {
		t.Helper()

		r := &Restore{
			Objects:   f.index,
			Latest:    func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) },
			DumpSet:   "db-dump",
			BinlogSet: binlogSet,
			At:        at,
			Client:    target,
			WorkDir:   t.TempDir(),
		}

		result, err := r.Run(ctx)
		require.NoError(t, err)

		return result
	}

	restore("db-binlog", at)
	require.Equal(t, "after|100\nbefore|100", target.rows())
	require.Equal(t, "2", target.sql("SELECT count(*) FROM app.notes"))

	target.sql("DROP DATABASE app")
	restore("db-binlog", time.Time{})
	require.Equal(t, "after|100\nbefore|100\nlate|100", target.rows())

	target.sql("DROP DATABASE app")
	result := restore("", time.Time{})
	require.Empty(t, result.Binlogs)
	require.Equal(t, "before|100", target.rows())

	source.insert("purged")
	source.sql("FLUSH BINARY LOGS")
	source.insert("purged2")
	source.sql("FLUSH BINARY LOGS")
	last := source.sql("SHOW MASTER STATUS")
	source.sql("PURGE BINARY LOGS TO '" + strings.Split(last, "|")[0] + "'")
	source.insert("purged3")

	_, err = runBinlogErr(f, source)
	require.ErrorIs(t, err, ErrGap)

	_, err = (&Dump{Walker: f.walker("db-dump"), Client: source, BinlogSet: "db-binlog"}).Run(ctx)
	require.NoError(t, err)
	source.insert("next")
	require.NotNil(t, runBinlog(t, f, source), "a dump after the hole clears it")

	source.sql("RESET MASTER")
	source.insert("reset")
	reset := runBinlog(t, f, source)
	require.Equal(t, "2", reset.Metadata[MetaChain], "RESET MASTER opens a new chain in the same set")
}

func runBinlogErr(f *fixture, source *mysqlContainer) (*proto.Commit, error) {
	b := &BinlogBackup{
		Walker:  f.walker("db-binlog"),
		Client:  source,
		Dir:     source.binlogs,
		DumpSet: "db-dump",
		Window:  14 * 24 * time.Hour,
		Now:     time.Now,
	}

	result, err := b.Run(f.ctx)
	if err != nil || result == nil {
		return nil, err
	}

	return result.Commit, nil
}

func runBinlog(t *testing.T, f *fixture, source *mysqlContainer) *proto.Commit {
	t.Helper()

	commit, err := runBinlogErr(f, source)
	require.NoError(t, err)

	return commit
}

// mysqlContainer is a Client that runs the client programs inside a
// container.
type mysqlContainer struct {
	t   *testing.T
	ctx context.Context
	testcontainers.Container

	// binlogs, when set, mirrors the container's binlogs, with their times,
	// after every query
	binlogs string
	// replayer runs mysqlbinlog against the server at replayHost
	replayer   *mysqlContainer
	replayHost string
}

func skipWithoutDocker(t *testing.T, err error) {
	t.Helper()

	if os.Getenv("CI") != "" {
		t.Fatalf("no Docker: %v", err)
	}

	t.Skipf("no Docker: %v", err)
}

func startMySQL(t *testing.T, net *testcontainers.DockerNetwork, image, alias string, args ...string) *mysqlContainer {
	t.Helper()

	return start(t, testcontainers.ContainerRequest{
		Image:          image,
		Env:            map[string]string{"MYSQL_ROOT_PASSWORD": "goback"},
		Cmd:            args,
		Networks:       []string{net.Name},
		NetworkAliases: map[string][]string{net.Name: {alias}},
		WaitingFor: wait.ForExec([]string{"mysql", "-uroot", "-pgoback", "-h127.0.0.1", "-e", "SELECT 1"}).
			WithStartupTimeout(3 * time.Minute).WithPollInterval(time.Second),
	})
}

func startReplayer(t *testing.T, net *testcontainers.DockerNetwork, image string) *mysqlContainer {
	t.Helper()

	return start(t, testcontainers.ContainerRequest{
		Image:      image,
		Entrypoint: []string{"sleep", "infinity"},
		Networks:   []string{net.Name},
	})
}

func start(t *testing.T, req testcontainers.ContainerRequest) *mysqlContainer {
	t.Helper()

	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if c != nil {
		t.Cleanup(func() {
			if t.Failed() {
				if logs, err := c.Logs(ctx); err == nil {
					out, _ := io.ReadAll(logs)
					t.Logf("%s logs:\n%s", req.Image, out)
				}
			}

			_ = c.Terminate(ctx)
		})
	}

	if err != nil {
		skipWithoutDocker(t, err)
	}

	return &mysqlContainer{t: t, ctx: ctx, Container: c}
}

func (c *mysqlContainer) exec(ctx context.Context, cmd ...string) ([]byte, error) {
	code, r, err := c.Exec(ctx, cmd)
	if err != nil {
		return nil, err
	}

	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, r); err != nil {
		return nil, err
	}

	if code != 0 {
		return stdout.Bytes(), fmt.Errorf("%v: exit %d: %s", cmd, code, stderr.String())
	}

	return stdout.Bytes(), nil
}

// sql runs statements as root and returns their output.
func (c *mysqlContainer) sql(statements string) string {
	c.t.Helper()

	out, err := c.exec(c.ctx, "mysql", "-uroot", "-pgoback", "--batch", "--skip-column-names", "-e", statements)
	require.NoError(c.t, err)

	return strings.TrimSpace(strings.ReplaceAll(string(out), "\t", "|"))
}

// insert writes 100 rows of phase, one transaction each.
func (c *mysqlContainer) insert(phase string) {
	c.t.Helper()

	var b strings.Builder
	for n := 1; n <= 100; n++ {
		fmt.Fprintf(&b, "INSERT INTO app.rows VALUES ('%s', %d);", phase, n)
	}

	c.sql(b.String())
}

func (c *mysqlContainer) rows() string {
	c.t.Helper()

	return c.sql("SELECT phase, count(*) FROM app.rows GROUP BY phase ORDER BY phase")
}

func (c *mysqlContainer) Query(ctx context.Context, sql string) ([][]string, error) {
	out, err := c.exec(ctx, "mysql", "-uroot", "-pgoback", "--batch", "--skip-column-names", "-e", sql)
	if err != nil {
		return nil, err
	}

	if c.binlogs != "" {
		if err := c.mirror(ctx); err != nil {
			return nil, err
		}
	}

	return parseBatch(out), nil
}

var binlogFile = regexp.MustCompile(`^[^.]+\.[0-9]{6,}$`)

// mirror makes the binlogs directory hold what the container's datadir
// holds of binlogs.
func (c *mysqlContainer) mirror(ctx context.Context) error {
	out, err := c.exec(ctx, "sh", "-c", `cd /var/lib/mysql && tar cf - $(ls | grep -E '^[^.]+\.[0-9]{6,}$')`)
	if err != nil {
		return err
	}

	seen := map[string]bool{}

	entries := tar.NewReader(bytes.NewReader(out))
	for {
		hdr, err := entries.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return err
		}

		content, err := io.ReadAll(entries)
		if err != nil {
			return err
		}

		path := filepath.Join(c.binlogs, hdr.Name)
		seen[hdr.Name] = true

		if old, err := os.ReadFile(path); err == nil && bytes.Equal(old, content) {
			if info, err := os.Stat(path); err == nil && info.ModTime().Equal(hdr.ModTime) {
				continue
			}
		}

		if err := os.WriteFile(path, content, 0o600); err != nil {
			return err
		}

		if err := os.Chtimes(path, hdr.ModTime, hdr.ModTime); err != nil {
			return err
		}
	}

	local, err := os.ReadDir(c.binlogs)
	if err != nil {
		return err
	}

	for _, entry := range local {
		if binlogFile.MatchString(entry.Name()) && !seen[entry.Name()] {
			if err := os.Remove(filepath.Join(c.binlogs, entry.Name())); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *mysqlContainer) DumpVersion(ctx context.Context) (string, error) {
	out, err := c.exec(ctx, "mysqldump", "--version")
	return string(out), err
}

func (c *mysqlContainer) Dump(ctx context.Context, args []string) (io.ReadCloser, error) {
	out, err := c.exec(ctx, append([]string{"mysqldump", "-uroot", "-pgoback"}, args...)...)
	return &fakeDump{Reader: bytes.NewReader(out), err: err}, nil
}

func (c *mysqlContainer) Load(ctx context.Context, r io.Reader) error {
	content, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	if err := c.CopyToContainer(ctx, content, "/tmp/dump.sql", 0o644); err != nil {
		return err
	}

	_, err = c.exec(ctx, "sh", "-c", "mysql -uroot -pgoback < /tmp/dump.sql")

	return err
}

func (c *mysqlContainer) BinlogVersion(ctx context.Context) (string, error) {
	out, err := c.replayer.exec(ctx, "mysqlbinlog", "--version")
	return string(out), err
}

func (c *mysqlContainer) Replay(ctx context.Context, args []string) error {
	words := []string{"TZ=UTC", "mysqlbinlog"}

	for _, arg := range args {
		if content, err := os.ReadFile(arg); err == nil {
			remote := "/tmp/" + filepath.Base(arg)
			if err := c.replayer.CopyToContainer(ctx, content, remote, 0o644); err != nil {
				return err
			}

			arg = remote
		}

		words = append(words, shellQuote(arg))
	}

	words = append(words, "|", "mysql", "-uroot", "-pgoback")
	if c.replayHost != "" {
		words = append(words, "-h", c.replayHost)
	}

	_, err := c.replayer.exec(ctx, "bash", "-o", "pipefail", "-c", strings.Join(words, " "))

	return err
}

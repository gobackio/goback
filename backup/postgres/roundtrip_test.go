package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestABaseBackupRoundTripThroughPostgres(t *testing.T) {
	for _, version := range []string{"9.6", "17"} {
		t.Run(version, func(t *testing.T) { roundTrip(t, version) })
	}
}

// roundTrip takes a base backup of a live cluster with pg_basebackup,
// restores it alone into a new cluster and checks it holds the rows written
// before it.
func roundTrip(t *testing.T, version string) {
	if testing.Short() {
		t.Skip("short mode")
	}

	image := "postgres:" + version + "-alpine"

	primary := startPostgres(t, testcontainers.ContainerRequest{
		Image:      image,
		Env:        map[string]string{"POSTGRES_PASSWORD": "goback"},
		Cmd:        []string{"postgres", "-c", "fsync=off", "-c", "wal_level=replica", "-c", "max_wal_senders=4"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(3 * time.Minute),
	})

	primary.run("sh", "-c", `echo "local replication all trust" >> "$PGDATA/pg_hba.conf"`)
	primary.psql("SELECT pg_reload_conf()")
	primary.psql("CREATE TABLE rows (phase text, n int); INSERT INTO rows SELECT 'before', generate_series(1, 1000)")

	f := newWALFixture(t)

	content := []byte(primary.run("pg_basebackup", "-D", "-", "-Ft", "-X", "fetch", "--checkpoint=fast"))
	base, err := f.runBase(content, nil)
	require.NoError(t, err)
	require.Equal(t, base.Commit.Metadata[MetaStopLSN], FormatLSN(stopInWAL(t, content, base.Commit.Metadata[MetaStartLSN])), "the stop location the WAL holds")

	primary.psql("INSERT INTO rows SELECT 'after', generate_series(1, 1000)")

	r := &Restore{
		Objects: f.index,
		Latest:  func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) },
		BaseSet: "db-base",
	}

	data := filepath.Join(t.TempDir(), "data")
	restored, err := r.Run(f.ctx, data)
	require.NoError(t, err)
	require.Nil(t, restored.WAL)

	bundle := tarDirs(t, map[string]string{"var/lib/postgresql/data": data})

	recovered := startPostgres(t, testcontainers.ContainerRequest{
		Image: image,
		Entrypoint: []string{"sh", "-c", "set -e; tar -xf /tmp/restore.tar -C /; chown -R postgres:postgres /var/lib/postgresql/data; " +
			"chmod 700 /var/lib/postgresql/data; exec docker-entrypoint.sh postgres"},
		Files: []testcontainers.ContainerFile{{Reader: bytes.NewReader(bundle), ContainerFilePath: "/tmp/restore.tar", FileMode: 0o644}},
	})

	recovered.waitFor(`[ "$(psql -tAc 'SELECT pg_is_in_recovery()')" = f ]`)
	require.Equal(t, "before|1000", recovered.psql("SELECT phase, count(*) FROM rows GROUP BY phase"))
}

type pgContainer struct {
	t   *testing.T
	ctx context.Context
	testcontainers.Container
}

func startPostgres(t *testing.T, req testcontainers.ContainerRequest) *pgContainer {
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
		if os.Getenv("CI") != "" {
			t.Fatalf("no Docker: %v", err)
		}

		t.Skipf("no Docker: %v", err)
	}

	return &pgContainer{t: t, ctx: ctx, Container: c}
}

// run runs cmd in the container as postgres and returns its output.
func (c *pgContainer) run(cmd ...string) string {
	c.t.Helper()

	code, out, err := c.exec(cmd...)
	require.NoError(c.t, err)
	require.Zero(c.t, code, "%v: %s", cmd, out)

	return out
}

func (c *pgContainer) exec(cmd ...string) (int, string, error) {
	code, r, err := c.Exec(c.ctx, cmd, tcexec.WithUser("postgres"))
	if err != nil {
		return 0, "", err
	}

	var stdout, stderr bytes.Buffer
	if _, err := stdcopy.StdCopy(&stdout, &stderr, r); err != nil {
		return 0, "", err
	}

	if code != 0 {
		return code, stderr.String(), nil
	}

	return code, stdout.String(), nil
}

func (c *pgContainer) psql(sql string) string {
	c.t.Helper()

	return strings.TrimSpace(c.run("psql", "-v", "ON_ERROR_STOP=1", "-tA", "-c", sql))
}

// waitFor runs the shell condition until it holds.
func (c *pgContainer) waitFor(condition string) {
	c.t.Helper()

	for deadline := time.Now().Add(2 * time.Minute); ; time.Sleep(250 * time.Millisecond) {
		code, _, err := c.exec("sh", "-c", condition)
		if err == nil && code == 0 {
			return
		}

		state, err := c.State(c.ctx)
		require.NoError(c.t, err)
		require.True(c.t, state.Running, "the container exited waiting for %s", condition)
		require.True(c.t, time.Now().Before(deadline), "waiting for %s", condition)
	}
}

// stopInWAL reads where the base backup that started at start stopped out
// of the WAL in its tar alone.
func stopInWAL(t *testing.T, content []byte, start string) uint64 {
	t.Helper()

	lsn, err := ParseLSN(start)
	require.NoError(t, err)

	ends := &walScan{}
	entries := tar.NewReader(&endOfArchive{r: bytes.NewReader(content)})
	for {
		hdr, err := entries.Next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)

		if dir, name := path.Split(path.Clean(hdr.Name)); (dir == "pg_wal/" || dir == "pg_xlog/") && isSegment(name) {
			_, err := io.Copy(ends.segment(name), entries)
			require.NoError(t, err)
		}
	}

	stop, ok := ends.stops[lsn]
	require.True(t, ok, "the WAL holds the backup's end")

	return stop
}

func walFiles(t *testing.T, f *walFixture, ref *proto.Ref) []string {
	t.Helper()

	obj, err := f.index.Get(f.ctx, ref)
	require.NoError(t, err)

	tree, err := backup.OpenTree(f.ctx, f.index, obj.GetCommit().GetTree(), nil, nil)
	require.NoError(t, err)

	var names []string
	for _, node := range tree.Nodes {
		names = append(names, string(node.Stat.Name))
	}

	return names
}

// tarDirs packs each local directory under the path it maps from.
func tarDirs(t *testing.T, dirs map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for under, dir := range dirs {
		err := filepath.WalkDir(dir, func(file string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			rel, err := filepath.Rel(dir, file)
			if err != nil {
				return err
			}

			name := path.Join(under, filepath.ToSlash(rel))

			if d.IsDir() {
				return tw.WriteHeader(&tar.Header{Name: name + "/", Typeflag: tar.TypeDir, Mode: 0o700})
			}

			content, err := os.ReadFile(file)
			if err != nil {
				return err
			}

			if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(content))}); err != nil {
				return err
			}

			_, err = tw.Write(content)

			return err
		})
		require.NoError(t, err)
	}

	require.NoError(t, tw.Close())

	return buf.Bytes()
}

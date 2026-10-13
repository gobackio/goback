package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// restoreCopied restores the sets of f to at, or to their end when at is
// zero, with the WAL copied in, onto a tree written front to back, starts
// a cluster of version on it and returns what rows holds there.
func restoreCopied(t *testing.T, f *walFixture, version string, at time.Time) (string, *RestoreResult) {
	t.Helper()

	r := &Restore{
		Objects: f.index,
		Latest:  func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) },
		BaseSet: "db-base",
		WALSet:  "db-wal",
		At:      at,
		FS:      frontToBack{backup.LocalFS{}},
	}

	data := filepath.Join(t.TempDir(), "data")
	result, err := r.Run(f.ctx, data)
	require.NoError(t, err)
	require.NotEmpty(t, result.Copied)

	recovered := startRecovered(t, version, data, "")
	defer func() { _ = recovered.Terminate(context.Background()) }()

	return recovered.psql("SELECT phase, count(*) FROM rows GROUP BY phase ORDER BY min(ctid)"), result
}

func TestARestoreWithTheWALCopiedInRecoversWithoutGoback(t *testing.T) {
	for _, version := range []string{"9.6", "12"} {
		t.Run(version, func(t *testing.T) { copiedFromAnArchive(t, version) })
	}

	t.Run("17", copiedFromAReceiver)
}

// copiedFromAnArchive restores a cluster whose WAL archive_command
// delivered to a moment between two inserts.
func copiedFromAnArchive(t *testing.T, version string) {
	if testing.Short() {
		t.Skip("short mode")
	}

	pg := startPostgres(t, testcontainers.ContainerRequest{
		Image: "postgres:" + version + "-alpine",
		Env:   map[string]string{"POSTGRES_PASSWORD": "goback"},
		Cmd: []string{"postgres", "-c", "fsync=off", "-c", "wal_level=replica", "-c", "max_wal_senders=4",
			"-c", "archive_mode=on", "-c", "archive_command=mkdir -p /tmp/archive && cp %p /tmp/archive/%f"},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(3 * time.Minute),
	})

	pg.run("sh", "-c", `echo "local replication all trust" >> "$PGDATA/pg_hba.conf"`)
	pg.psql("SELECT pg_reload_conf()")
	pg.psql("CREATE TABLE rows (phase text, n int); INSERT INTO rows SELECT 'before', generate_series(1, 1000)")

	f := newWALFixture(t)

	_, err := f.runBase([]byte(pg.run("pg_basebackup", "-D", "-", "-Ft", "-X", "fetch", "--checkpoint=fast")), nil)
	require.NoError(t, err)

	pg.psql("INSERT INTO rows SELECT 'after', generate_series(1, 1000)")

	time.Sleep(2 * time.Second)
	at := pg.now()
	time.Sleep(2 * time.Second)

	pg.psql("INSERT INTO rows SELECT 'later', generate_series(1, 1000)")

	switchWAL := "SELECT pg_walfile_name(pg_switch_wal())"
	if version == "9.6" {
		switchWAL = "SELECT pg_xlogfile_name(pg_switch_xlog())"
	}

	last := pg.psql(switchWAL)
	pg.waitFor(`[ "$(psql -tAc 'SELECT last_archived_wal FROM pg_stat_archiver')" = ` + last + ` ]`)

	archive := t.TempDir()
	untar(t, []byte(pg.run("tar", "-C", "/tmp/archive", "-cf", "-", ".")), archive)

	entries, err := os.ReadDir(archive)
	require.NoError(t, err)

	var segments []string
	for _, entry := range entries {
		if isSegment(entry.Name()) {
			segments = append(segments, entry.Name())
		}
	}

	slices.Sort(segments)

	w := &backup.Walker{Index: f.index, Objects: f.index, Set: "db-wal", AgentID: "a", Root: archive, Workers: 1, Metadata: map[string]string{
		MetaFirstWALFile: segments[0], MetaLastWALFile: segments[len(segments)-1], MetaCapture: "archive",
	}}
	_, err = w.Run(f.ctx)
	require.NoError(t, err)

	rows, result := restoreCopied(t, f, version, at)
	require.Equal(t, "before|1000\nafter|1000", rows)
	require.Contains(t, result.Copied, last, "to the end of the WAL set before Postgres 13")
}

// copiedFromAReceiver restores a cluster whose WAL a receiver streamed to a
// moment several segments before its end, and to the end, which only its
// partial segment holds.
func copiedFromAReceiver(t *testing.T) {
	x := startReplica(t, "17", "max_slot_wal_keep_size=1GB")

	x.insert("before")
	x.base()

	stop := x.start(x.receiver())

	x.insert("after")
	x.pg.psql("SELECT pg_switch_wal()")
	x.caughtUp()

	time.Sleep(3 * time.Second)
	at := x.now()
	time.Sleep(3 * time.Second)

	for _, phase := range []string{"later", "latest"} {
		x.insert(phase)
		x.pg.psql("SELECT pg_switch_wal()")
	}

	x.insert("last")
	last := x.caughtUp()
	require.NoError(t, stop())

	end, err := ParseLSN(last.GetMetadata()[MetaEndLSN])
	require.NoError(t, err)
	require.NotZero(t, end%(16<<20), "the newest WAL is only in the partial segment")

	var partial string
	for _, name := range walFiles(t, x.f, x.f.latestRef(t)) {
		if segment, ok := isPartial(name); ok {
			partial = segment
		}
	}

	rows, result := restoreCopied(t, x.f, "17", at)
	require.Equal(t, "before|1000\nafter|1000", rows)
	require.NotContains(t, result.Copied, partial, "up to one segment after the moment")

	rows, result = restoreCopied(t, x.f, "17", time.Time{})
	require.Equal(t, "before|1000\nafter|1000\nlater|1000\nlatest|1000\nlast|1000", rows)
	require.Contains(t, result.Copied, partial)
}

// untar extracts the regular files of a tar into dir.
func untar(t *testing.T, content []byte, dir string) {
	t.Helper()

	tr := tar.NewReader(bytes.NewReader(content))

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return
		}

		require.NoError(t, err)

		name := strings.TrimPrefix(hdr.Name, "./")
		if hdr.Typeflag != tar.TypeReg || strings.Contains(name, "/") {
			continue
		}

		data, err := io.ReadAll(tr)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o600))
	}
}

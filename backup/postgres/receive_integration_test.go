package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// replica is a live cluster with a replication role, and a WAL fixture the
// receiver commits to.
type replica struct {
	t       *testing.T
	version string
	f       *walFixture
	pg      *pgContainer
	conn    string
	spool   Spool
}

// startReplica starts a cluster of version with settings, a role goback
// that may replicate from anywhere with password goback, and a table rows.
func startReplica(t *testing.T, version string, settings ...string) *replica {
	t.Helper()

	if testing.Short() {
		t.Skip("short mode")
	}

	cmd := []string{"postgres", "-c", "fsync=off"}
	for _, s := range settings {
		cmd = append(cmd, "-c", s)
	}

	pg := startPostgres(t, testcontainers.ContainerRequest{
		Image:        "postgres:" + version + "-alpine",
		Env:          map[string]string{"POSTGRES_PASSWORD": "goback"},
		Cmd:          cmd,
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(3 * time.Minute),
	})

	pg.run("sh", "-c", `echo "host replication goback all scram-sha-256" >> "$PGDATA/pg_hba.conf"`)
	pg.psql("CREATE ROLE goback LOGIN REPLICATION PASSWORD 'goback'")
	pg.psql("SELECT pg_reload_conf()")
	pg.psql("CREATE TABLE rows (phase text, n int)")

	host, err := pg.Host(pg.ctx)
	require.NoError(t, err)

	port, err := pg.MappedPort(pg.ctx, "5432/tcp")
	require.NoError(t, err)

	return &replica{
		t:       t,
		version: version,
		f:       newWALFixture(t),
		pg:      pg,
		conn:    fmt.Sprintf("host=%s port=%s user=goback password=goback sslmode=disable", host, port.Port()),
		spool:   Spool{Dir: t.TempDir()},
	}
}

func (x *replica) receiver() *Receiver {
	return &Receiver{
		ConnString: x.conn,
		Spool:      x.spool,
		Walker: func() *backup.Walker {
			return &backup.Walker{Index: x.f.index, Objects: x.f.index, Set: "db-wal", AgentID: "a", Root: x.spool.Dir, Workers: 2}
		},
		BaseSet:     "db-base",
		Window:      14 * 24 * time.Hour,
		CommitEvery: 2 * time.Second,
		floor:       500 * time.Millisecond,
	}
}

// start runs r in the background; the returned func stops it and returns
// what Run did.
func (x *replica) start(r *Receiver) func() error {
	stop := make(chan struct{})
	done := make(chan error, 1)

	go func() { done <- r.Run(context.Background(), stop) }()

	return func() error {
		select {
		case err := <-done:
			return err
		default:
		}

		close(stop)

		return <-done
	}
}

// base takes a base backup with the cluster's pg_basebackup and commits it.
func (x *replica) base() *backup.WalkResult {
	x.t.Helper()

	result, err := x.f.runBase([]byte(x.pg.run("pg_basebackup", "-D", "-", "-Ft", "-X", "fetch", "--checkpoint=fast")), nil)
	require.NoError(x.t, err)

	return result
}

func (x *replica) insert(phase string) {
	x.pg.psql("INSERT INTO rows SELECT '" + phase + "', generate_series(1, 1000)")
}

// latest is the WAL set's newest commit; nil without one.
func (x *replica) latest() *proto.Commit {
	commit, err := latestCommit(x.f.ctx, x.f.index, x.f.index, "db-wal")
	require.NoError(x.t, err)

	return commit
}

// caughtUp waits until a commit holds the WAL the server has written.
func (x *replica) caughtUp() *proto.Commit {
	x.t.Helper()

	current, err := ParseLSN(x.pg.psql("SELECT pg_current_wal_lsn()"))
	require.NoError(x.t, err)

	for deadline := time.Now().Add(time.Minute); ; time.Sleep(200 * time.Millisecond) {
		if commit := x.latest(); commit != nil {
			if end, err := ParseLSN(commit.GetMetadata()[MetaEndLSN]); err == nil && end >= current {
				return commit
			}
		}

		require.True(x.t, time.Now().Before(deadline), "waiting for a commit to reach %s", FormatLSN(current))
	}
}

// slotKeepsWhatNoCommitHolds checks that the slot never moved past what
// the newest commit holds.
func (x *replica) slotKeepsWhatNoCommitHolds() {
	x.t.Helper()

	restart := x.pg.psql("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'goback_db_wal'")
	if restart == "" {
		return
	}

	lsn, err := ParseLSN(restart)
	require.NoError(x.t, err)

	commit := x.latest()
	if commit == nil {
		return
	}

	end, err := ParseLSN(commit.GetMetadata()[MetaEndLSN])
	require.NoError(x.t, err)
	require.LessOrEqual(x.t, lsn, end, "the slot moved past what a commit holds")
}

// now is the cluster's clock.
func (x *replica) now() time.Time {
	x.t.Helper()

	at, err := time.Parse("2006-01-02 15:04:05.999999-07", x.pg.psql("SELECT clock_timestamp()"))
	require.NoError(x.t, err)

	return at
}

// restore recovers the sets to at, or to their end when at is zero, in a
// new cluster, and returns what rows holds there.
func (x *replica) restore(at time.Time) string {
	x.t.Helper()

	r := &Restore{
		Objects:        x.f.index,
		Latest:         func(ctx context.Context, set string) (*proto.Ref, error) { return x.f.index.LatestCommit(ctx, set) },
		BaseSet:        "db-base",
		WALSet:         "db-wal",
		At:             at,
		RestoreCommand: func(*proto.Ref) string { return "cp /wal/%f %p" },
	}

	data := filepath.Join(x.t.TempDir(), "data")
	restored, err := r.Run(x.f.ctx, data)
	require.NoError(x.t, err)
	require.NotNil(x.t, restored.WAL)

	names := walFiles(x.t, x.f, restored.WAL)
	wal := x.t.TempDir()

	for _, name := range names {
		if segment, ok := isPartial(name); ok {
			if !servesPartial(names, segment) {
				continue
			}

			name = segment
		}

		require.NoError(x.t, FetchWAL(x.f.ctx, x.f.index, nil, restored.WAL, name, filepath.Join(wal, name)))
	}

	bundle := tarDirs(x.t, map[string]string{"var/lib/postgresql/data": data, "wal": wal})

	recovered := startPostgres(x.t, testcontainers.ContainerRequest{
		Image: "postgres:" + x.version + "-alpine",
		Entrypoint: []string{"sh", "-c", "set -e; tar -xf /tmp/restore.tar -C /; chown -R postgres:postgres /var/lib/postgresql/data /wal; " +
			"chmod 700 /var/lib/postgresql/data; exec docker-entrypoint.sh postgres"},
		Files: []testcontainers.ContainerFile{{Reader: bytes.NewReader(bundle), ContainerFilePath: "/tmp/restore.tar", FileMode: 0o644}},
	})
	defer func() { _ = recovered.Terminate(context.Background()) }()

	recovered.waitFor(`[ "$(psql -tAc 'SELECT pg_is_in_recovery()')" = f ]`)

	return recovered.psql("SELECT phase, count(*) FROM rows GROUP BY phase ORDER BY min(ctid)")
}

func TestAPointInTimeRestoreFromReplication(t *testing.T) {
	for _, version := range []string{"14", "17"} {
		t.Run(version, func(t *testing.T) {
			x := startReplica(t, version, "max_slot_wal_keep_size=1GB")

			x.insert("before")
			x.base()

			stop := x.start(x.receiver())

			x.insert("after")
			x.caughtUp()

			time.Sleep(2 * time.Second)
			at := x.now()
			time.Sleep(2 * time.Second)

			x.insert("later")
			last := x.caughtUp()

			require.NoError(t, stop())
			x.slotKeepsWhatNoCommitHolds()

			require.Equal(t, CaptureReplication, last.GetMetadata()[MetaCapture])

			end, err := ParseLSN(last.GetMetadata()[MetaEndLSN])
			require.NoError(t, err)
			require.NotZero(t, end%(16<<20), "the newest WAL is only in the partial segment")

			require.Equal(t, "before|1000\nafter|1000", x.restore(at))
			require.Equal(t, "before|1000\nafter|1000\nlater|1000", x.restore(time.Time{}))
		})
	}
}

func TestAReceiverDownPastTheCapEndsInAGapThatABaseBackupClears(t *testing.T) {
	x := startReplica(t, "17", "max_slot_wal_keep_size=64MB", "max_wal_size=96MB", "min_wal_size=32MB")

	x.insert("before")
	x.base()

	stop := x.start(x.receiver())
	x.insert("after")
	x.caughtUp()
	require.NoError(t, stop())

	require.NoError(t, os.RemoveAll(x.spool.Dir))

	for range 8 {
		x.insert("lost")
		x.pg.psql("SELECT pg_switch_wal()")
	}

	x.pg.psql("CHECKPOINT")
	x.pg.waitFor(`[ "$(psql -tAc "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'goback_db_wal'")" = lost ]`)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err := x.receiver().Run(ctx, make(chan struct{}))
	require.ErrorIs(t, err, ErrGap)

	x.base()

	stop = x.start(x.receiver())
	x.insert("after the gap")
	x.caughtUp()
	require.NoError(t, stop())

	require.Equal(t, "before|1000\nafter|1000\nlost|8000\nafter the gap|1000", x.restore(time.Time{}))
}

var errCrash = errors.New("crash")

func TestACrashBetweenReceivingAndCommittingLosesNothing(t *testing.T) {
	x := startReplica(t, "17", "max_slot_wal_keep_size=1GB")

	x.insert("zero")
	x.base()

	for i, crash := range []struct {
		point string
		spool bool
	}{
		{"cut", true},
		{"cut", false},
		{"committed", true},
		{"committed", false},
	} {
		r := x.receiver()
		r.hook = func(point string) error {
			if point == crash.point {
				return errCrash
			}

			return nil
		}

		x.insert(fmt.Sprintf("%s %d", crash.point, i))

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		require.ErrorIs(t, r.Run(ctx, make(chan struct{})), errCrash)
		cancel()
		x.slotKeepsWhatNoCommitHolds()

		if !crash.spool {
			require.NoError(t, os.RemoveAll(x.spool.Dir))
		}

		stop := x.start(x.receiver())
		x.caughtUp()
		require.NoError(t, stop())
		x.slotKeepsWhatNoCommitHolds()
	}

	require.Equal(t, "zero|1000\ncut 0|1000\ncut 1|1000\ncommitted 2|1000\ncommitted 3|1000", x.restore(time.Time{}))
}

func TestTheReceiverRefusesToBeASynchronousStandby(t *testing.T) {
	x := startReplica(t, "17", "max_slot_wal_keep_size=1GB")

	x.pg.psql("ALTER SYSTEM SET synchronous_standby_names = '*'")
	x.pg.psql("SELECT pg_reload_conf()")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	err := x.receiver().Run(ctx, make(chan struct{}))
	require.ErrorContains(t, err, "pg_read_all_stats", "the role can't see its sync_state")

	x.pg.psql("SET synchronous_commit = local; GRANT pg_read_all_stats TO goback")

	err = x.receiver().Run(ctx, make(chan struct{}))
	require.ErrorIs(t, err, ErrSynchronousStandby)
}

func TestTheReceiverRefusesAnUncappedSlot(t *testing.T) {
	x := startReplica(t, "17")

	err := x.receiver().Run(context.Background(), make(chan struct{}))
	require.ErrorIs(t, err, ErrUncappedSlot)
	require.Empty(t, x.pg.psql("SELECT slot_name FROM pg_replication_slots"), "no slot left behind")

	require.Empty(t, strings.Join(Check(context.Background(), x.conn, "goback_db_wal", true), "\n"))
	require.Contains(t, strings.Join(Check(context.Background(), x.conn, "goback_db_wal", false), "\n"), "max_slot_wal_keep_size")
}

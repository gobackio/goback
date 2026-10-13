package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
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

	return startReplicaOn(t, version, "", settings...)
}

// startReplicaOn starts the cluster as startReplica does, on network as
// primary when network is set.
func startReplicaOn(t *testing.T, version, network string, settings ...string) *replica {
	t.Helper()

	if testing.Short() {
		t.Skip("short mode")
	}

	cmd := []string{"postgres", "-c", "fsync=off"}
	for _, s := range settings {
		cmd = append(cmd, "-c", s)
	}

	req := testcontainers.ContainerRequest{
		Image:        "postgres:" + version + "-alpine",
		Env:          map[string]string{"POSTGRES_PASSWORD": "goback"},
		Cmd:          cmd,
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(3 * time.Minute),
	}

	if network != "" {
		req.Networks = []string{network}
		req.NetworkAliases = map[string][]string{network: {"primary"}}
	}

	pg := startPostgres(t, req)

	pg.run("sh", "-c", `echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"`)
	pg.psql("CREATE ROLE goback LOGIN REPLICATION PASSWORD 'goback'")
	pg.psql("SELECT pg_reload_conf()")
	pg.psql("CREATE TABLE rows (phase text, n int)")

	return &replica{
		t:       t,
		version: version,
		f:       newWALFixture(t),
		pg:      pg,
		conn:    connTo(t, pg),
		spool:   Spool{Dir: t.TempDir()},
	}
}

// connTo is the connection string of role goback to the cluster in pg.
func connTo(t *testing.T, pg *pgContainer) string {
	t.Helper()

	host, err := pg.Host(pg.ctx)
	require.NoError(t, err)

	port, err := pg.MappedPort(pg.ctx, "5432/tcp")
	require.NoError(t, err)

	return fmt.Sprintf("host=%s port=%s user=goback password=goback sslmode=disable", host, port.Port())
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

	go func() {
		err := r.Run(context.Background(), stop)
		if err != nil {
			x.t.Logf("the receiver ended: %v", err)
		}

		done <- err
	}()

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

// base takes a base backup and commits it: over a replication connection
// from Postgres 15 on, with the cluster's pg_basebackup before.
func (x *replica) base() *backup.WalkResult {
	x.t.Helper()

	stream, err := StartBase(x.f.ctx, x.conn)
	if errors.Is(err, ErrNoBaseStream) {
		result, err := x.f.runBase([]byte(x.pg.run("pg_basebackup", "-D", "-", "-Ft", "-X", "fetch", "--checkpoint=fast")), nil)
		require.NoError(x.t, err)

		return result
	}

	require.NoError(x.t, err)
	defer stream.Close()

	b := &BaseBackup{Walker: &backup.Walker{Index: x.f.index, Objects: x.f.index, Set: "db-base", AgentID: "a", Workers: 1}}
	result, err := b.Run(x.f.ctx, stream.Tar, stream.Wait)
	require.NoError(x.t, err)
	require.NotEmpty(x.t, result.Commit.GetMetadata()[MetaStopLSN])

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

	return x.pg.now()
}

// now is the cluster's clock.
func (c *pgContainer) now() time.Time {
	c.t.Helper()

	at, err := time.Parse("2006-01-02 15:04:05.999999-07", c.psql("SELECT clock_timestamp()"))
	require.NoError(c.t, err)

	return at
}

// restore recovers the sets to at, or to their end when at is zero, in a
// new cluster, and returns what rows holds there.
func (x *replica) restore(at time.Time) string {
	x.t.Helper()

	recovered := x.recover(at)
	defer func() { _ = recovered.Terminate(context.Background()) }()

	return recovered.psql("SELECT phase, count(*) FROM rows GROUP BY phase ORDER BY min(ctid)")
}

// recover starts a cluster recovered from the sets to at, or to their end
// when at is zero, with settings, and waits until it is promoted.
func (x *replica) recover(at time.Time, settings ...string) *pgContainer {
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

		err := FetchWAL(x.f.ctx, x.f.index, nil, restored.WAL, name, filepath.Join(wal, name))
		if errors.Is(err, backup.ErrNotFound) {
			continue
		}

		require.NoError(x.t, err)
	}

	return startRecovered(x.t, x.version, data, wal, settings...)
}

// startRecovered starts a cluster of version on the data directory data,
// with the directory wal at /wal when it is set, and waits until it is
// promoted.
func startRecovered(t *testing.T, version, data, wal string, settings ...string) *pgContainer {
	t.Helper()

	dirs := map[string]string{"var/lib/postgresql/data": data}
	owned := "/var/lib/postgresql/data"

	if wal != "" {
		dirs["wal"] = wal
		owned += " /wal"
	}

	bundle := tarDirs(t, dirs)

	command := "postgres"
	for _, s := range settings {
		command += " -c " + s
	}

	recovered := startPostgres(t, testcontainers.ContainerRequest{
		Image:        "postgres:" + version + "-alpine",
		Env:          map[string]string{"PGDATA": "/var/lib/postgresql/data"},
		ExposedPorts: []string{"5432/tcp"},
		Entrypoint: []string{"sh", "-c", "set -e; tar -xf /tmp/restore.tar -C /; chown -R postgres:postgres " + owned + "; " +
			"chmod 700 /var/lib/postgresql/data; exec docker-entrypoint.sh " + command},
		Files: []testcontainers.ContainerFile{{Reader: bytes.NewReader(bundle), ContainerFilePath: "/tmp/restore.tar", FileMode: 0o644}},
	})

	recovered.waitFor(`[ "$(psql -tAc 'SELECT pg_is_in_recovery()')" = f ]`)

	return recovered
}

func TestAPointInTimeRestoreFromReplication(t *testing.T) {
	for _, version := range []string{"14", "17", "18"} {
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

func TestTheReceiverFollowsAPromotionToTimeline2(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	net, err := network.New(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = net.Remove(context.Background()) })

	x := startReplicaOn(t, "17", net.Name, "max_slot_wal_keep_size=1GB")

	standby := startPostgres(t, testcontainers.ContainerRequest{
		Image:        "postgres:17-alpine",
		Networks:     []string{net.Name},
		ExposedPorts: []string{"5432/tcp"},
		Entrypoint: []string{"sh", "-c", "set -e; mkdir -p $PGDATA; chown postgres:postgres $PGDATA; chmod 700 $PGDATA; " +
			"until gosu postgres pg_isready -h primary -U postgres; do sleep 1; done; " +
			"gosu postgres env PGPASSWORD=goback pg_basebackup -h primary -U postgres -D $PGDATA -R -X stream; " +
			"exec gosu postgres postgres -c fsync=off -c max_slot_wal_keep_size=1GB"},
		WaitingFor: wait.ForLog("database system is ready to accept read-only connections").WithStartupTimeout(3 * time.Minute),
	})

	x.insert("before")
	x.base()

	stop := x.start(x.receiver())
	x.insert("on the primary")
	x.caughtUp()
	require.NoError(t, stop())

	standby.waitFor(`[ "$(psql -tAc 'SELECT count(*) FROM rows')" = 2000 ]`)
	standby.psql("SELECT pg_promote()")
	standby.waitFor(`[ "$(psql -tAc 'SELECT pg_is_in_recovery()')" = f ]`)

	x.pg, x.conn = standby, connTo(t, standby)

	stop = x.start(x.receiver())
	x.insert("after the promotion")
	last := x.caughtUp()

	time.Sleep(2 * time.Second)
	at := x.now()
	time.Sleep(2 * time.Second)

	x.insert("later")
	x.caughtUp()
	require.NoError(t, stop())

	require.Equal(t, "2", last.GetMetadata()[MetaTimeline])

	names := walFiles(t, x.f, x.f.latestRef(t))
	require.Contains(t, names, "00000002.history")

	require.Equal(t, "before|1000\non the primary|1000\nafter the promotion|1000", x.restore(at))
	require.Equal(t, "before|1000\non the primary|1000\nafter the promotion|1000\nlater|1000", x.restore(time.Time{}))
}

func TestTheReceiverGoesOnWhereARestoreToAnEarlierMomentForked(t *testing.T) {
	x := startReplica(t, "17", "max_slot_wal_keep_size=1GB")

	x.insert("before")
	x.base()

	stop := x.start(x.receiver())
	x.insert("kept")
	x.caughtUp()

	time.Sleep(2 * time.Second)
	fork := x.now()
	time.Sleep(2 * time.Second)

	x.insert("abandoned")
	x.caughtUp()
	require.NoError(t, stop())

	restored := x.recover(fork, "fsync=off", "max_slot_wal_keep_size=1GB")
	require.NoError(t, x.pg.Terminate(context.Background()))
	x.pg, x.conn = restored, connTo(t, restored)

	stop = x.start(x.receiver())
	x.insert("after the fork")
	last := x.caughtUp()

	time.Sleep(2 * time.Second)
	at := x.now()
	time.Sleep(2 * time.Second)

	x.insert("later")
	x.caughtUp()
	require.NoError(t, stop())
	x.slotKeepsWhatNoCommitHolds()

	require.Equal(t, "2", last.GetMetadata()[MetaTimeline])
	require.Contains(t, walFiles(t, x.f, x.f.latestRef(t)), "00000002.history")

	require.Equal(t, "before|1000\nkept|1000\nafter the fork|1000", x.restore(at))
	require.Equal(t, "before|1000\nkept|1000\nafter the fork|1000\nlater|1000", x.restore(time.Time{}))
}

func (f *walFixture) latestRef(t *testing.T) *proto.Ref {
	t.Helper()

	ref, err := f.index.LatestCommit(f.ctx, "db-wal")
	require.NoError(t, err)

	return ref
}

// downIndex fails every write while down is set, as an unreachable store
// does.
type downIndex struct {
	*sql.Index
	down *atomic.Bool
}

func (d downIndex) Put(ctx context.Context, obj *proto.Object) error {
	if d.down.Load() {
		return errors.New("the store is unreachable")
	}

	return d.Index.Put(ctx, obj)
}

func TestTheSpoolBridgesASlotLostWhileTheStoreIsDown(t *testing.T) {
	x := startReplica(t, "17", "max_slot_wal_keep_size=64MB", "max_wal_size=1GB", "min_wal_size=32MB")

	x.insert("before")
	x.base()

	stop := x.start(x.receiver())
	x.insert("after")
	x.caughtUp()
	require.NoError(t, stop())

	var down atomic.Bool
	down.Store(true)

	r := x.receiver()
	r.Walker = func() *backup.Walker {
		return &backup.Walker{Index: downIndex{x.f.index, &down}, Objects: x.f.index, Set: "db-wal", AgentID: "a", Root: x.spool.Dir, Workers: 2}
	}

	stop = x.start(r)

	for range 8 {
		x.insert("while the store is down")
		x.pg.psql("SELECT pg_switch_wal()")
	}

	current := x.pg.psql("SELECT pg_current_wal_lsn()")
	x.pg.waitFor(`[ "$(psql -tAc "SELECT sent_lsn >= '` + current + `' FROM pg_stat_replication")" = t ]`)

	x.pg.psql("CHECKPOINT")
	x.pg.waitFor(`[ "$(psql -tAc "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'goback_db_wal'")" = lost ]`)
	require.Error(t, stop(), "the server ends the connection of a slot it invalidates")

	down.Store(false)

	stop = x.start(x.receiver())
	x.insert("once it is back")
	x.caughtUp()
	require.NoError(t, stop())

	require.Equal(t, "before|1000\nafter|1000\nwhile the store is down|8000\nonce it is back|1000", x.restore(time.Time{}))
}

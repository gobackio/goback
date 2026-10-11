package mysql

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
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// source is a live server whose binlogs a receiver commits.
type source struct {
	t       *testing.T
	version string
	f       *fixture
	net     *testcontainers.DockerNetwork
	db      *mysqlContainer
	options string
	spool   Spool
}

// startSource starts a server of version with log_bin and args, a user
// goback holding only the replication privileges, and a table app.rows.
func startSource(t *testing.T, version string, args ...string) *source {
	t.Helper()

	if testing.Short() {
		t.Skip("short mode")
	}

	ctx := context.Background()

	net, err := network.New(ctx)
	if err != nil {
		skipWithoutDocker(t, err)
	}

	t.Cleanup(func() { _ = net.Remove(ctx) })

	db := start(t, testcontainers.ContainerRequest{
		Image:          "mysql:" + version,
		Env:            map[string]string{"MYSQL_ROOT_PASSWORD": "goback"},
		Cmd:            append([]string{"--log-bin=mysql-bin", "--server-id=7", "--binlog-format=ROW"}, args...),
		ExposedPorts:   []string{"3306/tcp"},
		Networks:       []string{net.Name},
		NetworkAliases: map[string][]string{net.Name: {"source"}},
		WaitingFor: wait.ForExec([]string{"mysql", "-uroot", "-pgoback", "-h127.0.0.1", "-e", "SELECT 1"}).
			WithStartupTimeout(3 * time.Minute).WithPollInterval(time.Second),
	})

	db.sql("CREATE USER 'goback'@'%' IDENTIFIED BY 'goback'; GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'goback'@'%'")
	db.sql("CREATE DATABASE app; CREATE TABLE app.rows (phase varchar(10), n int, PRIMARY KEY (phase, n)) ENGINE=InnoDB")

	host, err := db.Host(ctx)
	require.NoError(t, err)

	port, err := db.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err)

	options := filepath.Join(t.TempDir(), "goback.cnf")
	require.NoError(t, os.WriteFile(options, []byte(fmt.Sprintf("[client]\nhost=%s\nport=%s\nuser=goback\npassword=\"goback\"\n", host, port.Port())), 0o600))

	f := newFixture(t)
	f.srv = nil

	return &source{t: t, version: version, f: f, net: net, db: db, options: options, spool: Spool{Dir: t.TempDir()}}
}

func (s *source) dump() *backup.WalkResult {
	s.t.Helper()

	result, err := (&Dump{Walker: s.f.walker("db-dump"), Client: s.db, BinlogSet: "db-binlog"}).Run(context.Background())
	require.NoError(s.t, err)

	return result
}

// liveReceiver is a receiver running in the background; done closes once
// it returned err.
type liveReceiver struct {
	stop chan struct{}
	done chan struct{}
	err  error
}

func (s *source) receiver() *Receiver {
	return &Receiver{
		DefaultsExtraFile: s.options,
		Spool:             s.spool,
		Walker: func() *backup.Walker {
			w := s.f.walker("db-binlog")
			w.Root = s.spool.Dir

			return w
		},
		DumpSet:     "db-dump",
		Window:      14 * 24 * time.Hour,
		CommitEvery: 2 * time.Second,
		floor:       time.Second,
	}
}

func (s *source) start(r *Receiver) *liveReceiver {
	l := &liveReceiver{stop: make(chan struct{}), done: make(chan struct{})}

	go func() {
		l.err = r.Run(context.Background(), l.stop)
		close(l.done)
	}()

	s.t.Cleanup(func() { _ = l.end() })

	return l
}

// end stops the receiver and returns how it ended.
func (l *liveReceiver) end() error {
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}

	<-l.done

	return l.err
}

// position is where the server's binlog ends.
func (s *source) position() (string, int64) {
	s.t.Helper()

	srv, err := ReadServer(context.Background(), s.db)
	require.NoError(s.t, err)

	file, pos, err := Position(context.Background(), s.db, srv)
	require.NoError(s.t, err)

	return file, int64(pos)
}

// committed is the binlog set's latest commit; nil without one.
func (s *source) committed() *proto.Commit {
	ref, err := s.f.index.LatestCommit(context.Background(), "db-binlog")
	if errors.Is(err, backup.ErrNotFound) {
		return nil
	}

	require.NoError(s.t, err)

	commit, err := getCommit(context.Background(), s.f.index, ref)
	require.NoError(s.t, err)

	return commit
}

// caughtUp waits until the binlog set holds the server's binlog up to its
// current end.
func (s *source) caughtUp() *proto.Commit {
	s.t.Helper()

	file, pos := s.position()

	var last *proto.Commit

	require.Eventually(s.t, func() bool {
		last = s.committed()
		if last == nil {
			return false
		}

		meta := last.GetMetadata()
		c := compareBinlogs(meta[MetaEndBinlog], file)

		return c > 0 || c == 0 && parseInt(meta[MetaEndPos]) >= pos
	}, time.Minute, 200*time.Millisecond, "the binlog set reaches %s:%d", file, pos)

	return last
}

func (s *source) target(replayImage string) *mysqlContainer {
	s.t.Helper()

	target := startMySQL(s.t, s.net, "mysql:"+s.version, "target")
	target.replayer = target

	if replayImage != "" {
		target.replayer = startReplayer(s.t, s.net, replayImage)
		target.replayHost = "target"
	}

	return target
}

func (s *source) restore(target *mysqlContainer, at time.Time) *RestoreResult {
	s.t.Helper()

	target.sql("DROP DATABASE IF EXISTS app")

	r := &Restore{
		Objects:   s.f.index,
		Latest:    func(ctx context.Context, set string) (*proto.Ref, error) { return s.f.index.LatestCommit(ctx, set) },
		DumpSet:   "db-dump",
		BinlogSet: "db-binlog",
		At:        at,
		Client:    target,
		WorkDir:   s.t.TempDir(),
	}

	result, err := r.Run(context.Background())
	require.NoError(s.t, err)

	return result
}

// held reads a binlog the set's latest commit holds of chain.
func (s *source) held(chain int, name string) []byte {
	s.t.Helper()

	ref, err := s.f.index.LatestCommit(context.Background(), "db-binlog")
	require.NoError(s.t, err)

	commit, err := readBinlogCommit(context.Background(), s.f.index, nil, ref)
	require.NoError(s.t, err)

	for _, node := range commit.chains[chain] {
		if string(node.Stat.Name) != name {
			continue
		}

		path := filepath.Join(s.t.TempDir(), "binlog")
		_, err := (&backup.Restorer{Store: s.f.index, Key: commit.key}).RestoreFile(context.Background(), path, node.Stat, node.Ref)
		require.NoError(s.t, err)

		content, err := os.ReadFile(path)
		require.NoError(s.t, err)

		return content
	}

	s.t.Fatalf("chain %d holds no %s", chain, name)

	return nil
}

func TestReceivingThroughMySQL(t *testing.T) {
	for _, tc := range []struct{ version, replayImage, reset string }{
		{"5.5", "", "RESET MASTER"},
		{"8.0", "percona/percona-server:8.0", "RESET MASTER"},
		{"8.4", "percona/percona-server:8.4", "RESET BINARY LOGS AND GTIDS"},
	} {
		t.Run(tc.version, func(t *testing.T) { receiveRoundTrip(t, tc.version, tc.replayImage, tc.reset) })
	}
}

// receiveRoundTrip dumps a live server, receives the binlogs written after
// it, restores to a moment and to the end through the partial binlog,
// checks a binlog the receiver closed against the server's, and then
// purges the binlogs while the receiver is down and resets them while it
// runs.
func receiveRoundTrip(t *testing.T, version, replayImage, reset string) {
	s := startSource(t, version)

	s.db.insert("before")
	s.dump()

	live := s.start(s.receiver())

	s.db.insert("after")
	s.caughtUp()

	at := tick()
	tick()
	s.db.insert("late")

	commit := s.caughtUp()
	meta := commit.GetMetadata()
	require.NotEqual(t, meta[MetaLastBinlog], meta[MetaEndBinlog], "the binlog being written is committed as partial")

	target := s.target(replayImage)

	s.restore(target, at)
	require.Equal(t, "after|100\nbefore|100", target.rows())

	result := s.restore(target, time.Time{})
	require.Equal(t, "after|100\nbefore|100\nlate|100", target.rows(), "the newest rows are only in the partial binlog")
	require.NotEmpty(t, result.Binlogs)

	closed, _ := s.position()
	s.db.sql("FLUSH BINARY LOGS")
	s.db.insert("flushed")
	s.caughtUp()

	server, err := s.db.exec(context.Background(), "cat", "/var/lib/mysql/"+closed)
	require.NoError(t, err)
	require.True(t, bytes.Equal(server, s.held(1, closed)), "a binlog the receiver closed is the server's byte for byte")

	require.NoError(t, live.end())

	s.db.insert("purged")
	s.db.sql("FLUSH BINARY LOGS")
	s.db.insert("purged2")
	s.db.sql("FLUSH BINARY LOGS")
	last, _ := s.position()
	s.db.sql("PURGE BINARY LOGS TO '" + last + "'")

	require.ErrorIs(t, s.receiver().Run(context.Background(), make(chan struct{})), ErrGap)

	s.dump()
	live = s.start(s.receiver())
	s.db.insert("next")
	s.caughtUp()

	s.db.sql(reset)
	s.db.insert("reset")

	require.Eventually(t, func() bool {
		commit := s.committed()
		return commit.GetMetadata()[MetaChain] == "2" && strings.HasPrefix(commit.GetMetadata()[MetaEndBinlog], "mysql-bin.000001")
	}, time.Minute, 200*time.Millisecond, "the receiver goes on in chain 2 of the same set")

	require.NoError(t, live.end())
}

func TestAReceiverCrashLosesNothing(t *testing.T) {
	s := startSource(t, "8.0")

	s.db.insert("before")
	s.dump()

	for i, tc := range []struct {
		point string
		wipe  bool
	}{
		{"cut", false},
		{"cut", true},
		{"committed", false},
		{"committed", true},
	} {
		s.db.insert(fmt.Sprintf("crash%d", i))

		r := s.receiver()

		crashed := false
		r.hook = func(point string) error {
			if point == tc.point && !crashed {
				crashed = true
				return errors.New("crash")
			}

			return nil
		}

		require.ErrorContains(t, r.Run(context.Background(), make(chan struct{})), "crash")

		if tc.wipe {
			require.NoError(t, os.RemoveAll(s.spool.Dir))
		}

		live := s.start(s.receiver())
		s.caughtUp()
		require.NoError(t, live.end())
	}

	target := s.target("percona/percona-server:8.0")
	s.restore(target, time.Time{})
	require.Equal(t, "before|100\ncrash0|100\ncrash1|100\ncrash2|100\ncrash3|100", target.rows())
}

func TestEncryptedBinlogsAreReceived(t *testing.T) {
	s := startSource(t, "8.0", "--early-plugin-load=keyring_file.so", "--keyring-file-data=/var/lib/mysql-keyring/keyring", "--binlog-encryption=ON")
	require.Equal(t, "1", s.db.sql("SELECT @@binlog_encryption"))

	s.db.insert("before")
	s.dump()

	live := s.start(s.receiver())
	s.db.insert("after")
	s.caughtUp()
	require.NoError(t, live.end())

	target := s.target("percona/percona-server:8.0")
	s.restore(target, time.Time{})
	require.Equal(t, "after|100\nbefore|100", target.rows())
}

func TestTwoReceiversWithOneServerIDOnMySQL55(t *testing.T) {
	s := startSource(t, "5.5")

	s.db.insert("before")
	s.dump()

	first := s.receiver()
	first.ServerID = 1234

	second := s.receiver()
	second.ServerID = 1234
	second.Spool = Spool{Dir: t.TempDir()}
	second.Walker = func() *backup.Walker {
		w := s.f.walker("other-binlog")
		w.Root = second.Spool.Dir

		return w
	}

	a := s.start(first)
	b := s.start(second)

	// the server ends a dump it replaced only once the dump has an event to
	// send, so the source keeps writing
	var err error

	for deadline := time.Now().Add(2 * time.Minute); err == nil && time.Now().Before(deadline); {
		s.db.sql(fmt.Sprintf("INSERT INTO app.rows VALUES ('k', %d)", time.Now().UnixNano()%1e9))

		select {
		case <-a.done:
			err = a.err
		case <-b.done:
			err = b.err
		case <-time.After(300 * time.Millisecond):
		}
	}

	require.ErrorIs(t, err, ErrServerIDInUse, "one of the two ends with the server id named")
}

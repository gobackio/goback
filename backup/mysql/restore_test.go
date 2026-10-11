package mysql

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func (f *fixture) restore(binlogSet string, at time.Time) (*RestoreResult, error) {
	f.t.Helper()

	r := &Restore{
		Objects:   f.index,
		Latest:    func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) },
		DumpSet:   "db-dump",
		BinlogSet: binlogSet,
		At:        at,
		Client:    f.srv,
		WorkDir:   f.t.TempDir(),
	}

	return r.Run(f.ctx)
}

// tick waits for the next second, which separates what a restore to a
// moment can tell apart.
func tick() time.Time {
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 10*time.Millisecond)))
	return time.Now()
}

func TestRestoreLoadsTheNewestDumpAndReplaysItsChain(t *testing.T) {
	f := newFixture(t)

	f.srv.write()
	dump := f.mustDump()
	f.srv.write()
	f.srv.flush()
	f.srv.write()
	f.mustBinlog()

	result, err := f.restore("db-binlog", time.Time{})
	require.NoError(t, err)
	require.Equal(t, dump.Ref.Hash, result.DumpRef.Hash)
	require.Equal(t, []string{f.srv.dump}, f.srv.loaded)
	require.Equal(t, []string{"binlog.000001", "binlog.000002"}, result.Binlogs)
	require.Len(t, f.srv.replayed, 1)
	require.Equal(t, "--start-position="+dump.Commit.Metadata[MetaBinlogPos], f.srv.replayed[0][0])
	require.Len(t, f.srv.replayed[0], 3, "the start position and the two files")
}

func TestRestoreToAMomentStopsInTheFileThatHoldsIt(t *testing.T) {
	f := newFixture(t)

	f.mustDump()
	f.srv.write()
	f.srv.flush()
	at := tick()
	tick()
	f.srv.write()
	f.srv.flush()
	f.srv.write()
	f.srv.flush()
	f.mustBinlog()

	result, err := f.restore("db-binlog", at)
	require.NoError(t, err)
	require.Equal(t, []string{"binlog.000001", "binlog.000002"}, result.Binlogs, "binlog.000002 was written when at falls")
	require.Contains(t, result.ReplayArgs, "--stop-datetime="+at.UTC().Format(time.DateTime))
}

func TestRestoreWithoutBinlogSetLoadsTheDumpAlone(t *testing.T) {
	f := newFixture(t)

	f.mustDump()
	f.srv.write()
	f.mustBinlog()

	result, err := f.restore("", time.Time{})
	require.NoError(t, err)
	require.Len(t, f.srv.loaded, 1)
	require.Empty(t, f.srv.replayed)
	require.Empty(t, result.Binlogs)
}

func TestRestorePicksTheDumpOfTheChainThatReachesTheMoment(t *testing.T) {
	f := newFixture(t)

	first := f.mustDump()
	f.srv.write()
	beforeReset := tick()
	tick()
	f.srv.flush()
	f.mustBinlog()

	f.srv.resetMaster()
	f.srv.write()
	f.mustBinlog()
	afterReset := tick()
	tick()

	second := f.mustDump()
	require.Equal(t, "2", second.Commit.Metadata[MetaChain])
	f.srv.write()
	f.mustBinlog()

	result, err := f.restore("db-binlog", time.Time{})
	require.NoError(t, err)
	require.Equal(t, second.Ref.Hash, result.DumpRef.Hash)
	require.Equal(t, []string{"binlog.000002"}, result.Binlogs)

	result, err = f.restore("db-binlog", beforeReset)
	require.NoError(t, err)
	require.Equal(t, first.Ref.Hash, result.DumpRef.Hash)
	require.Equal(t, []string{"binlog.000001"}, result.Binlogs)

	_, err = f.restore("db-binlog", afterReset)
	require.ErrorIs(t, err, ErrNoDump, "no dump of the new chain is that old, and the old chain ended before")
}

func TestRestoreRefusesADumpWhoseStartFileClosedBeforeIt(t *testing.T) {
	held := &binlogCommit{chain: 1, chains: map[int][]*proto.TreeNode{1: {
		{Stat: &proto.FileInfo{Name: []byte("binlog.000001"), MtimeNs: time.Now().Add(-time.Hour).UnixNano()}},
	}}}

	dump := &proto.Commit{ScanStartNs: time.Now().UnixNano(), Timestamp: time.Now().Unix(),
		Metadata: map[string]string{MetaBinlogFile: "binlog.000001", MetaBinlogPos: "4", MetaServerID: "1", MetaChain: "1"}}

	_, reason := (&Restore{}).unusable(dump, held)
	require.Contains(t, reason, "closed before the dump began")
}

func TestRestoreRefusesATargetThatHoldsDatabases(t *testing.T) {
	f := newFixture(t)
	f.mustDump()
	f.srv.databases = []string{"app"}

	_, err := f.restore("", time.Time{})
	require.ErrorContains(t, err, "app")
	require.Empty(t, f.srv.loaded)
}

func TestRestoreNeedsMysqlbinlogBeforeItLoadsAnything(t *testing.T) {
	f := newFixture(t)
	f.mustDump()
	f.srv.write()
	f.srv.flush()
	f.mustBinlog()
	f.srv.noBinlog = true

	_, err := f.restore("db-binlog", time.Time{})
	require.ErrorContains(t, err, "mysqlbinlog")
	require.Empty(t, f.srv.loaded)
}

func TestRestoreToADirectoryWritesTheFilesOnly(t *testing.T) {
	f := newFixture(t)
	f.mustDump()
	f.srv.write()
	f.srv.flush()
	f.mustBinlog()

	to := filepath.Join(t.TempDir(), "out")
	r := &Restore{
		Objects:   f.index,
		Latest:    func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) },
		DumpSet:   "db-dump",
		BinlogSet: "db-binlog",
		Client:    f.srv,
		To:        to,
	}

	result, err := r.Run(f.ctx)
	require.NoError(t, err)
	require.Empty(t, f.srv.loaded)
	require.Empty(t, f.srv.replayed)

	content, err := os.ReadFile(result.DumpPath)
	require.NoError(t, err)
	require.Equal(t, f.srv.dump, string(content))

	binlog, err := os.ReadFile(filepath.Join(to, "binlog.000001"))
	require.NoError(t, err)

	disk, err := os.ReadFile(filepath.Join(f.srv.dir, "binlog.000001"))
	require.NoError(t, err)
	require.Equal(t, disk, binlog)
	require.Equal(t, filepath.Join(to, "binlog.000001"), result.ReplayArgs[len(result.ReplayArgs)-1])
}

func TestReplayArgs(t *testing.T) {
	at := time.Date(2026, 10, 2, 16, 30, 0, 500, time.FixedZone("CEST", 2*3600))

	require.Equal(t, []string{"--start-position=107", "--stop-datetime=2026-10-02 14:30:00", "--skip-gtids", "a", "b"},
		ReplayArgs(map[string]string{MetaBinlogPos: "107", MetaGTIDMode: "ON"}, at, []string{"a", "b"}))
	require.Equal(t, []string{"--start-position=4", "a"}, ReplayArgs(map[string]string{MetaBinlogPos: "4"}, time.Time{}, []string{"a"}))
}

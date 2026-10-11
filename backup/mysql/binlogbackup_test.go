package mysql

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestBinlogRotatesOnlyWhenTheActiveBinlogHoldsSomething(t *testing.T) {
	f := newFixture(t)
	f.srv.write()

	result := f.mustBinlog()
	require.Equal(t, 1, f.srv.flushes)
	require.Equal(t, map[int][]string{1: {"binlog.000001"}}, f.chains())
	require.Equal(t, "binlog.000001", result.Commit.Metadata[MetaLastBinlog])

	result, err := f.binlog()
	require.NoError(t, err)
	require.Nil(t, result, "an idle server gets no new binlog and no commit")
	require.Equal(t, 1, f.srv.flushes)
}

func TestBinlogKeepsWhatTheServerPurgedAndDropsWhatLeftTheWindow(t *testing.T) {
	f := newFixture(t)

	f.mustDump()
	f.srv.write()
	f.srv.flush()
	f.srv.write()
	f.mustBinlog()
	require.Equal(t, []string{"binlog.000001", "binlog.000002"}, f.chains()[1])

	f.srv.purge("binlog.000003")
	f.srv.write()
	f.mustBinlog()
	require.Equal(t, []string{"binlog.000001", "binlog.000002", "binlog.000003"}, f.chains()[1], "binlogs the server purged are carried")

	f.srv.write()
	f.mustDump()

	f.offset += 15 * 24 * time.Hour
	f.srv.write()
	f.mustBinlog()
	require.Equal(t, []string{"binlog.000004"}, f.chains()[1], "only the newest dump's binlogs are kept once the others left the window")
}

func TestBinlogFailsOnAGapUntilANewerDump(t *testing.T) {
	f := newFixture(t)

	f.mustDump()
	f.srv.write()
	f.mustBinlog()

	f.srv.write()
	f.srv.flush()
	f.srv.write()
	f.srv.flush()
	f.srv.purge("binlog.000003")
	f.srv.write()

	_, err := f.binlog()
	require.ErrorIs(t, err, ErrGap)
	require.ErrorContains(t, err, "binlog.000002")

	_, err = f.binlog()
	require.ErrorIs(t, err, ErrGap, "every run fails until a newer dump")

	f.mustDump()
	f.srv.write()
	f.mustBinlog()
	require.Equal(t, []string{"binlog.000001", "binlog.000003", "binlog.000004", "binlog.000005"}, f.chains()[1],
		"the binlogs before the hole stay for the older dump")
}

func TestBinlogOpensANewChainAfterAReset(t *testing.T) {
	f := newFixture(t)

	f.mustDump()
	f.srv.write()
	f.srv.flush()
	f.srv.write()
	f.mustBinlog()
	require.Equal(t, map[int][]string{1: {"binlog.000001", "binlog.000002"}}, f.chains())

	f.srv.resetMaster()
	f.srv.write()
	result := f.mustBinlog()
	require.Equal(t, "2", result.Commit.Metadata[MetaChain])
	require.Equal(t, map[int][]string{1: {"binlog.000001", "binlog.000002"}, 2: {"binlog.000001"}}, f.chains(),
		"the old chain stays while the newest dump needs it")

	f.srv.write()
	require.Equal(t, "2", f.mustDump().Commit.Metadata[MetaChain])

	f.offset += 15 * 24 * time.Hour
	f.srv.write()
	f.mustBinlog()
	require.Equal(t, map[int][]string{2: {"binlog.000002"}}, f.chains(), "the old chain drops once no kept dump needs it")
}

func TestBinlogFollowsAChainADumpOpened(t *testing.T) {
	f := newFixture(t)

	f.srv.write()
	f.mustBinlog()

	for range 5 {
		f.srv.flush()
	}

	f.mustDump()
	f.srv.resetMaster()
	for range 3 {
		f.srv.write()
		f.srv.flush()
	}

	f.srv.purge("binlog.000004")
	require.Equal(t, "2", f.mustDump().Commit.Metadata[MetaChain])

	f.srv.write()
	result := f.mustBinlog()
	require.Equal(t, "2", result.Commit.Metadata[MetaChain])
	require.Equal(t, []string{"binlog.000004"}, f.chains()[2])
}

func TestBinlogRefusesAnotherServer(t *testing.T) {
	f := newFixture(t)
	f.srv.write()
	f.mustBinlog()

	f.srv.uuid = "99999999-71ca-11e1-9e33-c80aa9429562"
	f.srv.write()

	_, err := f.binlog()
	require.ErrorIs(t, err, ErrForeignBinlog)
}

func TestBinlogRefusesABinlogWrittenByAnotherServerID(t *testing.T) {
	f := newFixture(t)
	f.srv.write()
	f.srv.flush()
	f.srv.serverID = 2

	_, err := f.binlog()
	require.ErrorIs(t, err, ErrForeignBinlog)
}

func TestBinlogFailsWhenAHeldBinlogChanged(t *testing.T) {
	f := newFixture(t)
	f.srv.write()
	f.mustBinlog()

	require.NoError(t, os.WriteFile(filepath.Join(f.srv.dir, "binlog.000001"), binlogContent(1, false, 3), 0o640))
	f.srv.write()

	_, err := f.binlog()
	require.ErrorIs(t, err, ErrBinlogChanged)
}

func TestBinlogNeedsLogBin(t *testing.T) {
	f := newFixture(t)
	f.srv.logBin = false

	_, err := f.binlog()
	require.ErrorIs(t, err, ErrNoBinlog)
}

func TestWindowKeepsPerChainFromTheOldestDumpInside(t *testing.T) {
	now := time.Now()
	dump := func(chain, file string, age time.Duration) *proto.Commit {
		return &proto.Commit{ReceivedAtNs: now.Add(-age).UnixNano(), Metadata: map[string]string{MetaChain: chain, MetaBinlogFile: file}}
	}

	cutoffs, ok := Window([]*proto.Commit{
		dump("2", "b.000009", time.Hour),
		dump("2", "b.000004", 2*time.Hour),
		dump("1", "b.000120", 3*time.Hour),
		dump("1", "b.000100", 30*24*time.Hour),
	}, 14*24*time.Hour, now)
	require.True(t, ok)
	require.Equal(t, map[int]string{2: "b.000004", 1: "b.000120"}, cutoffs)

	cutoffs, ok = Window([]*proto.Commit{dump("1", "b.000100", 30*24*time.Hour), dump("1", "b.000050", 40*24*time.Hour)}, 14*24*time.Hour, now)
	require.True(t, ok)
	require.Equal(t, map[int]string{1: "b.000100"}, cutoffs, "the newest dump counts when none is inside the window")

	_, ok = Window(nil, time.Hour, now)
	require.False(t, ok)
}

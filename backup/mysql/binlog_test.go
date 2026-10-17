package mysql

import (
	"bytes"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func TestReadBinlogServerIDOnRealBinlogs(t *testing.T) {
	for _, tc := range []struct {
		file     string
		serverID uint32
	}{
		{"testdata/binlog-5.5-closed", 7},
		{"testdata/binlog-5.5-active", 7},
		{"testdata/binlog-8.0-closed", 8},
		{"testdata/binlog-8.0-active", 8},
	} {
		content, err := os.ReadFile(tc.file)
		require.NoError(t, err)

		serverID, err := readBinlogServerID(bytes.NewReader(content))
		require.NoError(t, err, tc.file)
		require.Equal(t, tc.serverID, serverID, tc.file)
	}
}

func TestReadBinlogServerIDRefusesWhatIsNoBinlog(t *testing.T) {
	_, err := readBinlogServerID(bytes.NewReader([]byte("\xfdbin encrypted")))
	require.Error(t, err)

	content, err := os.ReadFile("testdata/binlog-8.0-closed")
	require.NoError(t, err)
	content[4+4] = 2

	_, err = readBinlogServerID(bytes.NewReader(content))
	require.Error(t, err)
}

func TestBinlogsOrderByNumberPastSixDigits(t *testing.T) {
	names := []string{"mysql-bin.1000000", "mysql-bin.999999", "mysql-bin.000010"}
	slices.SortFunc(names, compareBinlogs)
	require.Equal(t, []string{"mysql-bin.000010", "mysql-bin.999999", "mysql-bin.1000000"}, names)

	require.True(t, follows("mysql-bin.999999", "mysql-bin.1000000"))
	require.False(t, follows("mysql-bin.000001", "other.000002"))
}

func TestCheckContiguousNamesTheMissingBinlogs(t *testing.T) {
	names := []string{"b.000001", "b.000003", "b.000004", "b.000007"}

	require.ErrorContains(t, checkContiguous(names, "b.000003"), "b.000005 to b.000006")
	require.ErrorIs(t, checkContiguous(names, ""), ErrGap)
	require.NoError(t, checkContiguous(names[1:3], "b.000003"))
	require.ErrorContains(t, checkContiguous(names[2:], "b.000002"), "b.000002 to b.000003")
	require.NoError(t, checkContiguous(names[:2], "b.000009"), "nothing past the dump's start yet")
}

func TestIdentityByUUIDElseServerIDAndBase(t *testing.T) {
	require.True(t, Identity{ServerID: 1, ServerUUID: "a"}.Same(Identity{ServerID: 2, ServerUUID: "a"}))
	require.False(t, Identity{ServerID: 1, ServerUUID: "a"}.Same(Identity{ServerID: 1, ServerUUID: "b"}))
	require.True(t, Identity{ServerID: 1, Base: "mysql-bin"}.Same(Identity{ServerID: 1, Base: "mysql-bin"}))
	require.False(t, Identity{ServerID: 1, Base: "mysql-bin"}.Same(Identity{ServerID: 1, Base: "other"}))
	require.False(t, Identity{ServerID: 1}.Same(Identity{ServerID: 2}))
}

func TestWindowKeepsPerChainFromTheNewestDumpBeforeItsStart(t *testing.T) {
	now := time.Now()
	dump := func(chain, file string, age time.Duration) *proto.Commit {
		return &proto.Commit{ReceivedAtNs: now.Add(-age).UnixNano(), Metadata: map[string]string{MetaChain: chain, MetaBinlogFile: file}}
	}

	cutoffs, ok := Window([]*proto.Commit{
		dump("3", "b.000009", time.Hour),
		dump("3", "b.000004", 2*time.Hour),
		dump("2", "b.000120", 3*time.Hour),
		dump("2", "b.000100", 30*24*time.Hour),
		dump("1", "b.000050", 40*24*time.Hour),
	}, 14*24*time.Hour, now)
	require.True(t, ok)
	require.Equal(t, map[int]string{3: "b.000004", 2: "b.000100"}, cutoffs)

	cutoffs, ok = Window([]*proto.Commit{dump("1", "b.000100", 30*24*time.Hour), dump("1", "b.000050", 40*24*time.Hour)}, 14*24*time.Hour, now)
	require.True(t, ok)
	require.Equal(t, map[int]string{1: "b.000100"}, cutoffs, "the newest dump counts when none is inside the window")

	cutoffs, ok = Window([]*proto.Commit{dump("1", "b.000100", time.Hour), dump("1", "b.000050", 2*time.Hour)}, 14*24*time.Hour, now)
	require.True(t, ok)
	require.Equal(t, map[int]string{1: "b.000050"}, cutoffs, "the oldest dump counts when all are inside the window")

	_, ok = Window(nil, time.Hour, now)
	require.False(t, ok)
}

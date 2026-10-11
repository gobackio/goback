package mysql

import (
	"bytes"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadBinlogHeaderOnRealBinlogs(t *testing.T) {
	for _, tc := range []struct {
		file     string
		serverID uint32
		inUse    bool
		end      int64
	}{
		{"testdata/binlog-5.5-closed", 7, false, 107},
		{"testdata/binlog-5.5-active", 7, true, 107},
		{"testdata/binlog-8.0-closed", 8, false, 157},
		{"testdata/binlog-8.0-active", 8, true, 157},
	} {
		content, err := os.ReadFile(tc.file)
		require.NoError(t, err)

		hdr, err := readBinlogHeader(bytes.NewReader(content))
		require.NoError(t, err, tc.file)
		require.Equal(t, tc.serverID, hdr.serverID, tc.file)
		require.Equal(t, tc.inUse, hdr.inUse, tc.file)
		require.Equal(t, tc.end, hdr.end, tc.file)
	}
}

func TestReadBinlogHeaderRefusesWhatIsNoBinlog(t *testing.T) {
	_, err := readBinlogHeader(bytes.NewReader([]byte("\xfdbin encrypted")))
	require.Error(t, err)

	content, err := os.ReadFile("testdata/binlog-8.0-closed")
	require.NoError(t, err)
	content[4+4] = 2

	_, err = readBinlogHeader(bytes.NewReader(content))
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

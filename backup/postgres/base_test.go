package postgres

import (
	"archive/tar"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"
	"testing/iotest"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// baseTar builds a tar as pg_basebackup -Ft of Postgres 17 streams it, of
// the cluster systemID.
func baseTar(t *testing.T, systemID uint64, entries ...string) []byte {
	t.Helper()

	files := map[string][]byte{
		"backup_label":                    []byte(backupLabel),
		"PG_VERSION":                      []byte("17\n"),
		"global/pg_control":               control(systemID),
		"base/5/16384":                    relation(),
		"pg_wal/000000010000000000000007": header(1, 7*segSize, systemID),
		"backup_manifest":                 []byte(backupManifest),
	}

	if len(entries) == 0 {
		entries = []string{"backup_label", "PG_VERSION", "global/pg_control", "base/5/16384", "pg_wal/000000010000000000000007", "backup_manifest"}
	}

	return tarOf(t, files, entries)
}

// oldBaseTar builds a tar as pg_basebackup -Ft of Postgres 9.6 streams it,
// its WAL cut short after the backup's end.
func oldBaseTar(t *testing.T) []byte {
	t.Helper()

	wal, err := os.ReadFile("testdata/wal-9.6")
	require.NoError(t, err)

	files := map[string][]byte{
		"backup_label":                     []byte(backupLabel96),
		"PG_VERSION":                       []byte("9.6\n"),
		"global/pg_control":                control(42),
		"base/5/16384":                     relation(),
		"pg_xlog/000000010000000000000003": wal,
	}

	return tarOf(t, files, []string{"backup_label", "PG_VERSION", "base/5/16384", "global/pg_control", "pg_xlog/000000010000000000000003"})
}

func control(systemID uint64) []byte {
	control := make([]byte, 8192)
	binary.LittleEndian.PutUint64(control, systemID)

	return control
}

func relation() []byte {
	relation := make([]byte, 1<<20)
	_, _ = rand.Read(relation)

	return relation
}

func tarOf(t *testing.T, files map[string][]byte, entries []string) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range entries {
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: int64(len(files[name]))}))
		_, err := tw.Write(files[name])
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())

	return buf.Bytes()
}

func (f *walFixture) runBase(content []byte, wait func() error) (*backup.WalkResult, error) {
	b := &BaseBackup{Walker: &backup.Walker{Index: f.index, Objects: f.index, Set: "db-base", AgentID: "a", Workers: 1}}
	return b.Run(f.ctx, bytes.NewReader(content), wait)
}

func TestABaseBackupCommitsItsTarAndWhatItRecords(t *testing.T) {
	f := newWALFixture(t)
	content := baseTar(t, 42)

	result, err := f.runBase(content, nil)
	require.NoError(t, err)

	require.Equal(t, map[string]string{
		MetaSystemID:     "42",
		MetaTimeline:     "1",
		MetaStartLSN:     "0/7000028",
		MetaStopLSN:      "0/7000120",
		MetaStartWALFile: "000000010000000000000007",
		backup.MetaKind:  backup.KindPostgresBase,
	}, result.Commit.Metadata)

	tree, err := backup.OpenTree(f.ctx, f.index, result.Commit.Tree, nil, nil)
	require.NoError(t, err)

	var names []string
	for _, node := range tree.Nodes {
		names = append(names, string(node.Stat.Name))
	}

	require.Equal(t, []string{"PG_VERSION", "backup_label", "backup_manifest", "base", "global", "pg_wal"}, names, "the data directory as a tree")
	require.Equal(t, proto.NodeType_NODE_DIRECTORY, tree.Nodes[3].Stat.Type)
}

func TestABaseBackupWithoutAManifestStopsWhereItsWALSays(t *testing.T) {
	f := newWALFixture(t)

	result, err := f.runBase(oldBaseTar(t), nil)
	require.NoError(t, err)

	require.Equal(t, map[string]string{
		MetaSystemID:     "42",
		MetaTimeline:     "1",
		MetaStartLSN:     "0/3000028",
		MetaStopLSN:      "0/30000F8",
		MetaStartWALFile: "000000010000000000000003",
		backup.MetaKind:  backup.KindPostgresBase,
	}, result.Commit.Metadata)
}

func TestABaseBackupWhoseEndOfArchiveIsCutShortIsWhole(t *testing.T) {
	f := newWALFixture(t)

	content := baseTar(t, 42)
	_, err := f.runBase(content[:len(content)-244], nil)
	require.NoError(t, err)

	_, err = f.runBase(content[:len(content)-1526], nil)
	require.Error(t, err, "a tar cut short inside its last file")
}

func TestABaseBackupWhoseWriterFailedCommitsNothing(t *testing.T) {
	f := newWALFixture(t)
	failed := errors.New("pg_basebackup: exit status 1")

	_, err := f.runBase(baseTar(t, 42), func() error { return failed })
	require.ErrorIs(t, err, failed)

	_, err = f.index.LatestCommit(f.ctx, "db-base")
	require.ErrorIs(t, err, backup.ErrNotFound)
}

func TestABaseBackupWithoutAManifestOrItsEndInItsWALCommitsNothing(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42, "backup_label", "global/pg_control", "base/5/16384", "pg_wal/000000010000000000000007"), nil)
	require.ErrorContains(t, err, "backup_manifest")

	_, err = f.index.LatestCommit(f.ctx, "db-base")
	require.ErrorIs(t, err, backup.ErrNotFound)
}

func TestABaseBackupOfAnotherClusterIsRefused(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42), nil)
	require.NoError(t, err)

	_, err = f.runBase(baseTar(t, 43), nil)
	require.ErrorIs(t, err, ErrForeignCluster)
}

func TestABaseBackupWhoseStreamBreaksCommitsNothing(t *testing.T) {
	f := newWALFixture(t)
	broken := errors.New("broken pipe")

	b := &BaseBackup{Walker: &backup.Walker{Index: f.index, Objects: f.index, Set: "db-base", AgentID: "a", Workers: 1}}
	_, err := b.Run(f.ctx, io.MultiReader(bytes.NewReader(baseTar(t, 42)[:4096]), iotest.ErrReader(broken)), nil)
	require.ErrorIs(t, err, broken)

	_, err = f.index.LatestCommit(f.ctx, "db-base")
	require.ErrorIs(t, err, backup.ErrNotFound)
}

func TestABaseBackupThatIsNoTarIsRefused(t *testing.T) {
	f := newWALFixture(t)

	garbage := make([]byte, 4<<20)
	_, _ = rand.Read(garbage)

	_, err := f.runBase(garbage, nil)
	require.ErrorContains(t, err, "reading the stream")
}

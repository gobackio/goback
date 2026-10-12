package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func (f *walFixture) restore(at time.Time) (*RestoreResult, string, error) {
	f.t.Helper()

	dir := filepath.Join(f.t.TempDir(), "data")
	r := &Restore{
		Objects: f.index,
		Latest:  func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) },
		BaseSet: "db-base",
		WALSet:  "db-wal",
		At:      at,
		RestoreCommand: func(wal *proto.Ref) string {
			return "goback postgres wal-get --commit " + hex.EncodeToString(wal.Hash) + " %f %p"
		},
	}

	result, err := r.Run(f.ctx, dir)

	return result, dir, err
}

func TestARestoreWritesTheNewestBaseAndRecoversFromTheWALSet(t *testing.T) {
	f := newWALFixture(t)

	content := baseTar(t, 42)
	_, err := f.runBase(content, nil)
	require.NoError(t, err)

	f.spoolSegment(7, 42)
	f.spoolSegment(8, 42)
	wal, err := f.run()
	require.NoError(t, err)

	result, dir, err := f.restore(time.Time{})
	require.NoError(t, err)
	require.Equal(t, wal.Ref.Hash, result.WAL.Hash)

	entries := tar.NewReader(bytes.NewReader(content))
	for {
		hdr, err := entries.Next()
		if err != nil {
			break
		}

		want := new(bytes.Buffer)
		_, _ = want.ReadFrom(entries)

		got, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(hdr.Name)))
		require.NoError(t, err)
		require.Equal(t, want.Bytes(), got, hdr.Name)
	}

	require.FileExists(t, filepath.Join(dir, "recovery.signal"))

	conf, err := os.ReadFile(filepath.Join(dir, "postgresql.auto.conf"))
	require.NoError(t, err)
	require.Contains(t, string(conf), "restore_command = 'goback postgres wal-get --commit "+hex.EncodeToString(wal.Ref.Hash)+" %f %p'")
	require.NotContains(t, string(conf), "recovery_target_time")
}

func TestARestoreBeforePostgres12RecoversThroughRecoveryConf(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(oldBaseTar(t), nil)
	require.NoError(t, err)

	f.spoolSegment(3, 42)
	wal, err := f.run()
	require.NoError(t, err)

	_, dir, err := f.restore(time.Time{})
	require.NoError(t, err)

	require.NoFileExists(t, filepath.Join(dir, "recovery.signal"))
	require.NoFileExists(t, filepath.Join(dir, "postgresql.auto.conf"))

	conf, err := os.ReadFile(filepath.Join(dir, "recovery.conf"))
	require.NoError(t, err)
	require.Contains(t, string(conf), "restore_command = 'goback postgres wal-get --commit "+hex.EncodeToString(wal.Ref.Hash)+" %f %p'")
	require.Contains(t, string(conf), "recovery_target_timeline = 'latest'")
}

func TestARestoreToAMomentSetsItAsTheTarget(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42), nil)
	require.NoError(t, err)

	f.spoolSegment(7, 42)
	_, err = f.run()
	require.NoError(t, err)

	at := time.Now().Add(time.Hour)
	_, dir, err := f.restore(at)
	require.NoError(t, err)

	conf, err := os.ReadFile(filepath.Join(dir, "postgresql.auto.conf"))
	require.NoError(t, err)
	require.Contains(t, string(conf), "recovery_target_time = '"+at.UTC().Format("2006-01-02 15:04:05.999999+00")+"'")

	_, _, err = f.restore(time.Now().Add(-time.Hour))
	require.ErrorIs(t, err, ErrNoBase, "no base ended before then")
}

func TestARestoreRefusesABaseTheWALSetCannotContinue(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42), nil)
	require.NoError(t, err)

	f.spoolSegment(8, 42)
	_, err = f.run()
	require.NoError(t, err)

	_, _, err = f.restore(time.Time{})
	require.ErrorIs(t, err, ErrNoBase)
	require.ErrorContains(t, err, "before the WAL set's first file")
}

func TestARestoreRefusesABaseOfAnotherCluster(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42), nil)
	require.NoError(t, err)

	f.spoolSegment(7, 43)
	_, err = f.run()
	require.NoError(t, err)

	_, _, err = f.restore(time.Time{})
	require.ErrorIs(t, err, ErrNoBase)
	require.ErrorContains(t, err, "of cluster 42")
}

func TestARestoreWithoutWALRecoversOnlyItsBase(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42), nil)
	require.NoError(t, err)

	result, dir, err := f.restore(time.Time{})
	require.NoError(t, err)
	require.Nil(t, result.WAL)
	require.NoFileExists(t, filepath.Join(dir, "recovery.signal"))
}

func TestARestoreWithoutAWALSetRestoresTheBaseAlone(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42), nil)
	require.NoError(t, err)

	f.spoolSegment(7, 42)
	_, err = f.run()
	require.NoError(t, err)

	dir := filepath.Join(t.TempDir(), "data")
	r := &Restore{Objects: f.index, Latest: func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) }, BaseSet: "db-base"}
	result, err := r.Run(f.ctx, dir)
	require.NoError(t, err)
	require.Nil(t, result.WAL)
	require.FileExists(t, filepath.Join(dir, "PG_VERSION"))
	require.NoFileExists(t, filepath.Join(dir, "recovery.signal"))
}

func TestARestoreLeavesADirectoryInUseAlone(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42), nil)
	require.NoError(t, err)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "PG_VERSION"), []byte("17"), 0o600))

	r := &Restore{Objects: f.index, Latest: func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) }, BaseSet: "db-base", WALSet: "db-wal"}
	_, err = r.Run(f.ctx, dir)
	require.ErrorContains(t, err, "not empty")
}

func TestFetchWALWritesWhatTheCommitHolds(t *testing.T) {
	f := newWALFixture(t)

	name := f.spoolSegment(7, 42)
	want, err := os.ReadFile(filepath.Join(f.spool.Dir, name))
	require.NoError(t, err)

	wal, err := f.run()
	require.NoError(t, err)

	dst := filepath.Join(t.TempDir(), "RECOVERYXLOG")
	require.NoError(t, FetchWAL(f.ctx, f.index, nil, wal.Ref, name, dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, want, got)

	err = FetchWAL(f.ctx, f.index, nil, wal.Ref, "00000002.history", dst)
	require.ErrorIs(t, err, backup.ErrNotFound)
}

func TestFetchWALRefusesARefThatIsNoCommit(t *testing.T) {
	f := newWALFixture(t)

	name := f.spoolSegment(7, 42)

	wal, err := f.run()
	require.NoError(t, err)

	obj, err := f.index.Get(f.ctx, wal.Ref)
	require.NoError(t, err)

	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	err = FetchWAL(f.ctx, f.index, key, obj.GetCommit().Tree, name, filepath.Join(t.TempDir(), "RECOVERYXLOG"))
	require.ErrorContains(t, err, "not a commit")
}

func TestARestoreReadsAWALSetArchiveCommandFilled(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42), nil)
	require.NoError(t, err)

	dir := t.TempDir()
	name := walName(1, 7)
	want := append(header(1, 7*segSize, 42), 7)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), want, 0o600))

	w := &backup.Walker{Index: f.index, Objects: f.index, Set: "db-wal", AgentID: "a", Root: dir, Workers: 1, Metadata: map[string]string{
		MetaFirstWALFile: name, MetaLastWALFile: name, MetaSystemID: "42", MetaCapture: "archive",
	}}
	wal, err := w.Run(f.ctx)
	require.NoError(t, err)

	result, _, err := f.restore(time.Time{})
	require.NoError(t, err)
	require.Equal(t, wal.Ref.Hash, result.WAL.Hash)

	dst := filepath.Join(t.TempDir(), "RECOVERYXLOG")
	require.NoError(t, FetchWAL(f.ctx, f.index, nil, wal.Ref, name, dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// streamingStore serves a file's parts only through ReadParts, like the
// goback:// client.
type streamingStore struct {
	backup.ObjectStore
}

func (s streamingStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := s.ObjectStore.Get(ctx, ref)
	if err == nil && obj.GetBlob() != nil {
		return nil, backup.ErrNotFound
	}

	return obj, err
}

func (s streamingStore) ReadParts(ctx context.Context, file *proto.Ref, skip []int, fn func(int, *proto.Object) error) error {
	obj, err := s.ObjectStore.Get(ctx, file)
	if err != nil {
		return err
	}

	parts, err := backup.FileParts(ctx, s.ObjectStore, obj.GetFile())
	if err != nil {
		return err
	}

	for i, part := range parts {
		blob, err := s.ObjectStore.Get(ctx, part.Ref)
		if err != nil {
			return err
		}

		if err := fn(i, blob); err != nil {
			return err
		}
	}

	return nil
}

func TestFetchWALStreamsFromAStoreThatServesNoParts(t *testing.T) {
	f := newWALFixture(t)

	name := walName(1, 7)
	want := append(header(1, 7*segSize, 42), make([]byte, 1<<20)...)
	_, err := rand.Read(want[len(want)-1<<20:])
	require.NoError(t, err)

	require.NoError(t, f.spool.put(name, want))

	wal, err := f.run()
	require.NoError(t, err)

	dst := filepath.Join(t.TempDir(), "RECOVERYXLOG")
	require.NoError(t, FetchWAL(f.ctx, streamingStore{f.index}, nil, wal.Ref, name, dst))

	got, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestOnlyTheNewestTimelinesLastPartialSegmentIsServed(t *testing.T) {
	names := []string{walName(1, 1), walName(1, 2) + partialSuffix, "00000002.history", walName(2, 2), walName(2, 3) + partialSuffix}

	require.True(t, servesPartial(names, walName(2, 3)))
	require.False(t, servesPartial(names, walName(1, 2)), "an older timeline's")
	require.False(t, servesPartial(names, walName(2, 4)), "none held")
	require.False(t, servesPartial([]string{walName(1, 2) + partialSuffix, walName(1, 3)}, walName(1, 2)), "older than a whole segment")
	require.False(t, servesPartial([]string{walName(1, 2) + partialSuffix, "00000002.history"}, walName(1, 2)), "a newer timeline began")
}

func TestARestoreRefusesABaseThatStopsPastWhereTheNewestTimelineForked(t *testing.T) {
	for _, c := range []struct {
		history string
		usable  bool
	}{
		{"1\t0/7000100\tbefore 2026-10-12 10:00:00+00\n", false},
		{"1\t0/7800000\tbefore 2026-10-12 10:00:00+00\n", true},
	} {
		t.Run(c.history[2:11], func(t *testing.T) {
			f := newWALFixture(t)

			_, err := f.runBase(baseTar(t, 42), nil)
			require.NoError(t, err)

			f.spoolSegment(7, 42)
			f.spoolSegment(8, 42)
			require.NoError(t, f.spool.put("00000002.history", []byte(c.history)))

			_, err = f.receive(f.receiving(2, 7, 0x900000))
			require.NoError(t, err)

			_, _, err = f.restore(time.Time{})
			if c.usable {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrNoBase)
			}
		})
	}
}

func TestARestoreRefusesABaseThatStopsAfterTheWALSetEnds(t *testing.T) {
	f := newWALFixture(t)

	_, err := f.runBase(baseTar(t, 42), nil)
	require.NoError(t, err)

	_, err = f.receive(f.receiving(1, 7, 0x100))
	require.NoError(t, err)

	_, _, err = f.restore(time.Time{})
	require.ErrorIs(t, err, ErrNoBase)
	require.ErrorContains(t, err, "stops at 0/7000120, after the WAL set's end at 0/7000100")

	_, err = f.receive(f.receiving(1, 7, 0x200))
	require.NoError(t, err)

	_, _, err = f.restore(time.Time{})
	require.NoError(t, err)
}

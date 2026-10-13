package postgres

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// frontToBack is the local disk written only front to back, without links
// or modes, as an FTP server is.
type frontToBack struct{ backup.WriteFS }

func (frontToBack) Symlink(context.Context, string, string) error {
	return fmt.Errorf("no links here: %w", errors.ErrUnsupported)
}

func (frontToBack) Chmod(context.Context, string, os.FileMode) error {
	return fmt.Errorf("no modes here: %w", errors.ErrUnsupported)
}

func TestTheWALCopiedHoldsWhatRecoveryReadsUpToOneSegmentAfterTheMoment(t *testing.T) {
	at := func(minute int) time.Time { return time.Date(2026, 10, 13, 12, minute, 0, 0, time.UTC) }

	files := &commitFiles{}
	for _, file := range []struct {
		name   string
		closed time.Time
	}{
		{walName(1, 2), at(1)},
		{walName(1, 3), at(2)},
		{walName(1, 3) + ".00000028.backup", at(2)},
		{walName(1, 4), at(3)},
		{walName(1, 5), at(4)},
		{"00000002.history", at(4)},
		{walName(2, 4), at(5)},
		{walName(2, 5), at(6)},
		{walName(2, 6) + partialSuffix, at(7)},
	} {
		files.names = append(files.names, file.name)
		files.nodes = append(files.nodes, &proto.TreeNode{Stat: &proto.FileInfo{Name: []byte(file.name), Size: segSize, MtimeNs: file.closed.UnixNano()}})
	}

	h, err := parseHistory(2, []byte("1\t0/4800000\tbefore 2026-10-13 12:03:00+00\n"))
	require.NoError(t, err)

	copied := func(until time.Time) []string {
		var names []string
		for _, i := range files.copies(h, walName(1, 3), until) {
			names = append(names, files.names[i])
		}

		return names
	}

	all := []string{"00000002.history", walName(1, 3), walName(1, 4), walName(2, 4), walName(2, 5), walName(2, 6) + partialSuffix}
	require.Equal(t, all, copied(time.Time{}), "from the base's start, on the newest timeline's history")
	require.Equal(t, all, copied(at(8)), "to the end when the moment is past every segment")
	require.Equal(t, all[:5], copied(at(5).Add(-time.Second)))
	require.Equal(t, all[:3], copied(at(1)))
}

func TestARestoreWithoutARestoreCommandCopiesTheWALIntoTheDataDirectory(t *testing.T) {
	for name, fsys := range map[string]backup.WriteFS{"local": nil, "front to back": frontToBack{backup.LocalFS{}}} {
		t.Run(name, func(t *testing.T) {
			f := newWALFixture(t)

			_, err := f.runBase(baseTar(t, 42), nil)
			require.NoError(t, err)

			seven := f.spoolSegment(7, 42)
			want, err := os.ReadFile(filepath.Join(f.spool.Dir, seven))
			require.NoError(t, err)

			f.spoolSegment(8, 42)
			_, err = f.receive(f.receiving(1, 9, 1000))
			require.NoError(t, err)

			dir := filepath.Join(t.TempDir(), "data")
			r := &Restore{
				Objects: f.index,
				Latest:  func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) },
				BaseSet: "db-base",
				WALSet:  "db-wal",
				FS:      fsys,
			}

			result, err := r.Run(f.ctx, dir)
			require.NoError(t, err)
			require.Equal(t, []string{seven, walName(1, 8), walName(1, 9)}, result.Copied)
			require.Equal(t, fsys != nil, result.ModeUnset)

			got, err := os.ReadFile(filepath.Join(dir, "pg_wal", seven))
			require.NoError(t, err)
			require.Equal(t, want, got, "the WAL set's copy over the base's")

			got, err = os.ReadFile(filepath.Join(dir, "pg_wal", walName(1, 9)))
			require.NoError(t, err)
			require.Len(t, got, segSize, "the partial segment padded")
			require.Equal(t, segmentContent(1, 9, 1000), got[:1000])

			conf, err := os.ReadFile(filepath.Join(dir, "postgresql.auto.conf"))
			require.NoError(t, err)
			require.Contains(t, string(conf), "restore_command = 'exit 1'")
			require.FileExists(t, filepath.Join(dir, "recovery.signal"))
		})
	}
}

func TestARestoreOntoATreeWithoutLinksFailsOnABaseThatHasThem(t *testing.T) {
	f := newWALFixture(t)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	tr := tar.NewReader(bytes.NewReader(baseTar(t, 42)))

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}

		require.NoError(t, err)
		require.NoError(t, tw.WriteHeader(hdr))
		_, err = io.Copy(tw, tr)
		require.NoError(t, err)

		if hdr.Name == "base/5/16384" {
			require.NoError(t, tw.WriteHeader(&tar.Header{Name: "pg_tblspc/16385", Typeflag: tar.TypeSymlink, Linkname: "/srv/space", Mode: 0o777}))
		}
	}

	require.NoError(t, tw.Close())

	_, err := f.runBase(buf.Bytes(), nil)
	require.NoError(t, err)

	r := &Restore{
		Objects: f.index,
		Latest:  func(ctx context.Context, set string) (*proto.Ref, error) { return f.index.LatestCommit(ctx, set) },
		BaseSet: "db-base",
		FS:      frontToBack{backup.LocalFS{}},
	}

	_, err = r.Run(f.ctx, filepath.Join(t.TempDir(), "data"))
	require.ErrorContains(t, err, "pg_tblspc/16385")
}

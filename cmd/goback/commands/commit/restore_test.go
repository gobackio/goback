package commit

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

type pickIndex struct {
	backup.Index
	objects map[string]*proto.Object
	newest  *proto.Commit
	asked   time.Time
}

func (x *pickIndex) Get(_ context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, ok := x.objects[string(ref.Hash)]
	if !ok {
		return nil, backup.ErrNotFound
	}

	return obj, nil
}

func (x *pickIndex) CommitInfo(_ context.Context, _ string, notAfter time.Time, _ int) ([]*proto.Commit, error) {
	x.asked = notAfter
	if x.newest == nil {
		return nil, nil
	}

	return []*proto.Commit{x.newest}, nil
}

func TestPickByRefChecksTheSet(t *testing.T) {
	tree := &proto.Ref{Hash: make([]byte, proto.HashSize)}
	ours := proto.NewObject(&proto.Commit{Timestamp: 1, Tree: tree, BackupSet: "world"})
	theirs := proto.NewObject(&proto.Commit{Timestamp: 2, Tree: tree, BackupSet: "other"})
	newest := &proto.Commit{Timestamp: 3, Tree: tree, BackupSet: "world"}
	index := &pickIndex{
		objects: map[string]*proto.Object{string(ours.Ref().Hash): ours, string(theirs.Ref().Hash): theirs},
		newest:  newest,
	}
	c := &commit{ctx: context.Background(), index: index, set: "world"}

	c.ref = ours.Ref()
	picked, err := c.pick()
	require.NoError(t, err)
	require.Same(t, ours.GetCommit(), picked, "a ref wins over the newest commit")

	c.ref = theirs.Ref()
	_, err = c.pick()
	require.Error(t, err)

	c.ref = proto.NewObject(&proto.Commit{Timestamp: 4, Tree: tree, BackupSet: "world"}).Ref()
	_, err = c.pick()
	require.Error(t, err)

	at := time.Unix(1700000000, 0)
	c.ref, c.when = nil, at
	picked, err = c.pick()
	require.NoError(t, err)
	require.Same(t, newest, picked)
	require.Equal(t, at, index.asked)

	index.newest = nil
	_, err = c.pick()
	require.Error(t, err)
}

func TestRestoreTarget(t *testing.T) {
	now := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

	when, err := restoreTarget(now, "", "", "")
	require.NoError(t, err)
	require.Equal(t, now, when)

	when, err = restoreTarget(now, "2h", "", "")
	require.NoError(t, err)
	require.Equal(t, now.Add(-2*time.Hour), when)

	when, err = restoreTarget(now, "", "2026-10-01T08:30:00+02:00", "")
	require.NoError(t, err)
	require.True(t, when.Equal(time.Date(2026, 10, 1, 6, 30, 0, 0, time.UTC)))

	for _, args := range [][3]string{{"2h", "2026-10-01T08:30:00Z", ""}, {"2h", "", "ab"}, {"", "2026-10-01T08:30:00Z", "ab"}} {
		_, err = restoreTarget(now, args[0], args[1], args[2])
		require.Error(t, err, "%q", args)
	}

	_, err = restoreTarget(now, "", "yesterday", "")
	require.Error(t, err)
}

type restoreProgressRecord struct {
	Msg        string `json:"msg"`
	FilesDone  int64  `json:"files_done"`
	FilesTotal int64  `json:"files_total"`
	BytesDone  int64  `json:"bytes_done"`
	BytesTotal int64  `json:"bytes_total"`
}

// lastProgress restores into dst and returns the final progress record
// the restore logged as a JSON line.
func lastProgress(t *testing.T, c *commit, dst string) restoreProgressRecord {
	t.Helper()

	var stderr bytes.Buffer
	common.SetOutput(true)
	slog.SetDefault(slog.New(slog.NewJSONHandler(&stderr, nil)))
	t.Cleanup(func() { common.SetOutput(false) })

	c.base = dst
	require.NoError(t, c.restore())

	var last *restoreProgressRecord
	lines := bufio.NewScanner(&stderr)
	for lines.Scan() {
		var record restoreProgressRecord
		require.NoError(t, json.Unmarshal(lines.Bytes(), &record))

		if record.Msg == "progress" {
			fields := map[string]any{}
			require.NoError(t, json.Unmarshal(lines.Bytes(), &fields))
			for _, name := range []string{"files_done", "files_total", "bytes_done", "bytes_total"} {
				require.Contains(t, fields, name)
			}

			last = &record
		}
	}

	require.NotNil(t, last, "no progress record")

	return *last
}

func TestRestoreProgressCountsAFilesBytesWhileItIsWritten(t *testing.T) {
	p := &restoreProgress{}

	p.wrote(30)
	p.wrote(30)
	require.Equal(t, int64(60), p.bytesDone.Load(), "parts count before the file is through")
	require.Zero(t, p.filesDone.Load())

	p.done(100, 60)
	require.Equal(t, int64(100), p.bytesDone.Load(), "what no part wrote, as for a file found unchanged, counts at the end")
	require.Equal(t, int64(1), p.filesDone.Load())

	p.wrote(15)
	p.done(10, 15)
	require.Equal(t, int64(115), p.bytesDone.Load(), "a file never takes back what its parts counted")
}

func TestRestoreProgressCountsWhatTheRestoreCovers(t *testing.T) {
	ctx := context.Background()

	index := sql.NewMemory(t.Name(), nil)
	packs, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(memblob.OpenBucket(nil))), pack.WithArchiveIndex(index))
	require.NoError(t, err)
	index.ObjectStore = packs
	require.NoError(t, index.Open())
	require.NoError(t, packs.Open())
	t.Cleanup(func() { _ = packs.Close() })

	src := t.TempDir()
	for name, size := range map[string]int{"a.txt": 5, "sub/b.txt": 7, "sub/deeper/c.txt": 11} {
		path := filepath.Join(src, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte("x"), size), 0o644))
	}

	walker := &backup.Walker{Index: index, Objects: index, Set: "world", AgentID: "a", Root: src, Workers: 1}
	result, err := walker.Run(ctx)
	require.NoError(t, err)

	newCommit := func(from string) *commit {
		return &commit{
			ctx:              ctx,
			index:            index,
			store:            index,
			set:              "world",
			ref:              result.Ref,
			from:             from,
			restorer:         &backup.Restorer{Store: index, Workers: 2, Overwrite: backup.OverwriteIfChanged},
			progressInterval: time.Hour,
		}
	}

	whole := restoreProgressRecord{Msg: "progress", FilesDone: 3, FilesTotal: 3, BytesDone: 23, BytesTotal: 23}
	dst := t.TempDir()
	require.Equal(t, whole, lastProgress(t, newCommit(""), dst))
	require.Equal(t, whole, lastProgress(t, newCommit(""), dst), "unchanged files count as done")

	sub := restoreProgressRecord{Msg: "progress", FilesDone: 2, FilesTotal: 2, BytesDone: 18, BytesTotal: 18}
	require.Equal(t, sub, lastProgress(t, newCommit("sub"), t.TempDir()))
}

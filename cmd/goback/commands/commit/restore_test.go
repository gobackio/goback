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
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

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

func TestARestoreDeletesNeitherInTheWorkingDirectoryNorInARoot(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	require.NoError(t, os.Mkdir("sub", 0o755))

	wd, err := os.Getwd()
	require.NoError(t, err)

	require.Error(t, checkDeleteTarget("."))
	require.Error(t, checkDeleteTarget(wd))
	require.Error(t, checkDeleteTarget(filepath.Join("sub", "..")))
	require.Error(t, checkDeleteTarget(filepath.VolumeName(wd)+string(filepath.Separator)))
	require.NoError(t, checkDeleteTarget("sub"))
	require.NoError(t, checkDeleteTarget(filepath.Join(wd, "sub")))
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

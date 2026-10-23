package job_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup/job"
	"github.com/stretchr/testify/require"
)

func TestAPathIsBackedUpWhenEveryFilterKeepsItAndUnchangedDirectoriesAreTakenAsTheyWere(t *testing.T) {
	ctx := context.Background()

	repo, err := goback.Open(ctx, goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	src := t.TempDir()
	write := func(name, content string) {
		path := filepath.Join(src, filepath.FromSlash(name))
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	}

	// older than the first run's scan start, or the next run reads them
	// again as racily clean
	old := time.Now().Add(-time.Hour)
	for _, name := range []string{"docs/a.txt", "docs/b.log", "cache/c.txt", "node_modules/d.js", "e.txt"} {
		write(name, name)
		require.NoError(t, os.Chtimes(filepath.Join(src, filepath.FromSlash(name)), old, old))
	}

	var mtx sync.Mutex
	asked := map[string]int{}

	filters := []job.Filter{
		func(rel string) bool {
			mtx.Lock()
			asked[rel]++
			mtx.Unlock()
			return true
		},
		func(rel string) bool { return rel != "cache" },
		func(rel string) bool { return !strings.HasSuffix(rel, "node_modules") },
		job.Globs(nil, nil, []string{"/**/*.log"}),
	}

	backup := func(unchanged func(string) bool) int64 {
		result, err := job.Path(ctx, &job.Env{Store: repo.Index, Index: repo.Index, AgentID: "pc"}, job.PathSettings{
			Set: "pc/files", Dir: src, ProgressInterval: -1, Filters: filters, Unchanged: unchanged,
		})
		require.NoError(t, err)

		return result.Read
	}

	held := func(name string) bool {
		versions, err := repo.Index.FileInfo(ctx, "pc/files", name, time.Now().Add(time.Hour), 10)
		require.NoError(t, err)
		return len(versions) > 0
	}

	backup(nil)

	for rel, n := range asked {
		require.Equal(t, 1, n, "a filter is asked once per path: %s", rel)
	}
	require.Contains(t, asked, "cache", "kept-out paths included")

	for name, want := range map[string]bool{"docs/a.txt": true, "e.txt": true, "docs/b.log": false, "cache/c.txt": false, "node_modules/d.js": false} {
		require.Equal(t, want, held(name), name)
	}

	write("docs/a.txt", "edited")
	require.NoError(t, os.Chtimes(filepath.Join(src, "docs", "a.txt"), time.Now(), time.Now().Add(time.Minute)))

	require.Zero(t, backup(func(rel string) bool { return rel == "docs" }), "docs is taken from the latest commit")
	require.EqualValues(t, 1, backup(nil), "and read once nothing vouches for it")
}

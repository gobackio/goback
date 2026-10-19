package job_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/stretchr/testify/require"
)

func TestAPathRunHandsItsCountersToProgressAndLastWhenItEnds(t *testing.T) {
	ctx := context.Background()

	repo, err := goback.Open(ctx, goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	src := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(src, name), []byte(name), 0o644))
	}

	var (
		mtx   sync.Mutex
		heard []backup.WalkResult
	)

	result, err := job.Path(ctx, &job.Env{Store: repo.Index, Index: repo.Index, AgentID: "pc"}, job.PathSettings{
		Set: "pc/files", Dir: src, ProgressInterval: -1,
		Progress: func(r backup.WalkResult) {
			mtx.Lock()
			heard = append(heard, r)
			mtx.Unlock()
		},
	})
	require.NoError(t, err)

	mtx.Lock()
	defer mtx.Unlock()

	require.NotEmpty(t, heard, "progress is heard though the log line is off")
	require.EqualValues(t, 3, heard[len(heard)-1].Files)
	require.Equal(t, result.Bytes, heard[len(heard)-1].Bytes)
}

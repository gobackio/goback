package job_test

import (
	"context"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"

	"github.com/stretchr/testify/require"
)

// panickingFS is the local disk whose listings panic.
type panickingFS struct{ backup.LocalFS }

func (panickingFS) ReadDir(context.Context, string) ([]fs.DirEntry, error) {
	panic("the walk broke")
}

func TestThePostHookRunsWhenTheWalkPanics(t *testing.T) {
	ctx := context.Background()

	repo, err := goback.Open(ctx, goback.Settings{Storage: t.TempDir(), Index: t.TempDir(), AgentID: "pc"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	marker := filepath.Join(t.TempDir(), "released")

	require.PanicsWithValue(t, "the walk broke", func() {
		_, _ = job.Path(ctx, &job.Env{Store: repo.Index, Index: repo.Index, AgentID: "pc"}, job.PathSettings{
			Set:              "pc/files",
			Dir:              t.TempDir(),
			FS:               panickingFS{},
			ScanWorkers:      -1,
			ProgressInterval: -1,
			PreHook:          "echo quiesced",
			PostHook:         "echo released > " + marker,
		})
	})

	require.FileExists(t, marker)
}

package goback

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/backup/job"

	"github.com/stretchr/testify/require"
)

func TestAPathBackupWithVSSBacksUpWhetherOrNotAShadowCopyCanBeTaken(t *testing.T) {
	ctx := context.Background()

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o600))

	env, err := repo.Env(ctx, Caches{})
	require.NoError(t, err)
	defer env.Close()

	result, err := job.Path(ctx, env, job.PathSettings{Set: "files", Dir: dir, ProgressInterval: -1, VSS: true})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Files)
}

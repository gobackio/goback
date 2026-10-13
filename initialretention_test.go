package goback

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/admin"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// backUp commits a directory of one file to set through repo.
func backUp(t *testing.T, ctx context.Context, repo *Repo, set string, initial *retention.Policy) {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte(set), 0o600))

	env, err := repo.Env(ctx, Caches{})
	require.NoError(t, err)
	defer env.Close()

	_, err = job.Path(ctx, env, job.PathSettings{Set: set, Dir: dir, ProgressInterval: -1, InitialRetention: initial})
	require.NoError(t, err)
}

func ownPolicy(t *testing.T, ctx context.Context, repo *Repo, set string) *retention.Policy {
	t.Helper()

	ret, err := repo.Index.(admin.Index).GetPolicy(ctx, set)
	require.NoError(t, err)

	return ret.Policy
}

func TestALocalRunCreatesItsSetWithItsInitialRetention(t *testing.T) {
	ctx := context.Background()

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	backUp(t, ctx, repo, "db/base", &retention.DatabaseBase)
	require.Equal(t, &retention.DatabaseBase, ownPolicy(t, ctx, repo, "db/base"))

	backUp(t, ctx, repo, "db/base", &retention.DatabaseLog)
	require.Equal(t, &retention.DatabaseBase, ownPolicy(t, ctx, repo, "db/base"), "a set that exists keeps its own")

	backUp(t, ctx, repo, "files", nil)
	require.Nil(t, ownPolicy(t, ctx, repo, "files"))
}

func TestARunAgainstAServerCreatesItsSetWithItsInitialRetention(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan error, 1)
	go func() {
		served <- repo.Serve(ctx, listener, ServeOptions{Secret: "s3cret", PlaintextBehindProxy: true, Schedule: maintenance.DefaultSchedule})
	}()

	agent, err := Open(ctx, Settings{Storage: "goback+insecure://s3cret@" + listener.Addr().String(), AgentID: "agent"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = agent.Close() })

	backUp(t, ctx, agent, "db/wal", &retention.DatabaseLog)
	require.Equal(t, &retention.DatabaseLog, ownPolicy(t, ctx, repo, "db/wal"))

	require.NoError(t, repo.Index.(admin.Index).SetPolicy(ctx, "db/wal", nil))
	backUp(t, ctx, agent, "db/wal", &retention.DatabaseBase)
	require.Nil(t, ownPolicy(t, ctx, repo, "db/wal"), "an explicit inherit stays")

	_, err = agent.Store.(backup.SessionStore).BeginSession(ctx, &backup.Session{Set: "bad", InitialRetention: &retention.Policy{}})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

	cancel()
	require.NoError(t, <-served)
}

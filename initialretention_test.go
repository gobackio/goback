package goback

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobackio/goback/admin"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index"
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

// serving serves a fresh repo on loopback until the test ends and opens
// an agent's repo of it.
func serving(t *testing.T, ctx context.Context) (repo, agent *Repo) {
	t.Helper()

	ctx, cancel := context.WithCancel(ctx)

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan error, 1)
	go func() {
		served <- repo.Serve(ctx, listener, ServeOptions{Secret: "s3cret", PlaintextBehindProxy: true, Schedule: maintenance.DefaultSchedule})
	}()

	agent, err = Open(ctx, Settings{Storage: "goback+insecure://s3cret@" + listener.Addr().String(), AgentID: "agent"})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = agent.Close()
		cancel()
		require.NoError(t, <-served)
		_ = repo.Close()
	})

	return repo, agent
}

func TestARunAgainstAServerCreatesItsSetWithItsInitialRetention(t *testing.T) {
	ctx := context.Background()
	repo, agent := serving(t, ctx)

	backUp(t, ctx, agent, "db/wal", &retention.DatabaseLog)
	require.Equal(t, &retention.DatabaseLog, ownPolicy(t, ctx, repo, "db/wal"))

	require.NoError(t, repo.Index.(admin.Index).SetPolicy(ctx, "db/wal", nil))
	backUp(t, ctx, agent, "db/wal", &retention.DatabaseBase)
	require.Nil(t, ownPolicy(t, ctx, repo, "db/wal"), "an explicit inherit stays")

	_, err := agent.Store.(backup.SessionStore).BeginSession(ctx, &backup.Session{Set: "bad", InitialRetention: &retention.Policy{}})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
}

func TestAnAgentSetsItsSetsRetentionThroughTheServer(t *testing.T) {
	ctx := context.Background()
	repo, agent := serving(t, ctx)
	backUp(t, ctx, agent, "files", nil)

	policies := agent.Index.(interface {
		GetPolicy(ctx context.Context, set string) (index.SetRetention, error)
		SetPolicy(ctx context.Context, set string, p *retention.Policy) (bool, error)
	})

	own := &retention.Policy{KeepLast: 2, KeepWithin: 36 * time.Hour, Brackets: []retention.Bracket{{Period: retention.Yearly}}}
	pending, err := policies.SetPolicy(ctx, "files", own)
	require.NoError(t, err)
	require.False(t, pending)
	require.Equal(t, own, ownPolicy(t, ctx, repo, "files"))

	got, err := policies.GetPolicy(ctx, "files")
	require.NoError(t, err)
	require.Equal(t, index.SetRetention{Policy: own, Effective: *own}, got)

	def := retention.Policy{KeepLast: 3, Brackets: []retention.Bracket{{Period: retention.Daily, Count: 10}}}
	require.NoError(t, repo.Index.(admin.Index).SetDefaultPolicy(ctx, &def))
	_, err = policies.SetPolicy(ctx, "files", nil)
	require.NoError(t, err)

	got, err = policies.GetPolicy(ctx, "files")
	require.NoError(t, err)
	require.Nil(t, got.Policy)
	require.Equal(t, def, got.Effective, "the set keeps the store's default")

	_, err = policies.SetPolicy(ctx, "files", &retention.Policy{KeepLast: 1, Brackets: []retention.Bracket{{Period: "fortnightly", Count: 2}}})
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

	_, err = policies.GetPolicy(ctx, "nothing")
	require.Equal(t, codes.NotFound, status.Code(err), "%v", err)
}

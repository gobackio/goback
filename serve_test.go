package goback

import (
	"context"
	"net"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/stretchr/testify/require"
)

func TestAServedRepoIsAStoreForAnAgentUntilItsContextEnds(t *testing.T) {
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

	_, err = agent.Index.LatestCommit(ctx, "world")
	require.ErrorIs(t, err, backup.ErrNotFound, "an agent with the secret reaches the store")

	stranger, err := Open(ctx, Settings{Storage: "goback+insecure://wrong@" + listener.Addr().String(), AgentID: "stranger"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = stranger.Close() })

	_, err = stranger.Index.LatestCommit(ctx, "world")
	require.Error(t, err)
	require.NotErrorIs(t, err, backup.ErrNotFound, "one without it does not")

	cancel()
	require.NoError(t, <-served)
}

func TestServeRefusesNoSecretAndUndeclaredPlaintext(t *testing.T) {
	repo := &Repo{}

	require.Error(t, repo.Serve(context.Background(), nil, ServeOptions{PlaintextBehindProxy: true}))
	require.Error(t, repo.Serve(context.Background(), nil, ServeOptions{Secret: "s3cret"}))
}

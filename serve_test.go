package goback

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

func TestAServedRepoIsReadyOnlyWhileItsIndexAnswers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan error, 1)
	go func() {
		served <- repo.Serve(ctx, listener, ServeOptions{Secret: "s3cret", PlaintextBehindProxy: true})
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ready := func() healthpb.HealthCheckResponse_ServingStatus {
		t.Helper()

		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)

		return resp.GetStatus()
	}

	require.Equal(t, healthpb.HealthCheckResponse_SERVING, ready())

	require.NoError(t, repo.Index.Close())
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, ready())

	cancel()
	require.NoError(t, <-served)
}

// failingListener accepts one connection, then fails once fail is closed.
type failingListener struct {
	net.Listener
	accepted int
	fail     chan struct{}
}

func (l *failingListener) Accept() (net.Conn, error) {
	if l.accepted++; l.accepted > 1 {
		<-l.fail
		return nil, errors.New("the listener broke")
	}

	return l.Listener.Accept()
}

func TestServeStopsServingWhenItsListenerFails(t *testing.T) {
	repo, err := Open(context.Background(), Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	inner, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = inner.Close() })

	listener := &failingListener{Listener: inner, fail: make(chan struct{})}

	served := make(chan error, 1)
	go func() {
		served <- repo.Serve(context.Background(), listener, ServeOptions{Secret: "s3cret", PlaintextBehindProxy: true})
	}()

	conn, err := grpc.NewClient(inner.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	check := func() error {
		_, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{Service: "liveness"})
		return err
	}

	require.NoError(t, check())

	close(listener.fail)
	require.ErrorContains(t, <-served, "the listener broke")
	require.Error(t, check(), "a connection the server took before is served no more")
}

func TestServeRefusesNoSecretAndUndeclaredPlaintext(t *testing.T) {
	repo := &Repo{}

	require.Error(t, repo.Serve(context.Background(), nil, ServeOptions{PlaintextBehindProxy: true}))
	require.Error(t, repo.Serve(context.Background(), nil, ServeOptions{Secret: "s3cret"}))
}

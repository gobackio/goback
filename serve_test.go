package goback

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
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

// panickingIndex panics where an agent asks for a set's latest commit.
type panickingIndex struct{ backup.Index }

func (panickingIndex) LatestCommit(context.Context, string) (*proto.Ref, error) {
	panic("the index broke")
}

func TestACallThatPanicsFailsAloneAndLogsItsStack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opened, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = opened.Close() })

	var logged syncBuffer
	repo := &Repo{Store: opened.Store, Index: panickingIndex{opened.Index}, settings: Settings{Logger: slog.New(slog.NewTextHandler(&logged, nil))}}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan error, 1)
	go func() {
		served <- repo.Serve(ctx, listener, ServeOptions{Secret: "s3cret", PlaintextBehindProxy: true})
	}()

	agent, err := Open(ctx, Settings{Storage: "goback+insecure://s3cret@" + listener.Addr().String(), AgentID: "agent"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = agent.Close() })

	_, err = agent.Index.LatestCommit(ctx, "world")
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, logged.String(), "panickingIndex.LatestCommit", "the stack is logged")

	_, err = agent.Index.CommitInfo(ctx, "world", time.Now(), 1)
	require.NoError(t, err, "the server serves on")

	cancel()
	require.NoError(t, <-served)
}

type syncBuffer struct {
	mtx sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	return b.buf.String()
}

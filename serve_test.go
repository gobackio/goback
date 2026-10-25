package goback

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/admin"
	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"
	adminpb "github.com/gobackio/goback/proto/admin"
	"github.com/gobackio/goback/storage"
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

// servedWithKeys serves a new store to the backup key "backup" and to
// adminKey, and returns its address.
func servedWithKeys(t *testing.T, adminKey string) string {
	t.Helper()

	addr, _ := servedRepo(t, adminKey)

	return addr
}

// servedRepo is servedWithKeys that also returns the served repo.
func servedRepo(t *testing.T, adminKey string) (string, *Repo) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	repo, err := Open(ctx, Settings{Storage: t.TempDir(), Index: t.TempDir()})
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	served := make(chan error, 1)
	go func() {
		served <- repo.Serve(ctx, listener, ServeOptions{Secret: "backup", AdminKey: adminKey, PlaintextBehindProxy: true, Schedule: maintenance.DefaultSchedule})
	}()

	t.Cleanup(func() {
		cancel()
		<-served
		_ = repo.Close()
	})

	return listener.Addr().String(), repo
}

// clientWith is a client of the store server at addr presenting key.
func clientWith(t *testing.T, addr, key string) (*storage.Client, adminpb.AdminClient) {
	t.Helper()

	repo, err := Open(context.Background(), Settings{Storage: "goback+insecure://" + key + "@" + addr, AgentID: "agent"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = repo.Close() })

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(auth.Credentials{Secret: key, AgentID: "agent", Plaintext: true}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return repo.Index.(*storage.Client), adminpb.NewAdminClient(conn)
}

func TestTheBackupKeyBacksUpReadsAndPinsAndDeletesNothing(t *testing.T) {
	ctx := context.Background()
	addr := servedWithKeys(t, "admin")
	agent, agentAdmin := clientWith(t, addr, "backup")

	sctx, err := agent.BeginSession(ctx, &backup.Session{Set: "world"})
	require.NoError(t, err)

	a, aNode := fileNamed("a.txt", "one")
	tree := proto.NewObject(&proto.Tree{Nodes: []*proto.TreeNode{aNode}})
	require.NoError(t, agent.PutObjects(sctx, []*proto.Object{a, tree}))
	require.NoError(t, agent.PutCommits(sctx, []*proto.Object{snapshotOf("world", 1, tree)}))

	head, err := agent.LatestCommit(ctx, "world")
	require.NoError(t, err)

	pctx, err := agent.BeginSession(ctx, &backup.Session{Set: "world"})
	require.NoError(t, err)
	require.NoError(t, agent.Put(pctx, proto.NewObject(&proto.Pin{Target: head})))
	require.NoError(t, agent.EndSession(pctx))

	pins, err := agent.Pins(ctx)
	require.NoError(t, err)
	require.Len(t, pins, 1)

	_, err = agentAdmin.ListSets(ctx, &adminpb.ListSetsRequest{})
	require.NoError(t, err)

	trash := int32(0)
	denied := map[string]error{
		"DeleteCommit":   agent.DeleteCommit(ctx, head),
		"UndeleteCommit": agent.UndeleteCommit(ctx, head),
		"DeleteSet":      agent.DeleteSet(ctx, "world", true),
		"Unpin":          agent.Unpin(ctx, pins[0].GetRef()),
	}
	_, denied["SetRetention"] = agent.SetPolicy(ctx, "world", &retention.Policy{KeepLast: 1})
	_, denied["Admin.SetRetention"] = agentAdmin.SetRetention(ctx, &adminpb.SetRetentionRequest{TrashDays: &trash})
	_, denied["Admin.DeleteSet"] = agentAdmin.DeleteSet(ctx, &adminpb.DeleteSetRequest{Name: "world", Erase: true})
	_, denied["Admin.Retire"] = agentAdmin.Retire(ctx, &adminpb.RetireRequest{})
	_, denied["Admin.CollectGarbage"] = agentAdmin.CollectGarbage(ctx, &adminpb.CollectGarbageRequest{})
	_, denied["Admin.SetStorePolicy"] = agentAdmin.SetStorePolicy(ctx, &adminpb.SetStorePolicyRequest{Mode: "none"})

	for call, err := range denied {
		require.Equal(t, codes.PermissionDenied, status.Code(err), call)
	}

	operator, _ := clientWith(t, addr, "admin")
	_, err = operator.SetPolicy(ctx, "world", &retention.Policy{KeepLast: 1})
	require.NoError(t, err)
	require.NoError(t, operator.Unpin(ctx, pins[0].GetRef()))
	require.NoError(t, operator.DeleteSet(ctx, "world", false))
}

// escrowedCopy is key escrowed under passphrase.
func escrowedCopy(t *testing.T, key *storekey.Key, passphrase string) backup.EscrowedKey {
	t.Helper()

	escrow, err := key.Escrow(passphrase)
	require.NoError(t, err)

	return backup.EscrowedKey{KeyID: key.IDString(), Escrowed: escrow}
}

func TestTheBackupKeyEscrowsTheStoreKeyOnlyWhereTheStoreKeepsNoCopyOfIt(t *testing.T) {
	ctx := context.Background()
	addr := servedWithKeys(t, "admin")
	agent, _ := clientWith(t, addr, "backup")
	operator, _ := clientWith(t, addr, "admin")

	key, err := storekey.Generate("s1")
	require.NoError(t, err)
	first, second := escrowedCopy(t, key, "correct horse"), escrowedCopy(t, key, "battery staple")

	require.NoError(t, agent.PutEscrowedKey(ctx, first), "into a store that keeps none")
	require.NoError(t, agent.PutEscrowedKey(ctx, first), "the copy kept")
	require.Equal(t, codes.PermissionDenied, status.Code(agent.PutEscrowedKey(ctx, second)), "beside the copy kept")

	require.NoError(t, operator.PutEscrowedKey(ctx, second))

	kept, err := agent.EscrowedKeys(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []backup.EscrowedKey{first, second}, kept)
}

func TestTheBackupKeyLengthensASetsRetentionAndNeverShortensIt(t *testing.T) {
	ctx := context.Background()
	repo, agent, operator := serving(t, ctx)
	backUp(t, ctx, agent, "files", &retention.DatabaseBase)

	type policies interface {
		SetPolicy(ctx context.Context, set string, p *retention.Policy) (bool, error)
	}

	set := func(r *Repo, p *retention.Policy) error {
		_, err := r.Index.(policies).SetPolicy(ctx, "files", p)
		return err
	}

	longer := &retention.Policy{KeepLast: 2, Brackets: []retention.Bracket{
		{Period: retention.Daily, Count: 90},
		{Period: retention.Weekly, Count: 4},
		{Period: retention.Monthly},
	}}
	require.NoError(t, set(agent, longer))
	require.Equal(t, longer, ownPolicy(t, ctx, repo, "files"))

	for name, shorter := range map[string]*retention.Policy{
		"fewer days": {KeepLast: 2, Brackets: []retention.Bracket{
			{Period: retention.Daily, Count: 30}, {Period: retention.Weekly, Count: 4}, {Period: retention.Monthly},
		}},
		"weeks for days": {KeepLast: 2, Brackets: []retention.Bracket{
			{Period: retention.Weekly, Count: 90}, {Period: retention.Weekly, Count: 4}, {Period: retention.Monthly},
		}},
	} {
		require.Equal(t, codes.PermissionDenied, status.Code(set(agent, shorter)), name)
		require.Equal(t, longer, ownPolicy(t, ctx, repo, "files"), name)
	}

	require.NoError(t, set(operator, &retention.DatabaseBase), "the admin key shortens")
	require.Equal(t, &retention.DatabaseBase, ownPolicy(t, ctx, repo, "files"))

	require.NoError(t, set(agent, nil), "the store's default keeps everything")
	require.Nil(t, ownPolicy(t, ctx, repo, "files"))

	require.NoError(t, repo.Index.(admin.Index).SetDefaultPolicy(ctx, &retention.DatabaseBase))
	require.NoError(t, set(agent, longer))
	require.Equal(t, codes.PermissionDenied, status.Code(set(agent, nil)), "the store's default keeps less")
	require.Equal(t, longer, ownPolicy(t, ctx, repo, "files"))
}

func TestWithoutAnAdminKeyNoCallOverTheNetworkDeletes(t *testing.T) {
	ctx := context.Background()
	addr := servedWithKeys(t, "")

	agent, _ := clientWith(t, addr, "backup")
	require.Equal(t, codes.PermissionDenied, status.Code(agent.DeleteSet(ctx, "world", false)))

	guess, _ := clientWith(t, addr, "admin")
	require.Equal(t, codes.Unauthenticated, status.Code(guess.DeleteSet(ctx, "world", false)))
}

func TestAServerHoldsBackAnAddressThatGuessesKeys(t *testing.T) {
	ctx := context.Background()
	addr := servedWithKeys(t, "")

	guess, _ := clientWith(t, addr, "guess")
	for range serveFailures {
		_, err := guess.LatestCommit(ctx, "world")
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}

	agent, _ := clientWith(t, addr, "backup")
	_, err := agent.LatestCommit(ctx, "world")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

// streamCalled is a server stream of a call whose context is ctx.
type streamCalled struct {
	grpc.ServerStream
	ctx context.Context
}

func (s streamCalled) Context() context.Context { return s.ctx }

func TestOnlySoManyUploadsRunAtOnceAndTheRestWait(t *testing.T) {
	slots := uploadSlots(2)
	upload := &grpc.StreamServerInfo{FullMethod: proto.Store_SendUpload_FullMethodName}
	ctx := context.Background()

	release := make(chan struct{})
	running := make(chan struct{}, 3)
	hold := func(any, grpc.ServerStream) error {
		running <- struct{}{}
		<-release
		return nil
	}

	done := make(chan error, 3)
	for range 3 {
		go func() { done <- slots(nil, streamCalled{ctx: ctx}, upload, hold) }()
	}

	<-running
	<-running
	select {
	case <-running:
		t.Fatal("a third upload runs beside two")
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, slots(nil, streamCalled{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: proto.Store_ReadFile_FullMethodName}, func(any, grpc.ServerStream) error { return nil }),
		"other streams do not wait")

	gone, cancel := context.WithCancel(ctx)
	cancel()
	require.Equal(t, codes.Canceled, status.Code(slots(nil, streamCalled{ctx: gone}, upload, hold)), "a waiting call ends with its context")

	release <- struct{}{}
	<-running
	close(release)

	for range 3 {
		require.NoError(t, <-done)
	}
}

func TestServeRefusesAnAdminKeyThatIsTheBackupKey(t *testing.T) {
	repo := &Repo{}

	require.Error(t, repo.Serve(context.Background(), nil, ServeOptions{Secret: "s3cret", AdminKey: "s3cret", PlaintextBehindProxy: true}))
}

func TestAnAgentKeepsThePresenceFiltersItFetchedInItsCacheDirectory(t *testing.T) {
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

	agent, err := Open(ctx, Settings{Storage: "goback+insecure://s3cret@" + listener.Addr().String(), AgentID: "agent", CacheDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = agent.Close() })

	_, err = agent.Store.(backup.PresenceSource).Presence(ctx, "world")
	require.NoError(t, err)

	cached, err := os.ReadDir(agent.StoreCache("presence"))
	require.NoError(t, err)
	require.Len(t, cached, 1)

	cancel()
	require.NoError(t, <-served)
}

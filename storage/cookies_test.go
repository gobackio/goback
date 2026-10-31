package storage

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// affinityServer sets a cookie on a call that came without one, the way a
// load balancer pins a client, and records the cookies each call brought.
type affinityServer struct {
	mu   sync.Mutex
	seen [][]string
}

func (a *affinityServer) admit(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	brought := md.Get("cookie")

	a.mu.Lock()
	a.seen = append(a.seen, brought)
	a.mu.Unlock()

	if len(brought) == 0 {
		_ = grpc.SetHeader(ctx, metadata.Pairs("set-cookie", "GOOGAPPUID=instance-7; Path=/; HttpOnly"))
	}
}

func TestTheAgentReturnsTheCookiesItWasGiven(t *testing.T) {
	a := &affinityServer{}
	listener := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			a.admit(ctx)
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			a.admit(ss.Context())
			return handler(srv, ss)
		}),
	)
	healthpb.RegisterHealthServer(srv, health.NewServer())

	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)

	jar := newCookies("dns:///store.example.com:443", false)
	con, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithUnaryInterceptor(jar.unary()),
		grpc.WithStreamInterceptor(jar.stream()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = con.Close() })

	client := healthpb.NewHealthClient(con)
	ctx := context.Background()

	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)

	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)

	a.mu.Lock()
	defer a.mu.Unlock()
	require.Equal(t, [][]string{nil, {"GOOGAPPUID=instance-7"}, {"GOOGAPPUID=instance-7"}}, a.seen)
}

func TestACookieFromOneMethodGoesWithEveryMethod(t *testing.T) {
	jar := newCookies("store.example.com:443", true)
	jar.keep("/goback.Store/ReadParts", metadata.Pairs("set-cookie", "GOOGAPPUID=instance-2; Path=/; Secure"))

	md, _ := metadata.FromOutgoingContext(jar.attach(context.Background(), "/goback.Store/Put"))
	require.Equal(t, []string{"GOOGAPPUID=instance-2"}, md.Get("cookie"))
}

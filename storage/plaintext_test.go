package storage

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// servePlaintext runs a store server with no transport security at all,
// the way a local deployment behind nothing does, and returns its address.
func servePlaintext(t *testing.T, index backup.Index) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	remote := NewServer(NewStore(index, nil))
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(auth.UnaryInterceptor(testSecret), remote.UnaryInterceptor()),
		grpc.ChainStreamInterceptor(auth.StreamInterceptor(testSecret), remote.StreamInterceptor()),
	)
	proto.RegisterStoreServer(srv, remote)

	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)

	return listener.Addr().String()
}

func TestAPlaintextClientReachesAPlaintextServer(t *testing.T) {
	ctx := context.Background()
	addr := servePlaintext(t, newMemIndex())

	client, err := NewPlaintextClient(addr, auth.Credentials{
		Secret: testSecret, AgentID: "laptop", Plaintext: true,
	})
	require.NoError(t, err)

	blob := proto.NewObject(&proto.Blob{Data: []byte("in the clear")})
	require.NoError(t, client.Put(ctx, blob))

	file := proto.NewObject(&proto.File{Parts: []*proto.FilePart{{Length: 12, Ref: blob.Ref()}}})
	require.NoError(t, client.Put(ctx, file))

	got, err := client.Get(ctx, file.Ref())
	require.NoError(t, err)
	require.True(t, got.Ref().Equal(file.Ref()))
}

func TestPlaintextNeedsCredentialsThatAllowIt(t *testing.T) {
	_, err := NewPlaintextClient("127.0.0.1:1", auth.Credentials{Secret: testSecret})
	require.Error(t, err, "credentials that demand TLS cannot dial without it")
}

func TestCredentialsRefuseAPlaintextWireUnlessTold(t *testing.T) {
	require.True(t, auth.Credentials{Secret: "s"}.RequireTransportSecurity())
	require.False(t, auth.Credentials{Secret: "s", Plaintext: true}.RequireTransportSecurity())
}

func TestAClientWithADialerConnectsThroughIt(t *testing.T) {
	ctx := context.Background()
	addr := servePlaintext(t, newMemIndex())

	var dialed atomic.Int32

	client, err := NewPlaintextClient(addr, auth.Credentials{Secret: testSecret, AgentID: "laptop", Plaintext: true},
		WithDialer(func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialed.Add(1)
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}))
	require.NoError(t, err)

	require.NoError(t, client.Put(ctx, proto.NewObject(&proto.Blob{Data: []byte("dialed")})))
	require.Positive(t, dialed.Load())
}

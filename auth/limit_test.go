package auth

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func callFrom(addr, key string, forwarded ...string) context.Context {
	md := metadata.Pairs(secretHeader, "Bearer "+key, AgentHeader, "node-7")
	for _, hop := range forwarded {
		md.Append("x-forwarded-for", hop)
	}

	ctx := metadata.NewIncomingContext(context.Background(), md)

	return peer.NewContext(ctx, &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP(addr), Port: 40000}})
}

func TestAnAddressThatFailsTooOftenIsRefusedUntilItsAllowanceReturns(t *testing.T) {
	now := time.Unix(1000, 0)
	limit := FailureLimit{Burst: 3, Every: time.Minute}
	l := newLimiter(limit)
	l.now = func() time.Time { return now }
	authenticate := l.wrap(SharedSecret("s3cret"))

	for range limit.Burst {
		_, err := authenticate(callFrom("192.0.2.1", "guess"))
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}

	_, err := authenticate(callFrom("192.0.2.1", "s3cret"))
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "even the right key is not checked")

	_, err = authenticate(callFrom("192.0.2.2", "s3cret"))
	require.NoError(t, err, "another address is not held back")

	now = now.Add(limit.Every)
	_, err = authenticate(callFrom("192.0.2.1", "s3cret"))
	require.NoError(t, err)
}

func TestBehindAProxyTheForwardedAddressIsLimited(t *testing.T) {
	limit := FailureLimit{Burst: 1, Every: time.Hour, Forwarded: true}
	authenticate := LimitFailures(SharedSecret("s3cret"), limit)

	_, err := authenticate(callFrom("10.0.0.1", "guess", "203.0.113.9, 198.51.100.7"))
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	_, err = authenticate(callFrom("10.0.0.1", "s3cret", "198.51.100.7"))
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	_, err = authenticate(callFrom("10.0.0.1", "s3cret", "198.51.100.8"))
	require.NoError(t, err, "the proxy's other clients are not held back")
}

func TestTheLimiterForgetsAddressesBeyondItsBound(t *testing.T) {
	l := newLimiter(FailureLimit{Burst: 1, Every: time.Hour})
	authenticate := l.wrap(SharedSecret("s3cret"))

	for i := range limitedPeers + 10 {
		_, _ = authenticate(callFrom(net.IPv4(10, byte(i>>16), byte(i>>8), byte(i)).String(), "guess"))
	}

	require.LessOrEqual(t, len(l.peers), limitedPeers)
}

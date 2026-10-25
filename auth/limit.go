package auth

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// FailureLimit is how often a peer address may fail to authenticate:
// Burst times at once, then once every Every.
type FailureLimit struct {
	Burst int
	Every time.Duration
	// Forwarded names the peer by the last x-forwarded-for entry a call
	// carries, which is the one a proxy in front appended, rather than by
	// the proxy's own address.
	Forwarded bool
}

// limitedPeers bounds the addresses a FailureLimit tracks at once.
const limitedPeers = 4096

// LimitFailures refuses the calls of a peer address that has used up its
// failures with ResourceExhausted, without checking what key they carry,
// until the limit lets it try again. A failure is a call authenticate
// answers Unauthenticated.
func LimitFailures(authenticate Authenticator, limit FailureLimit) Authenticator {
	return newLimiter(limit).wrap(authenticate)
}

func newLimiter(limit FailureLimit) *limiter {
	return &limiter{limit: limit, peers: make(map[string]*allowance), now: time.Now}
}

func (l *limiter) wrap(authenticate Authenticator) Authenticator {
	return func(ctx context.Context) (context.Context, error) {
		addr := l.address(ctx)
		if !l.allows(addr) {
			return nil, status.Error(codes.ResourceExhausted, "too many failed keys from this address; try again later")
		}

		authenticated, err := authenticate(ctx)
		if status.Code(err) == codes.Unauthenticated {
			l.failed(addr)
		}

		return authenticated, err
	}
}

type limiter struct {
	limit FailureLimit
	now   func() time.Time

	mtx   sync.Mutex
	peers map[string]*allowance
}

// allowance is the failures a peer has left as of at.
type allowance struct {
	left float64
	at   time.Time
}

func (l *limiter) address(ctx context.Context) string {
	if l.limit.Forwarded {
		md, _ := metadata.FromIncomingContext(ctx)
		if forwarded := md.Get("x-forwarded-for"); len(forwarded) > 0 {
			hops := strings.Split(forwarded[len(forwarded)-1], ",")
			if last := strings.TrimSpace(hops[len(hops)-1]); last != "" {
				return last
			}
		}
	}

	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return ""
	}

	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}

	return host
}

// refill brings a's failures up to date; l.mtx is held.
func (l *limiter) refill(a *allowance, now time.Time) {
	if l.limit.Every > 0 {
		a.left += float64(now.Sub(a.at)) / float64(l.limit.Every)
	}

	a.left = min(a.left, float64(l.limit.Burst))
	a.at = now
}

func (l *limiter) allows(addr string) bool {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	a, ok := l.peers[addr]
	if !ok {
		return true
	}

	l.refill(a, l.now())

	return a.left >= 1
}

func (l *limiter) failed(addr string) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	now := l.now()

	a, ok := l.peers[addr]
	if !ok {
		l.makeRoom(now)

		a = &allowance{left: float64(l.limit.Burst), at: now}
		l.peers[addr] = a
	}

	l.refill(a, now)
	a.left = max(a.left-1, 0)
}

// makeRoom forgets the peers whose failures are all back, and an
// arbitrary one when that frees nothing; l.mtx is held.
func (l *limiter) makeRoom(now time.Time) {
	if len(l.peers) < limitedPeers {
		return
	}

	for addr, a := range l.peers {
		if l.refill(a, now); a.left >= float64(l.limit.Burst) {
			delete(l.peers, addr)
		}
	}

	for addr := range l.peers {
		if len(l.peers) < limitedPeers {
			return
		}

		delete(l.peers, addr)
	}
}

// Package auth carries the caller's identity through a request: the key
// it presented, the backup key every agent of a server shares or the
// admin key, and the agent id each agent declares.
package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"github.com/gobackio/goback/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Principal is who is calling: the agent id the caller declared, and
// whether it presented the admin key.
type Principal struct {
	AgentID string
	Admin   bool
	// Bounded marks a caller that may only add to what the store keeps:
	// it escrows the store key only where no copy of it is kept, and
	// changes a set's retention only to a policy that keeps at least as
	// much.
	Bounded bool
}

// ErrUnauthenticated is returned when a request carries no principal.
var ErrUnauthenticated = errors.New("no caller identity")

// ErrForbidden is returned when the principal may not do what it asked.
var ErrForbidden = errors.New("not allowed for this caller")

type principalKey struct{}

// WithPrincipal attaches the principal to the context.
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the request's principal, if any.
func FromContext(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(*Principal)
	return p, ok && p != nil
}

// Require returns the principal or ErrUnauthenticated.
func Require(ctx context.Context) (*Principal, error) {
	p, ok := FromContext(ctx)
	if !ok {
		return nil, ErrUnauthenticated
	}

	return p, nil
}

// AuthorizeCommit checks that a commit names the principal's agent.
func (p *Principal) AuthorizeCommit(c *proto.Commit) error {
	if p.AgentID != "" && c.GetAgentId() != p.AgentID {
		return fmt.Errorf("%w: commit is by agent %q", ErrForbidden, c.GetAgentId())
	}

	return nil
}

const (
	secretHeader = "authorization"
	// AgentHeader carries the agent id the caller declares.
	AgentHeader = "goback-agent"
)

// Bearer returns the bearer token of the incoming call, if any.
func Bearer(ctx context.Context) (string, bool) {
	md, _ := metadata.FromIncomingContext(ctx)
	presented := md.Get(secretHeader)
	if len(presented) == 0 {
		return "", false
	}

	return strings.TrimPrefix(presented[0], "Bearer "), true
}

// Agent returns the agent id the incoming call declares, if any.
func Agent(ctx context.Context) (string, bool) {
	md, _ := metadata.FromIncomingContext(ctx)
	agents := md.Get(AgentHeader)
	if len(agents) == 0 || agents[0] == "" {
		return "", false
	}

	return agents[0], true
}

// Authenticator decides who an incoming call is from and returns the
// context to handle it with, or a status error refusing it.
type Authenticator func(ctx context.Context) (context.Context, error)

// SharedSecret authenticates callers presenting secret as bearer token
// and attaches the agent they declare as the principal.
func SharedSecret(secret string) Authenticator {
	return Keys(secret, "")
}

// Keys authenticates callers presenting the backup or the admin key as
// bearer token and attaches the agent they declare as the principal, an
// Admin one for the admin key. An empty admin key admits no admin.
func Keys(backup, admin string) Authenticator {
	return func(ctx context.Context) (context.Context, error) {
		presented, ok := Bearer(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing secret")
		}

		isAdmin := admin != "" && equalSecrets(presented, admin)
		if !isAdmin && !equalSecrets(presented, backup) {
			return nil, status.Error(codes.Unauthenticated, "wrong secret")
		}

		agent, ok := Agent(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing agent id")
		}

		return WithPrincipal(ctx, &Principal{AgentID: agent, Admin: isAdmin}), nil
	}
}

// Credentials sends the shared secret and the agent id with every call and
// refuses to do so over a plaintext connection unless Plaintext says
// otherwise.
type Credentials struct {
	Secret  string
	AgentID string
	// Plaintext allows the secret onto a connection nothing encrypts,
	// where anyone on the path can read it and everything it uploads.
	// It is for a store on the same machine, nothing further.
	Plaintext bool
}

// GetRequestMetadata implements credentials.PerRPCCredentials.
func (c Credentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{secretHeader: "Bearer " + c.Secret, AgentHeader: c.AgentID}, nil
}

// RequireTransportSecurity implements credentials.PerRPCCredentials.
func (c Credentials) RequireTransportSecurity() bool { return !c.Plaintext }

func equalSecrets(a, b string) bool {
	x, y := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(x[:], y[:]) == 1
}

// UnaryInterceptor rejects calls that do not present secret and attaches
// the declared agent as the principal of the ones that do.
func UnaryInterceptor(secret string) grpc.UnaryServerInterceptor {
	return Unary(SharedSecret(secret))
}

// StreamInterceptor is UnaryInterceptor for streaming calls.
func StreamInterceptor(secret string) grpc.StreamServerInterceptor {
	return Stream(SharedSecret(secret))
}

// Unary runs authenticate before every unary call.
func Unary(authenticate Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, err := authenticate(ctx)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// Stream runs authenticate before every streaming call.
func Stream(authenticate Authenticator) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(ss.Context())
		if err != nil {
			return err
		}

		return handler(srv, &stream{ServerStream: ss, ctx: ctx})
	}
}

type stream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *stream) Context() context.Context { return s.ctx }

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// sessionHeader carries the session id on every call of a session.
const sessionHeader = "goback-session"

var _ backup.SessionStore = (*Client)(nil)

// outgoing adds the session header when the context carries a session.
func (r *Client) outgoing(ctx context.Context) context.Context {
	s, ok := backup.SessionFromContext(ctx)
	if !ok {
		return ctx
	}

	return metadata.AppendToOutgoingContext(ctx, sessionHeader, s.ID)
}

// BeginSession opens a session on the server and returns a context whose
// calls belong to it.
func (r *Client) BeginSession(ctx context.Context, s *backup.Session) (context.Context, error) {
	resp, err := r.store.BeginSession(ctx, &proto.BeginSessionRequest{
		BackupSet:        s.Set,
		Restore:          s.Restore,
		InitialRetention: m.RetentionPolicy(s.InitialRetention),
	})
	if err != nil {
		return nil, err
	}

	s.ID = resp.SessionId
	s.Started = time.Now()

	return backup.WithSession(ctx, s), nil
}

// EndSession drops what the context's session has not committed; a
// session the store no longer has, such as one its commit ended, is
// backup.ErrNoSession.
func (r *Client) EndSession(ctx context.Context) error {
	s, ok := backup.SessionFromContext(ctx)
	if !ok {
		return nil
	}

	_, err := r.store.EndSession(ctx, &proto.EndSessionRequest{SessionId: s.ID})
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("session %s: %w", s.ID, backup.ErrNoSession)
	}

	return err
}

// BeginSession implements proto.StoreServer.
func (r *Server) BeginSession(ctx context.Context, request *proto.BeginSessionRequest) (*proto.BeginSessionResponse, error) {
	session, err := r.store.BeginSession(ctx, request.BackupSet, request.Restore, m.FromRetentionPolicy(request.InitialRetention))
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.BeginSessionResponse{SessionId: session.ID, LeaseSeconds: int64(r.store.Lease / time.Second)}, nil
}

// EndSession implements proto.StoreServer.
func (r *Server) EndSession(ctx context.Context, request *proto.EndSessionRequest) (*proto.EndSessionResponse, error) {
	err := r.store.EndSession(ctx, request.SessionId)
	if err != nil {
		return nil, ToStatus(err)
	}

	return &proto.EndSessionResponse{}, nil
}

// withSession attaches the session named by the request metadata, if any.
func (r *Server) withSession(ctx context.Context) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	ids := md.Get(sessionHeader)
	if len(ids) == 0 {
		return ctx, nil
	}

	session, err := r.store.Session(ctx, ids[0])
	if err != nil {
		return nil, ToStatus(err)
	}

	return backup.WithSession(ctx, session), nil
}

// UnaryInterceptor resolves the session header; it runs after
// authentication.
func (r *Server) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, err := r.admit(ctx)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamInterceptor is UnaryInterceptor for streaming calls.
func (r *Server) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := r.admit(ss.Context())
		if err != nil {
			return err
		}

		return handler(srv, &sessionStream{ServerStream: ss, ctx: ctx})
	}
}

func (r *Server) admit(ctx context.Context) (context.Context, error) {
	if _, err := auth.Require(ctx); err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	return r.withSession(ctx)
}

type sessionStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *sessionStream) Context() context.Context { return s.ctx }

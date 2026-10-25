// Package admin implements the Admin service of a store server: sets, the
// store policy, retention, escrow and job triggers. The server serves it
// beside the Store service.
package admin

import (
	"context"
	"errors"
	"time"

	"github.com/gobackio/goback/admin/mapping/gen"
	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql"
	pb "github.com/gobackio/goback/proto/admin"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// m maps what the index reports to the admin protos.
var m = gen.MapperImpl{}

// Index is what the admin surface needs from a store's index.
type Index interface {
	ListSets(ctx context.Context) ([]index.SetInfo, error)
	DeleteSet(ctx context.Context, name string, erase bool) error
	UndeleteSet(ctx context.Context, name string) error
	GetStorePolicy(ctx context.Context) (index.StorePolicy, error)
	SetStorePolicy(ctx context.Context, policy storekey.Policy, acknowledge bool, now time.Time) (index.StorePolicy, error)
	GetPolicy(ctx context.Context, set string) (index.SetRetention, error)
	SetPolicy(ctx context.Context, set string, p *retention.Policy) error
	GetDefaultPolicy(ctx context.Context) (retention.Policy, bool, error)
	SetDefaultPolicy(ctx context.Context, p *retention.Policy) error
	Windows(ctx context.Context) (index.Windows, error)
	SetWindows(ctx context.Context, w index.Windows) error
}

// Server implements the Admin service over a store index.
type Server struct {
	pb.UnimplementedAdminServer

	Index Index
	// RetireJob and CollectJob run the retention and garbage collection
	// jobs; nil means the job is not available.
	RetireJob  func(ctx context.Context) (int, error)
	CollectJob func(ctx context.Context) (string, error)
	// Escrow keeps the escrowed store key; nil means the store keeps none.
	Escrow backup.KeyEscrow
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

var _ pb.AdminServer = (*Server)(nil)
var _ Index = (*sql.Index)(nil)

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}

	return time.Now()
}

// ListSets implements pb.AdminServer.
func (s *Server) ListSets(ctx context.Context, request *pb.ListSetsRequest) (*pb.ListSetsResponse, error) {
	sets, err := s.Index.ListSets(ctx)
	if err != nil {
		return nil, Status(err)
	}

	resp := &pb.ListSetsResponse{}
	for _, set := range sets {
		resp.Sets = append(resp.Sets, m.Set(set))
	}

	return resp, nil
}

// DeleteSet implements pb.AdminServer.
func (s *Server) DeleteSet(ctx context.Context, request *pb.DeleteSetRequest) (*pb.DeleteSetResponse, error) {
	err := s.Index.DeleteSet(ctx, request.Name, request.Erase)
	if err != nil {
		return nil, Status(err)
	}

	return &pb.DeleteSetResponse{}, nil
}

// UndeleteSet implements pb.AdminServer.
func (s *Server) UndeleteSet(ctx context.Context, request *pb.UndeleteSetRequest) (*pb.UndeleteSetResponse, error) {
	err := s.Index.UndeleteSet(ctx, request.Name)
	if err != nil {
		return nil, Status(err)
	}

	return &pb.UndeleteSetResponse{}, nil
}

// GetStorePolicy implements pb.AdminServer.
func (s *Server) GetStorePolicy(ctx context.Context, _ *pb.GetStorePolicyRequest) (*pb.StorePolicy, error) {
	p, err := s.Index.GetStorePolicy(ctx)
	if err != nil {
		return nil, Status(err)
	}

	return m.Policy(p), nil
}

// SetStorePolicy implements pb.AdminServer.
func (s *Server) SetStorePolicy(ctx context.Context, request *pb.SetStorePolicyRequest) (*pb.StorePolicy, error) {
	policy := m.WritePolicy(request)

	switch policy.Mode {
	case storekey.ModeSealed, storekey.ModeNone:
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown policy mode %q", request.Mode)
	}

	if _, err := backup.ParsePresenceScope(policy.PresenceScope); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p, err := s.Index.SetStorePolicy(ctx, policy, request.AcknowledgeKey, s.now())
	if err != nil {
		return nil, Status(err)
	}

	return m.Policy(p), nil
}

// PutEscrowedKey implements pb.AdminServer.
func (s *Server) PutEscrowedKey(ctx context.Context, request *pb.PutEscrowedKeyRequest) (*pb.PutEscrowedKeyResponse, error) {
	if s.Escrow == nil {
		return nil, status.Error(codes.Unimplemented, "this store keeps no escrowed key")
	}

	err := s.Escrow.PutEscrowedKey(ctx, backup.EscrowedKey{KeyID: request.KeyId, Escrowed: []byte(request.Escrowed)})
	if err != nil {
		return nil, Status(err)
	}

	return &pb.PutEscrowedKeyResponse{}, nil
}

// GetRetention implements pb.AdminServer.
func (s *Server) GetRetention(ctx context.Context, request *pb.GetRetentionRequest) (*pb.Retention, error) {
	return s.retention(ctx, request.Set)
}

// SetRetention implements pb.AdminServer.
func (s *Server) SetRetention(ctx context.Context, request *pb.SetRetentionRequest) (*pb.Retention, error) {
	var policy *retention.Policy
	if request.Policy != nil {
		p := m.FromRetention(request.Policy)
		policy = &p
	}

	switch {
	case request.Set != "" && (policy != nil || request.Inherit):
		if err := s.Index.SetPolicy(ctx, request.Set, policy); err != nil {
			return nil, Status(err)
		}
	case request.Set == "" && (policy != nil || request.Inherit):
		if err := s.Index.SetDefaultPolicy(ctx, policy); err != nil {
			return nil, Status(err)
		}
	}

	if request.Set == "" && request.TrashDays != nil {
		if err := s.Index.SetWindows(ctx, index.Windows{TrashDays: int(*request.TrashDays)}); err != nil {
			return nil, Status(err)
		}
	}

	return s.retention(ctx, request.Set)
}

func (s *Server) retention(ctx context.Context, set string) (*pb.Retention, error) {
	w, err := s.Index.Windows(ctx)
	if err != nil {
		return nil, Status(err)
	}

	resp := &pb.Retention{Set: set, TrashDays: int32(w.TrashDays)}

	if set == "" {
		policy, stored, err := s.Index.GetDefaultPolicy(ctx)
		if err != nil {
			return nil, Status(err)
		}

		resp.Effective = m.Retention(policy)
		if stored {
			resp.Policy = resp.Effective
		}

		return resp, nil
	}

	ret, err := s.Index.GetPolicy(ctx, set)
	if err != nil {
		return nil, Status(err)
	}

	resp.Effective = m.Retention(ret.Effective)
	resp.Paused = ret.Paused
	if ret.Policy != nil {
		resp.Policy = m.Retention(*ret.Policy)
	}

	return resp, nil
}

// Retire implements pb.AdminServer.
func (s *Server) Retire(ctx context.Context, _ *pb.RetireRequest) (*pb.RetireResponse, error) {
	if s.RetireJob == nil {
		return nil, status.Error(codes.Unimplemented, "this index keeps no retention state")
	}

	n, err := s.RetireJob(ctx)
	if err != nil {
		return nil, Status(err)
	}

	return &pb.RetireResponse{Retired: int64(n)}, nil
}

// CollectGarbage implements pb.AdminServer.
func (s *Server) CollectGarbage(ctx context.Context, _ *pb.CollectGarbageRequest) (*pb.CollectGarbageResponse, error) {
	if s.CollectJob == nil {
		return nil, status.Error(codes.Unimplemented, "this store cannot garbage collect itself")
	}

	report, err := s.CollectJob(ctx)
	if err != nil {
		return nil, Status(err)
	}

	return &pb.CollectGarbageResponse{Report: report}, nil
}

// Status maps the index's sentinel errors to gRPC codes; an error that
// already is a status passes through.
func Status(err error) error {
	switch {
	case errors.Is(err, backup.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, backup.ErrTombstoned), errors.Is(err, backup.ErrSetClosed),
		errors.Is(err, backup.ErrOtherKeyEscrowed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, auth.ErrForbidden):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, backup.ErrNotImplemented):
		return status.Error(codes.Unimplemented, err.Error())
	case errors.Is(err, retention.ErrInvalidPolicy), errors.Is(err, backup.ErrInvalidEscrow):
		return status.Error(codes.InvalidArgument, err.Error())
	}

	if _, ok := status.FromError(err); ok {
		return err
	}

	return status.Error(codes.Internal, err.Error())
}

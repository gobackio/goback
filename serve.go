package goback

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path"
	"runtime/debug"
	"time"

	"github.com/gobackio/goback/admin"
	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/health"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	adminpb "github.com/gobackio/goback/proto/admin"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/storage/pack"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// serveStreams is how many calls one connection to a store server has
// open at once; a client past it waits.
const serveStreams = 1024

// serveKeepalive closes connections without calls for a quarter hour, and
// after an hour asks clients to dial again for their next calls, leaving
// the calls running to finish however long they take. A client that stops
// answering pings is cut off.
var serveKeepalive = keepalive.ServerParameters{
	MaxConnectionIdle: 15 * time.Minute,
	MaxConnectionAge:  time.Hour,
	Time:              2 * time.Minute,
	Timeout:           20 * time.Second,
}

// serveFailures and serveFailureEvery are how often one address may
// present a wrong key: ten times at once, then once every six seconds.
const (
	serveFailures     = 10
	serveFailureEvery = 6 * time.Second
)

// serveUploads is how many SendUpload streams a store server ingests at
// once, each holding up to a few dozen MiB; one past them waits.
const serveUploads = 16

// ServeOptions say how Serve serves a store.
type ServeOptions struct {
	// Secret is the backup key every agent presents, which backs up, reads
	// and pins, escrows the store key where no copy of it is kept and
	// lengthens a set's retention, and nothing else; Serve refuses an
	// empty one.
	Secret string
	// AdminKey is what an operator presents for every other call: deleting
	// commits and sets, shortening retention or changing the store policy,
	// retiring, collecting and escrowing another copy of the store key.
	// Empty refuses those calls.
	AdminKey string
	// TLS is what the server presents to agents. Without it Serve listens
	// in plaintext only when PlaintextBehindProxy says a proxy in front
	// terminates TLS.
	TLS                  *tls.Config
	PlaintextBehindProxy bool
	// Schedule is how often each maintenance job runs, as is: a zero
	// interval never runs its job. maintenance.DefaultSchedule is what
	// goback server runs by default.
	Schedule maintenance.Schedule
}

// Serve serves the repo to agents over gRPC on listener, as goback server
// does: the store API, the Admin service when the index keeps sets and
// policy, with the calls that delete or could keep less open to the admin
// key alone, health checks, whose readiness fails while the index does not
// answer or the server drains, and the maintenance jobs of opts.Schedule.
// An address that presents wrong keys too often is refused for a while.
// It returns once ctx ends, after the calls in flight finish, or when the
// listener fails, and in either case once the maintenance job under way
// has stopped, so the repo may be closed then.
func (r *Repo) Serve(ctx context.Context, listener net.Listener, opts ServeOptions) error {
	if opts.Secret == "" {
		return errors.New("no secret: a store server accepts no anonymous calls")
	}

	if opts.AdminKey == opts.Secret {
		return errors.New("the admin key is the backup key: every agent could then delete what the others backed up")
	}

	var creds credentials.TransportCredentials

	switch {
	case opts.TLS != nil && opts.PlaintextBehindProxy:
		return errors.New("TLS and plaintext behind a proxy are exclusive")
	case opts.TLS != nil:
		creds = credentials.NewTLS(opts.TLS)
	case opts.PlaintextBehindProxy:
		creds = insecure.NewCredentials()
	default:
		return errors.New("no TLS: a store server serves plaintext only behind a proxy that terminates TLS")
	}

	logger := r.settings.logger()

	if opts.TLS == nil {
		logger.Warn("serving plaintext; a proxy must terminate TLS in front of this listener")
	}

	sessions, _ := r.Store.(backup.SessionStore)
	if sessions == nil {
		logger.Warn("the store has no sessions; uploads are visible as they arrive", "store", fmt.Sprintf("%T", r.Store))
	}

	if _, ok := r.Index.(backup.PresenceIndex); !ok {
		logger.Warn("the index stores no presence filters; agents upload every new chunk", "index", fmt.Sprintf("%T", r.Index))
	}

	if _, ok := r.Index.(backup.RefScope); !ok {
		logger.Warn("the index cannot scope reads; every ref is served", "index", fmt.Sprintf("%T", r.Index))
	}

	remote := storage.NewServer(storage.NewStore(r.Index, sessions))
	keys := auth.LimitFailures(auth.Keys(opts.Secret, opts.AdminKey), auth.FailureLimit{
		Burst:     serveFailures,
		Every:     serveFailureEvery,
		Forwarded: opts.PlaintextBehindProxy,
	})

	srv := grpc.NewServer(
		grpc.Creds(creds),
		grpc.MaxConcurrentStreams(serveStreams),
		grpc.KeepaliveParams(serveKeepalive),
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithPublicEndpoint())),
		grpc.ChainUnaryInterceptor(recoverUnary(logger), health.Unary(auth.Unary(keys), gateUnary, remote.UnaryInterceptor())),
		grpc.ChainStreamInterceptor(recoverStream(logger), health.Stream(auth.Stream(keys), gateStream, uploadSlots(serveUploads), remote.StreamInterceptor())),
	)

	proto.RegisterStoreServer(srv, remote)

	probe := &health.Probe{}
	if index, ok := r.Index.(interface{ Ping(context.Context) error }); ok {
		probe.Add("index", index.Ping)
	}

	probe.Register(srv)

	runner := maintenance.NewRunner(r.Store, r.Index, opts.Schedule)
	runner.Logger = logger
	runner.OnCollect = func(report *pack.CollectReport) {
		logger.Info("garbage collected", "report", report.Summary())
	}

	if x, ok := r.Index.(*sql.Index); ok {
		adminpb.RegisterAdminServer(srv, adminServer(x, runner))
	} else {
		logger.Warn("the index keeps no sets or policy; serving no Admin service", "index", fmt.Sprintf("%T", r.Index))
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ran := make(chan struct{})

	go func() {
		defer close(ran)
		runner.Run(ctx)
	}()

	defer func() {
		cancel()
		<-ran
	}()

	stopped := context.AfterFunc(ctx, func() {
		probe.Drain()
		srv.GracefulStop()
	})
	defer stopped()

	logger.Info("listening", "address", listener.Addr().String())

	err := srv.Serve(listener)
	if ctx.Err() != nil {
		return nil
	}

	if stopped() {
		srv.Stop()
	}

	return err
}

// backupCalls are the calls the backup key makes: backing up, reading,
// pinning and reporting the agent's schedule. Every other call takes the
// admin key.
var backupCalls = map[string]bool{
	proto.Store_Put_FullMethodName:            true,
	proto.Store_PutCommits_FullMethodName:     true,
	proto.Store_Missing_FullMethodName:        true,
	proto.Store_PutObjects_FullMethodName:     true,
	proto.Store_BeginUpload_FullMethodName:    true,
	proto.Store_SendUpload_FullMethodName:     true,
	proto.Store_UploadStatus_FullMethodName:   true,
	proto.Store_Get_FullMethodName:            true,
	proto.Store_ReadFile_FullMethodName:       true,
	proto.Store_ReadFiles_FullMethodName:      true,
	proto.Store_FileInfo_FullMethodName:       true,
	proto.Store_ReadDir_FullMethodName:        true,
	proto.Store_CommitInfo_FullMethodName:     true,
	proto.Store_LatestCommit_FullMethodName:   true,
	proto.Store_GetTree_FullMethodName:        true,
	proto.Store_BeginCommit_FullMethodName:    true,
	proto.Store_BeginSession_FullMethodName:   true,
	proto.Store_EndSession_FullMethodName:     true,
	proto.Store_TouchSession_FullMethodName:   true,
	proto.Store_GetPresence_FullMethodName:    true,
	proto.Store_ListTrash_FullMethodName:      true,
	proto.Store_CountCommits_FullMethodName:   true,
	proto.Store_GetRetention_FullMethodName:   true,
	proto.Store_ListPins_FullMethodName:       true,
	proto.Store_EscrowedKeys_FullMethodName:   true,
	proto.Store_ReportSchedule_FullMethodName: true,

	adminpb.Admin_ListSets_FullMethodName:       true,
	adminpb.Admin_GetStorePolicy_FullMethodName: true,
	adminpb.Admin_GetRetention_FullMethodName:   true,
}

// boundedCalls are the calls the backup key makes as an auth.Principal
// that is Bounded: escrowing the store key where no copy of it is kept, and
// changing a set's retention to one that keeps at least as much.
var boundedCalls = map[string]bool{
	adminpb.Admin_PutEscrowedKey_FullMethodName: true,
	proto.Store_SetRetention_FullMethodName:     true,
}

// admit refuses a call the caller's key does not make, and returns the
// context to handle the others with.
func admit(ctx context.Context, method string) (context.Context, error) {
	p, err := auth.Require(ctx)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	switch {
	case p.Admin || backupCalls[method]:
		return ctx, nil
	case boundedCalls[method]:
		bounded := *p
		bounded.Bounded = true

		return auth.WithPrincipal(ctx, &bounded), nil
	}

	return nil, status.Errorf(codes.PermissionDenied, "%s takes the server's admin key; the backup key only backs up, reads and pins", path.Base(method))
}

func gateUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	ctx, err := admit(ctx, info.FullMethod)
	if err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func gateStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx, err := admit(ss.Context(), info.FullMethod)
	if err != nil {
		return err
	}

	return handler(srv, admitted{ServerStream: ss, ctx: ctx})
}

// admitted is a stream handled with the context admit returned.
type admitted struct {
	grpc.ServerStream
	ctx context.Context
}

func (s admitted) Context() context.Context { return s.ctx }

// uploadSlots has at most n SendUpload streams run at once; the others wait
// for one of them to end, or for their own call to.
func uploadSlots(n int64) grpc.StreamServerInterceptor {
	slots := semaphore.NewWeighted(n)

	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod != proto.Store_SendUpload_FullMethodName {
			return handler(srv, ss)
		}

		if err := slots.Acquire(ss.Context(), 1); err != nil {
			return status.FromContextError(err).Err()
		}
		defer slots.Release(1)

		return handler(srv, ss)
	}
}

// recoverUnary answers a call whose handler panics with codes.Internal and
// logs the stack, so the server goes on serving the other calls.
func recoverUnary(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if p := recover(); p != nil {
				err = panicked(logger, info.FullMethod, p)
			}
		}()

		return handler(ctx, req)
	}
}

// recoverStream is recoverUnary for streaming calls.
func recoverStream(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if p := recover(); p != nil {
				err = panicked(logger, info.FullMethod, p)
			}
		}()

		return handler(srv, ss)
	}
}

func panicked(logger *slog.Logger, method string, p any) error {
	logger.Error("a call panicked", "method", method, "panic", fmt.Sprint(p), "stack", string(debug.Stack()))

	return status.Error(codes.Internal, "the server failed handling the call")
}

var (
	_ storage.PolicyLengthener = (*sql.Index)(nil)
	_ backup.FirstEscrow       = (*sql.Index)(nil)
)

// adminServer is the Admin service over the index and the runner's jobs.
func adminServer(x *sql.Index, runner *maintenance.Runner) *admin.Server {
	server := &admin.Server{Index: x, Escrow: x}

	if runner.Retirer != nil {
		server.RetireJob = runner.Retire
	}

	if runner.Collector != nil {
		server.CollectJob = func(ctx context.Context) (string, error) {
			report, err := runner.Collect(ctx)
			if report == nil {
				return "", err
			}

			return report.Summary(), err
		}
	}

	return server
}

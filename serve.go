package goback

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// ServeOptions say how Serve serves a store.
type ServeOptions struct {
	// Secret is what every agent presents; Serve refuses an empty one.
	Secret string
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
// policy, health checks, and the maintenance jobs of opts.Schedule. It
// returns once ctx ends, after the calls in flight finish, or when the
// listener fails.
func (r *Repo) Serve(ctx context.Context, listener net.Listener, opts ServeOptions) error {
	if opts.Secret == "" {
		return errors.New("no secret: a store server accepts no anonymous calls")
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

	srv := grpc.NewServer(
		grpc.Creds(creds),
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithPublicEndpoint())),
		grpc.ChainUnaryInterceptor(health.Unary(auth.UnaryInterceptor(opts.Secret), remote.UnaryInterceptor())),
		grpc.ChainStreamInterceptor(health.Stream(auth.StreamInterceptor(opts.Secret), remote.StreamInterceptor())),
	)

	proto.RegisterStoreServer(srv, remote)
	(&health.Probe{}).Register(srv)

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

	go runner.Run(ctx)

	stopped := context.AfterFunc(ctx, srv.GracefulStop)
	defer stopped()

	logger.Info("listening", "address", listener.Addr().String())

	err := srv.Serve(listener)
	if ctx.Err() != nil {
		return nil
	}

	return err
}

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

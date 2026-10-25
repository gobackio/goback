package goback

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
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
// policy, health checks, whose readiness fails while the index does not
// answer or the server drains, and the maintenance jobs of opts.Schedule.
// It returns once ctx ends, after the calls in flight finish, or when the
// listener fails, and in either case once the maintenance job under way
// has stopped, so the repo may be closed then.
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
		grpc.MaxConcurrentStreams(serveStreams),
		grpc.KeepaliveParams(serveKeepalive),
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithPublicEndpoint())),
		grpc.ChainUnaryInterceptor(health.Unary(auth.UnaryInterceptor(opts.Secret), remote.UnaryInterceptor())),
		grpc.ChainStreamInterceptor(health.Stream(auth.StreamInterceptor(opts.Secret), remote.StreamInterceptor())),
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

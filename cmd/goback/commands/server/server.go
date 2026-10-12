package server

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"strings"

	"github.com/gobackio/goback/admin"
	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/gc"
	"github.com/gobackio/goback/health"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	adminpb "github.com/gobackio/goback/proto/admin"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/telemetry"

	"github.com/urfave/cli"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// Command is the server command.
var Command = cli.Command{
	Action: serverAction,
	Name:   "server",
	Usage:  "Serve one store to many agents over gRPC",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "address",
			Usage: "address the server listens on; :6060 serves agents on other hosts",
			Value: "127.0.0.1:6060",
		},
		cli.StringFlag{
			Name:  "secret",
			Usage: "the secret every agent of this server presents",
		},
		cli.StringFlag{
			Name:  "secret-file",
			Usage: "file holding the secret, for when a flag would show it",
		},
		cli.StringFlag{
			Name:  "tls-cert",
			Usage: "PEM certificate chain the server presents; with --tls-key, agents connect over TLS",
		},
		cli.StringFlag{
			Name:  "tls-key",
			Usage: "PEM private key for --tls-cert",
		},
		cli.BoolFlag{
			Name:  "plaintext-behind-proxy",
			Usage: "serve plaintext because a proxy in front of the server terminates TLS; never expose this listener directly",
		},
		cli.DurationFlag{
			Name:  "retire-interval",
			Usage: "how often retired commits past their window are tombstoned; 0 disables the job",
			Value: maintenance.DefaultSchedule.Retire,
		},
		cli.DurationFlag{
			Name:  "gc-interval",
			Usage: "how often the store is garbage collected; 0 disables the job",
			Value: maintenance.DefaultSchedule.Collect,
		},
		cli.DurationFlag{
			Name:  "compact-interval",
			Usage: "how often small archives are compacted; 0 disables the job",
			Value: maintenance.DefaultSchedule.Compact,
		},
		cli.DurationFlag{
			Name:  "sweep-interval",
			Usage: "how often idle archives are finalized and expired sessions ended",
			Value: maintenance.DefaultSchedule.Sweep,
		},
		cli.DurationFlag{
			Name:  "presence-interval",
			Usage: "how often presence filters are built and set sizes measured for new commits; 0 disables the job",
			Value: maintenance.DefaultSchedule.Presence,
		},
	},
}

func serverAction(ctx *cli.Context) {
	secret, err := sharedSecret(ctx)
	if err != nil {
		common.Fatal(err)
	}

	creds, tlsConfig, err := storage.ServerTLS(ctx.String("tls-cert"), ctx.String("tls-key"), ctx.Bool("plaintext-behind-proxy"))
	if err != nil {
		common.Fatal(err)
	}

	if tlsConfig == nil {
		log.Println("Serving plaintext; a proxy must terminate TLS in front of this listener")
	}

	repo := common.OpenRepo(ctx)
	s := repo.Store
	idx := repo.Index

	listener, err := net.Listen("tcp", ctx.String("address"))
	if err != nil {
		common.Fatal(err)
	}

	if _, err := telemetry.Setup(context.Background(), "goback"); err != nil {
		common.Fatalf("failed setting up telemetry: %s", err)
	}

	sessions, _ := s.(backup.SessionStore)
	if sessions == nil {
		log.Printf("Store %T has no sessions; uploads are visible as they arrive", s)
	}

	store := storage.NewStore(idx, sessions)

	if _, ok := idx.(backup.PresenceIndex); !ok {
		log.Printf("Index %T stores no presence filters; agents upload every new chunk", idx)
	}

	if _, ok := idx.(backup.RefScope); !ok {
		log.Printf("Index %T cannot scope reads; every ref is served", idx)
	}

	remote := storage.NewServer(store)

	srv := grpc.NewServer(
		grpc.Creds(creds),
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithPublicEndpoint())),
		grpc.ChainUnaryInterceptor(health.Unary(auth.UnaryInterceptor(secret), remote.UnaryInterceptor())),
		grpc.ChainStreamInterceptor(health.Stream(auth.StreamInterceptor(secret), remote.StreamInterceptor())),
	)

	proto.RegisterStoreServer(srv, remote)

	probe := &health.Probe{}
	probe.Register(srv)

	runner := maintenance.NewRunner(s, idx, maintenance.Schedule{
		Sweep:    ctx.Duration("sweep-interval"),
		Compact:  ctx.Duration("compact-interval"),
		Collect:  ctx.Duration("gc-interval"),
		Retire:   ctx.Duration("retire-interval"),
		Presence: ctx.Duration("presence-interval"),
	})
	runner.OnCollect = gc.Log
	go runner.Run(context.Background())

	if x, ok := idx.(*sql.Index); ok {
		adminpb.RegisterAdminServer(srv, adminServer(x, runner))
	} else {
		log.Printf("Index %T keeps no sets or policy; the server serves no Admin service", idx)
	}

	log.Println("Listening on", listener.Addr().String())
	common.Fatal(srv.Serve(listener))
}

// sharedSecret reads --secret or --secret-file; the server accepts no
// anonymous calls.
func sharedSecret(ctx *cli.Context) (string, error) {
	secret, file := ctx.String("secret"), ctx.String("secret-file")

	switch {
	case secret != "" && file != "":
		return "", errors.New("--secret and --secret-file are exclusive")
	case file != "":
		data, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}

		secret = strings.TrimSpace(string(data))
	}

	if secret == "" {
		return "", errors.New("--secret or --secret-file is required: the server accepts no anonymous calls")
	}

	return secret, nil
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

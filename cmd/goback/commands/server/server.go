package server

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/maintenance"
	"github.com/gobackio/goback/telemetry"

	"github.com/urfave/cli"
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

func serverAction(c *cli.Context) {
	secret, err := sharedSecret(c)
	if err != nil {
		common.Fatal(err)
	}

	_, tlsConfig, err := storage.ServerTLS(c.String("tls-cert"), c.String("tls-key"), c.Bool("plaintext-behind-proxy"))
	if err != nil {
		common.Fatal(err)
	}

	repo := common.OpenRepo(c)

	listener, err := net.Listen("tcp", c.String("address"))
	if err != nil {
		common.Fatal(err)
	}

	if _, err := telemetry.Setup(context.Background(), "goback"); err != nil {
		common.Fatalf("failed setting up telemetry: %s", err)
	}

	common.Fatal(repo.Serve(context.Background(), listener, goback.ServeOptions{
		Secret:               secret,
		TLS:                  tlsConfig,
		PlaintextBehindProxy: c.Bool("plaintext-behind-proxy"),
		Schedule: maintenance.Schedule{
			Sweep:    c.Duration("sweep-interval"),
			Compact:  c.Duration("compact-interval"),
			Collect:  c.Duration("gc-interval"),
			Retire:   c.Duration("retire-interval"),
			Presence: c.Duration("presence-interval"),
		},
	}))
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

package server

import (
	"context"
	"errors"
	"fmt"
	"log"
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
			Usage: "address to listen on; use :6060 to serve agents on other hosts",
			Value: "127.0.0.1:6060",
		},
		cli.StringFlag{
			Name:  "secret",
			Usage: "secret that agents present to this server",
		},
		cli.StringFlag{
			Name:  "secret-file",
			Usage: "file that holds the secret, to keep it off the command line",
		},
		cli.StringFlag{
			Name:   "admin-key",
			Usage:  "key that admin commands present, which alone deletes, changes retention or the store policy, retires, collects and escrows",
			EnvVar: "GOBACK_ADMIN_KEY",
		},
		cli.StringFlag{
			Name:  "admin-key-file",
			Usage: "file that holds the admin key, to keep it off the command line",
		},
		cli.StringFlag{
			Name:  "tls-cert",
			Usage: "PEM certificate chain to present; with --tls-key, agents connect over TLS",
		},
		cli.StringFlag{
			Name:  "tls-key",
			Usage: "PEM private key for --tls-cert",
		},
		cli.BoolFlag{
			Name:  "plaintext-behind-proxy",
			Usage: "serve plaintext behind a proxy that terminates TLS; do not expose this listener directly",
		},
		cli.DurationFlag{
			Name:  "retire-interval",
			Usage: "how often to tombstone retired commits whose trash window has ended; 0 disables",
			Value: maintenance.DefaultSchedule.Retire,
		},
		cli.DurationFlag{
			Name:  "gc-interval",
			Usage: "how often to collect garbage; 0 disables",
			Value: maintenance.DefaultSchedule.Collect,
		},
		cli.DurationFlag{
			Name:  "compact-interval",
			Usage: "how often to compact small archives; 0 disables",
			Value: maintenance.DefaultSchedule.Compact,
		},
		cli.DurationFlag{
			Name:  "sweep-interval",
			Usage: "how often to finalize idle archives and end expired sessions",
			Value: maintenance.DefaultSchedule.Sweep,
		},
		cli.DurationFlag{
			Name:  "presence-interval",
			Usage: "how often to build presence filters and measure set sizes for new commits; 0 disables",
			Value: maintenance.DefaultSchedule.Presence,
		},
	},
}

func serverAction(c *cli.Context) {
	secret, err := sharedSecret(c)
	if err != nil {
		common.Fatal(err)
	}

	adminKey, err := keyFrom(c, "admin-key")
	if err != nil {
		common.Fatal(err)
	}

	_, tlsConfig, err := storage.ServerTLS(c.String("tls-cert"), c.String("tls-key"), c.Bool("plaintext-behind-proxy"))
	if err != nil {
		common.Fatal(err)
	}

	ctx := common.Context(c)
	repo := common.OpenRepo(c)

	listener, err := net.Listen("tcp", c.String("address"))
	if err != nil {
		common.Fatal(err)
	}

	stopTelemetry, err := telemetry.Setup(context.Background(), "goback")
	if err != nil {
		common.Fatalf("failed setting up telemetry: %s", err)
	}

	err = repo.Serve(ctx, listener, goback.ServeOptions{
		Secret:               secret,
		AdminKey:             adminKey,
		TLS:                  tlsConfig,
		PlaintextBehindProxy: c.Bool("plaintext-behind-proxy"),
		Schedule: maintenance.Schedule{
			Sweep:    c.Duration("sweep-interval"),
			Compact:  c.Duration("compact-interval"),
			Collect:  c.Duration("gc-interval"),
			Retire:   c.Duration("retire-interval"),
			Presence: c.Duration("presence-interval"),
		},
	})

	if cerr := repo.Close(); cerr != nil {
		log.Printf("Closing the store failed: %v", cerr)
	}

	if terr := stopTelemetry(context.Background()); terr != nil {
		log.Printf("Flushing telemetry failed: %v", terr)
	}

	if err != nil {
		common.Fatal(err)
	}
}

// sharedSecret reads --secret or --secret-file; the server accepts no
// anonymous calls.
func sharedSecret(ctx *cli.Context) (string, error) {
	secret, err := keyFrom(ctx, "secret")
	if err != nil {
		return "", err
	}

	if secret == "" {
		return "", errors.New("--secret or --secret-file is required: the server accepts no anonymous calls")
	}

	return secret, nil
}

// keyFrom reads the flag name or the file its name-file flag names.
func keyFrom(ctx *cli.Context, name string) (string, error) {
	key, file := ctx.String(name), ctx.String(name+"-file")

	switch {
	case key != "" && file != "":
		return "", fmt.Errorf("--%s and --%s-file are exclusive", name, name)
	case file != "":
		data, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}

		key = strings.TrimSpace(string(data))
	}

	return key, nil
}

package main

import (
	"errors"
	"fmt"
	"os"
	"runtime"

	"github.com/gobackio/goback/cmd/goback/commands/agent"
	"github.com/gobackio/goback/cmd/goback/commands/commit"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/file"
	"github.com/gobackio/goback/cmd/goback/commands/fix"
	"github.com/gobackio/goback/cmd/goback/commands/gc"
	"github.com/gobackio/goback/cmd/goback/commands/indexing"
	"github.com/gobackio/goback/cmd/goback/commands/key"
	"github.com/gobackio/goback/cmd/goback/commands/maintain"
	"github.com/gobackio/goback/cmd/goback/commands/mysql"
	"github.com/gobackio/goback/cmd/goback/commands/object"
	"github.com/gobackio/goback/cmd/goback/commands/pin"
	"github.com/gobackio/goback/cmd/goback/commands/postgres"
	"github.com/gobackio/goback/cmd/goback/commands/repair"
	"github.com/gobackio/goback/cmd/goback/commands/scrub"
	"github.com/gobackio/goback/cmd/goback/commands/server"
	"github.com/gobackio/goback/cmd/goback/commands/set"

	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() + 1)

	app := cli.NewApp()
	app.Name = "goback"
	app.Usage = "Take snapshots of your files and restore them."
	app.Commands = []cli.Command{
		agent.Command,
		commit.Command,
		file.Command,
		fix.Command,
		gc.Command,
		indexing.Command,
		key.Command,
		maintain.Command,
		mysql.Command,
		object.Command,
		pin.Command,
		postgres.Command,
		repair.Command,
		scrub.Command,
		server.Command,
		set.Command,
		docsCommand,
	}
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:   "json",
			Usage:  "print results and errors as JSON on stdout, and logs as JSON lines on stderr",
			EnvVar: "GOBACK_JSON",
		},
		cli.StringFlag{
			Name:   "storage",
			Usage:  "where the objects live: a directory, gcs://bucket, s3://bucket?endpoint=…, goback://key@host:port for a store server, or goback+insecure://key@host:port for one on this machine with no TLS in front of it; required",
			EnvVar: "GOBACK_STORAGE",
		},
		cli.StringFlag{
			Name:   "index",
			Usage:  "where a directory or bucket store keeps its index: a directory for SQLite, or a postgres:// url; required unless --storage is a store server",
			EnvVar: "GOBACK_INDEX",
		},
		cli.StringFlag{
			Name:  "set, s",
			Usage: "name of the backup set a command works on",
		},
		cli.StringFlag{
			Name:  "agent-id",
			Usage: "identifier recorded in commits and presented to a goback:// store server; defaults to the hostname",
		},
		cli.StringFlag{
			Name:  "store-key",
			Usage: "store key file; names and contents are encrypted with it before upload",
		},
		cli.StringFlag{
			Name:   "passphrase",
			Usage:  "without --store-key, open the store key the store keeps escrowed under this passphrase",
			EnvVar: "GOBACK_PASSPHRASE",
		},
		cli.StringFlag{
			Name:  "at-rest-key",
			Usage: "key file a local, gcs:// or s3:// store seals its archives with, from goback key at-rest; empty stores objects as received",
		},
		cli.StringFlag{
			Name:   "cache-dir",
			Usage:  "per-machine directory for the caches of every store: blobs a restore need not download, the stat and tree caches a backup skips unchanged files with, and a bucket's archive metadata; empty caches nothing",
			EnvVar: "GOBACK_CACHE_DIR",
		},
		cli.StringFlag{
			Name:  "metadata-cache-size",
			Usage: "size of a bucket's metadata cache under --cache-dir, past which the commits, trees and files least recently used are let go; 0 keeps them all",
			Value: "1GiB",
		},
		cli.StringFlag{
			Name:  "blob-cache-size",
			Usage: "size the blob cache under --cache-dir is trimmed to after a backup or restore",
			Value: "4GB",
		},
		cli.BoolTFlag{
			Name:  "blob-cache-on-backup",
			Usage: "also put every uploaded blob into the blob cache, so a rollback to the last backup downloads nothing",
		},
	}

	app.Before = func(c *cli.Context) error {
		common.SetOutput(c.GlobalBool("json"))

		return nil
	}

	app.ExitErrHandler = func(_ *cli.Context, err error) {
		var coded cli.ExitCoder
		if errors.As(err, &coded) {
			switch {
			case err.Error() != "":
				common.FailError(err)
			case common.JSON():
				common.Fail(fmt.Sprintf("exit status %d", coded.ExitCode()))
			}

			os.Exit(coded.ExitCode())
		}
	}

	common.SetOutput(false)

	err := app.Run(os.Args)
	if err != nil {
		common.Fatal(err)
	}
}

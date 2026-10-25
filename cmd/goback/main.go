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

	"github.com/joho/godotenv"
	"github.com/urfave/cli"
)

// loadEnvFile sets the variables of --env-file that the environment lacks,
// and gives the global flags read from them their values, since those flags
// took theirs from the environment before the file was read.
func loadEnvFile(c *cli.Context) error {
	path := c.GlobalString("env-file")
	if path == "" {
		return nil
	}

	unset := map[string]string{}
	for _, f := range c.App.Flags {
		var name, env string
		switch f := f.(type) {
		case cli.StringFlag:
			name, env = f.Name, f.EnvVar
		case cli.BoolFlag:
			name, env = f.Name, f.EnvVar
		}

		if env != "" && !c.GlobalIsSet(name) {
			unset[name] = env
		}
	}

	if err := godotenv.Load(path); err != nil {
		return fmt.Errorf("reading the env file: %w", err)
	}

	for name, env := range unset {
		if v, ok := os.LookupEnv(env); ok {
			if err := c.GlobalSet(name, v); err != nil {
				return fmt.Errorf("%s from the env file: %w", env, err)
			}
		}
	}

	return nil
}

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
		cli.StringFlag{
			Name:   "env-file",
			Usage:  "file of NAME=value lines to set in the environment, for variables not set already",
			EnvVar: "GOBACK_ENV_FILE",
		},
		cli.BoolFlag{
			Name:   "json",
			Usage:  "print results and errors as JSON on stdout; logs go to stderr as JSON lines",
			EnvVar: "GOBACK_JSON",
		},
		cli.StringFlag{
			Name:   "storage",
			Usage:  "the store: a directory, gcs://bucket, s3://bucket?endpoint=…, goback://key@host:port for a store server, or goback+insecure://key@host:port for a store server without TLS, such as one on this machine; required",
			EnvVar: "GOBACK_STORAGE",
		},
		cli.StringFlag{
			Name:   "index",
			Usage:  "index of a directory or bucket store: a directory for SQLite, or a postgres:// URL; required unless --storage is a store server",
			EnvVar: "GOBACK_INDEX",
		},
		cli.StringFlag{
			Name:  "set, s",
			Usage: "backup set to work on",
		},
		cli.StringFlag{
			Name:  "agent-id",
			Usage: "this machine's id; goback records it in commits and sends it to a goback:// store server; the hostname by default",
		},
		cli.StringFlag{
			Name:  "store-key",
			Usage: "store key file; goback encrypts names and contents with it before upload",
		},
		cli.StringFlag{
			Name:   "passphrase",
			Usage:  "passphrase of the escrowed store key kept in the store; used when --store-key is not set",
			EnvVar: "GOBACK_PASSPHRASE",
		},
		cli.StringFlag{
			Name:   "passphrase-file",
			Usage:  "file holding the passphrase of the escrowed store key; used when --passphrase is not set",
			EnvVar: "GOBACK_PASSPHRASE_FILE",
		},
		cli.StringFlag{
			Name:  "at-rest-key",
			Usage: "key file that seals the archives of a local, gcs:// or s3:// store, made with goback key at-rest; without it, objects are stored as received",
		},
		cli.StringFlag{
			Name:   "cache-dir",
			Usage:  "directory for this machine's caches of all stores; it holds blobs a restore can use instead of downloading, the stat and tree caches that let a backup skip unchanged files, and a bucket's archive metadata; empty turns caching off",
			EnvVar: "GOBACK_CACHE_DIR",
		},
		cli.StringFlag{
			Name:  "metadata-cache-size",
			Usage: "size limit of a bucket's metadata cache under --cache-dir; above it, the least recently used commits, trees and files are evicted; 0 keeps them all",
			Value: "1GiB",
		},
		cli.StringFlag{
			Name:  "blob-cache-size",
			Usage: "size to trim the blob cache under --cache-dir to after a backup or restore",
			Value: "4GB",
		},
		cli.BoolTFlag{
			Name:  "blob-cache-on-backup",
			Usage: "also add uploaded blobs to the blob cache, so a rollback to the last backup can restore from the cache",
		},
	}

	app.Before = func(c *cli.Context) error {
		if err := loadEnvFile(c); err != nil {
			return err
		}

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

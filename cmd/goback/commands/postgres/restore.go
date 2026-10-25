package postgres

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/proto"

	"github.com/urfave/cli"
)

var restoreCmd = cli.Command{
	Name:      "restore",
	Usage:     "Write a base backup into an empty data directory; with --wal-set, Postgres recovers from that WAL when it starts on the directory",
	ArgsUsage: "<data directory>",
	Description: "With --wal-set, restore sets a restore_command that runs goback for each WAL file Postgres asks\n" +
		"for. It runs this binary with the global flags given here, or the command in --goback. The command\n" +
		"and its flags go into postgresql.auto.conf, or into recovery.conf before Postgres 12. Settings\n" +
		"given as environment variables must also be set in Postgres's environment. With --copy-wal, restore\n" +
		"copies the WAL into pg_wal instead. Postgres then needs neither goback nor the store.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "base-set",
			Usage: "set that holds the cluster's base backups; a number names the set by id and reads it from the store instead of the index",
		},
		cli.StringFlag{
			Name:  "wal-set",
			Usage: "set that holds the cluster's WAL, for recovering past the end of the base backup; a number names the set by id",
		},
		cli.StringFlag{
			Name:  "at",
			Usage: "moment to recover to, in RFC 3339 (2026-10-02T14:30:00Z); the end of the WAL set by default",
		},
		cli.StringFlag{
			Name:  "goback",
			Usage: "command that restore_command uses to run goback, including the global flags",
		},
		cli.BoolFlag{
			Name:  "copy-wal",
			Usage: "copy the WAL recovery reads into the data directory instead of fetching it through restore_command",
		},
		cli.IntFlag{
			Name:  "workers",
			Usage: "how many files to write at once",
			Value: backup.DefaultRestoreWorkers(),
		},
	},
	Action: restoreAction,
}

func restoreAction(c *cli.Context) error {
	if c.NArg() != 1 || c.String("base-set") == "" {
		return cli.NewExitError("--base-set and the data directory are required", 2)
	}

	if c.String("wal-set") == "" && c.String("at") != "" {
		return cli.NewExitError("--at needs --wal-set to recover to", 2)
	}

	if c.Bool("copy-wal") && c.String("goback") != "" {
		return cli.NewExitError("--goback is for restore_command, which --copy-wal does without", 2)
	}

	var at time.Time
	if s := c.String("at"); s != "" {
		var err error
		if at, err = time.Parse(time.RFC3339Nano, s); err != nil {
			return cli.NewExitError(fmt.Sprintf("--at: %v", err), 2)
		}
	}

	repo := common.OpenRepo(c)
	store := repo.Store

	defer func() {
		common.CloseRepo(repo)
	}()

	r := &postgres.Restore{
		Objects: store,
		Key:     common.StoreKey(c, repo),
		Latest:  repo.LatestCommit,
		BaseSet: c.String("base-set"),
		WALSet:  c.String("wal-set"),
		At:      at,
		Workers: c.Int("workers"),
	}

	if !c.Bool("copy-wal") {
		goback := c.String("goback")
		if goback == "" {
			var err error
			if goback, err = invocation(c); err != nil {
				return err
			}
		}

		r.RestoreCommand = func(wal *proto.Ref) string {
			return strings.ReplaceAll(goback, "%", "%%") + " postgres wal-get --wal-set " + shellQuote(c.String("wal-set")) + " --commit " + hex.EncodeToString(wal.Hash) + " %f %p"
		}
	}

	result, err := r.Run(common.Context(c), c.Args().First())
	if err != nil {
		return err
	}

	common.Result(struct {
		Base      views.CommitView `json:"base"`
		WAL       string           `json:"wal,omitempty"`
		Copied    []string         `json:"copied,omitempty"`
		ModeUnset bool             `json:"mode_unset,omitempty"`
	}{common.View.Commit(result.Base), views.Hex(result.WAL), result.Copied, result.ModeUnset}, func() {
		log.Printf("Restored the base backup of %s (commit %x)", time.Unix(result.Base.GetTimestamp(), 0).UTC().Format(time.RFC3339), result.BaseRef.Hash)

		switch {
		case r.WALSet == "":
			log.Printf("Postgres recovers to the end of the base backup")
		case result.WAL == nil:
			log.Printf("The WAL set %s has no commits: Postgres recovers to the end of the base backup", r.WALSet)
		case r.RestoreCommand == nil:
			log.Printf("Copied %d WAL files of commit %x into the data directory; Postgres recovers from them when it starts", len(result.Copied), result.WAL.Hash)
		default:
			log.Printf("Postgres recovers from WAL commit %x when it starts", result.WAL.Hash)
		}

		if result.ModeUnset {
			log.Printf("The data directory could not be given mode 0700, which Postgres needs before it starts")
		}
	})

	return nil
}

// invocation is this binary with the global flags it was started with, quoted
// for the shell restore_command runs in.
func invocation(c *cli.Context) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}

	words := []string{shellQuote(exe)}
	for _, arg := range globalArgs(c, os.Args) {
		words = append(words, shellQuote(arg))
	}

	return strings.Join(words, " "), nil
}

// globalArgs is the part of args, the command line c was parsed from, that
// the app's own flags took: what comes before the first command's name.
func globalArgs(c *cli.Context, args []string) []string {
	for c.Parent() != nil {
		c = c.Parent()
	}

	end := len(args) - len(c.Args())
	if end < 1 {
		return nil
	}

	return args[1:end]
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

var walGetCmd = cli.Command{
	Name:      "wal-get",
	Usage:     "Write one WAL file from a WAL commit to a path; restore_command runs this",
	ArgsUsage: "<%f> <%p>",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "wal-set",
			Usage: "set that holds the WAL commit",
		},
		cli.StringFlag{
			Name:  "commit",
			Usage: "ref of the WAL commit, in hex",
		},
	},
	Action: walGetAction,
}

// walGetFatal is wal-get's status for every failure but a file the commit
// does not hold: Postgres reads 1 as the end of the archive and promotes,
// and stops recovery on a status above 125.
const walGetFatal = 255

// walGetRetries are the waits before each retry of a failed fetch.
var walGetRetries = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}

func walGetAction(c *cli.Context) error {
	hash, err := hex.DecodeString(c.String("commit"))
	if c.NArg() != 2 || err != nil || len(hash) == 0 || c.String("wal-set") == "" {
		return cli.NewExitError("--wal-set, --commit and the file's name and destination are required", walGetFatal)
	}

	ctx := common.Context(c)
	ref := &proto.Ref{Hash: hash}

	for attempt := 0; ; attempt++ {
		err = fetchWAL(ctx, c, ref)
		if err == nil || errors.Is(err, postgres.ErrNoWALFile) || attempt == len(walGetRetries) || ctx.Err() != nil {
			break
		}

		wait := walGetRetries[attempt]
		log.Printf("Fetching %s failed, retrying in %v: %v", c.Args().Get(0), wait, err)

		select {
		case <-ctx.Done():
		case <-time.After(wait):
		}
	}

	switch {
	case errors.Is(err, postgres.ErrNoWALFile):
		return cli.NewExitError("", 1)
	case err != nil:
		return cli.NewExitError(err.Error(), walGetFatal)
	}

	common.Result(common.Done{Action: "fetched", Name: c.Args().Get(0)}, func() {})

	return nil
}

func fetchWAL(ctx context.Context, c *cli.Context, ref *proto.Ref) error {
	settings, err := common.Settings(c)
	if err != nil {
		return err
	}

	repo, err := goback.Open(ctx, settings)
	if err != nil {
		return err
	}
	defer common.CloseRepo(repo)

	key, err := repo.Key(ctx)
	if err != nil {
		return err
	}

	return postgres.FetchWAL(ctx, repo.Store, key, ref, c.Args().Get(0), c.Args().Get(1))
}

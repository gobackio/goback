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

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/postgres"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"

	"github.com/urfave/cli"
)

var restoreCmd = cli.Command{
	Name:      "restore",
	Usage:     "write a base backup into an empty data directory, set up to recover from the WAL set, if given, when Postgres starts on it",
	ArgsUsage: "<data directory>",
	Description: "restore_command runs goback again for every WAL file Postgres asks for: this binary with the\n" +
		"global flags given here, unless --goback names another command. Flags end up in\n" +
		"postgresql.auto.conf, or recovery.conf before Postgres 12; settings from the environment have to\n" +
		"be in Postgres's environment.",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "base-set",
			Usage: "set the cluster's base backups go to; digits name it by id, from the set's head rather than the index",
		},
		cli.StringFlag{
			Name:  "wal-set",
			Usage: "set the cluster's WAL goes to, for recovering past the end of the base; digits name it by id, as --base-set",
		},
		cli.StringFlag{
			Name:  "at",
			Usage: "moment to recover to, as RFC 3339 (2026-10-02T14:30:00Z); the end of the WAL set when empty",
		},
		cli.StringFlag{
			Name:  "goback",
			Usage: "command restore_command runs goback as, global flags included",
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

	var at time.Time
	if s := c.String("at"); s != "" {
		var err error
		if at, err = time.Parse(time.RFC3339Nano, s); err != nil {
			return cli.NewExitError(fmt.Sprintf("--at: %v", err), 2)
		}
	}

	goback := c.String("goback")
	if goback == "" {
		var err error
		if goback, err = invocation(c); err != nil {
			return err
		}
	}

	store := common.GetObjectStore(c)
	index := common.OpenIndex(c, store)

	defer func() {
		common.CloseAll(store, index)
	}()

	r := &postgres.Restore{
		Objects: store,
		Key:     common.StoreKey(c, store),
		Latest:  latest(index, store),
		BaseSet: c.String("base-set"),
		WALSet:  c.String("wal-set"),
		At:      at,
		Hold: func(ctx context.Context, set string, ref *proto.Ref) (context.Context, func(), error) {
			return common.RestoreSession(ctx, store, set, ref)
		},
		RestoreCommand: func(wal *proto.Ref) string {
			return strings.ReplaceAll(goback, "%", "%%") + " postgres wal-get --wal-set " + shellQuote(c.String("wal-set")) + " --commit " + hex.EncodeToString(wal.Hash) + " %f %p"
		},
	}

	result, err := r.Run(common.Context(c), c.Args().First())
	if err != nil {
		return err
	}

	common.Result(struct {
		Base views.CommitView `json:"base"`
		WAL  string           `json:"wal,omitempty"`
	}{common.View.Commit(result.Base), views.Hex(result.WAL)}, func() {
		log.Printf("Restored the base backup of %s (commit %x)", time.Unix(result.Base.GetTimestamp(), 0).UTC().Format(time.RFC3339), result.BaseRef.Hash)

		if r.WALSet == "" {
			log.Printf("Postgres recovers to the end of the base backup")
		} else if result.WAL == nil {
			log.Printf("The WAL set %s has no commits: Postgres recovers to the end of the base backup", r.WALSet)
		} else {
			log.Printf("Postgres recovers from WAL commit %x when it starts", result.WAL.Hash)
		}
	})

	return nil
}

// latest finds a set's newest commit: by id from the set's head, by name
// through the index, and through the
// heads the store keeps when the index has none.
func latest(index backup.Index, store backup.ObjectStore) func(context.Context, string) (*proto.Ref, error) {
	return func(ctx context.Context, set string) (*proto.Ref, error) {
		if id, ok := backup.SetID(set); ok {
			return headOf(store, id)
		}

		ref, err := index.LatestCommit(ctx, set)
		if !errors.Is(err, backup.ErrNotFound) {
			return ref, err
		}

		ps, ok := common.Unwrap(store).(*pack.PackStorage)
		if !ok {
			return nil, err
		}

		heads, herr := ps.Heads()
		if herr != nil {
			return nil, herr
		}

		var newest *pack.Head
		for i, head := range heads {
			if head.Set == set && (newest == nil || head.ReceivedAtNs > newest.ReceivedAtNs) {
				newest = &heads[i]
			}
		}

		if newest == nil {
			return nil, err
		}

		return &proto.Ref{Hash: newest.Commit}, nil
	}
}

// headOf is the newest commit of the set with id.
func headOf(store backup.ObjectStore, id uint64) (*proto.Ref, error) {
	ps, ok := common.Unwrap(store).(*pack.PackStorage)
	if !ok {
		return nil, fmt.Errorf("store %T keeps no set heads to find set %d in", store, id)
	}

	heads, err := ps.Heads()
	if err != nil {
		return nil, err
	}

	for _, head := range heads {
		if head.SetID == id {
			return &proto.Ref{Hash: head.Commit}, nil
		}
	}

	return nil, fmt.Errorf("%w: set %d has no head", backup.ErrNotFound, id)
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
	Usage:     "write a WAL file of a WAL commit to a path, as restore_command",
	ArgsUsage: "<%f> <%p>",
	Flags: []cli.Flag{
		cli.StringFlag{
			Name:  "wal-set",
			Usage: "set the WAL commit belongs to",
		},
		cli.StringFlag{
			Name:  "commit",
			Usage: "the WAL commit, in hex",
		},
	},
	Action: walGetAction,
}

func walGetAction(c *cli.Context) error {
	hash, err := hex.DecodeString(c.String("commit"))
	if c.NArg() != 2 || err != nil || len(hash) == 0 || c.String("wal-set") == "" {
		return cli.NewExitError("--wal-set, --commit and the file's name and destination are required", 2)
	}

	store := common.GetObjectStore(c)
	defer common.CloseStore(store)

	ref := &proto.Ref{Hash: hash}

	ctx, done, err := common.RestoreSession(common.Context(c), store, c.String("wal-set"), ref)
	if err != nil {
		return err
	}
	defer done()

	err = postgres.FetchWAL(ctx, store, common.StoreKey(c, store), ref, c.Args().Get(0), c.Args().Get(1))
	if errors.Is(err, backup.ErrNotFound) {
		return cli.NewExitError("", 1)
	}

	if err != nil {
		return err
	}

	common.Result(common.Done{Action: "fetched", Name: c.Args().Get(0)}, func() {})

	return nil
}

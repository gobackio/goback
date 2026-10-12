package mysql

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/urfave/cli"
)

var restoreCmd = cli.Command{
	Name:  "restore",
	Usage: "load a dump into an empty running server and replay the binlog set, if given, after it",
	Description: "The client programs connect to the target as MySQL's option files say: ~/.my.cnf, or\n" +
		"--defaults-extra-file. --to only writes the dump and the binlogs into a directory and prints\n" +
		"the commands that apply them.",
	Flags: withClient(
		cli.StringFlag{
			Name:  "dump-set",
			Usage: "set the server's dumps go to",
		},
		cli.StringFlag{
			Name:  "binlog-set",
			Usage: "set the server's binlogs go to, for restoring past the dump",
		},
		cli.StringFlag{
			Name:  "at",
			Usage: "moment to restore to, as RFC 3339 (2026-10-02T14:30:00Z), to the second; the end of the binlog set when empty",
		},
		cli.StringFlag{
			Name:  "to",
			Usage: "empty directory to write the dump and the binlogs into instead of applying them",
		},
		cli.StringFlag{
			Name:  "work-dir",
			Usage: "directory the binlogs are fetched into before the replay; the system's temporary directory when empty",
		},
		cli.BoolFlag{
			Name:  "force",
			Usage: "load into a server that holds databases already",
		},
	),
	Action: restoreAction,
}

func restoreAction(c *cli.Context) error {
	if c.String("dump-set") == "" {
		return cli.NewExitError("--dump-set is required", 2)
	}

	if c.String("binlog-set") == "" && c.String("at") != "" {
		return cli.NewExitError("--at needs --binlog-set to replay to", 2)
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

	exec := client(c)

	r := &mysql.Restore{
		Objects:   store,
		Key:       common.StoreKey(c, repo),
		Latest:    repo.LatestCommit,
		DumpSet:   c.String("dump-set"),
		BinlogSet: c.String("binlog-set"),
		At:        at,
		Client:    exec,
		WorkDir:   c.String("work-dir"),
		To:        c.String("to"),
		Force:     c.Bool("force"),
	}

	if r.WorkDir == "" {
		r.WorkDir = os.TempDir()
	}

	result, err := r.Run(common.Context(c))
	if err != nil {
		return err
	}

	if result.BinlogFormat == "STATEMENT" {
		log.Printf("The binlogs are in STATEMENT format: a replay of nondeterministic statements can differ from the source")
	}

	var commands []string
	if r.To != "" {
		commands = append(commands, exec.LoadCommand(result.DumpPath))
		if len(result.ReplayArgs) > 0 {
			commands = append(commands, exec.ReplayCommand(result.ReplayArgs))
		}
	}

	common.Result(struct {
		Dump     views.CommitView `json:"dump"`
		Binlog   string           `json:"binlog,omitempty"`
		Binlogs  []string         `json:"binlogs"`
		Commands []string         `json:"commands,omitempty"`
	}{common.View.Commit(result.Dump), views.Hex(result.Binlog), result.Binlogs, commands}, func() {
		taken := time.Unix(result.Dump.GetTimestamp(), 0).UTC().Format(time.RFC3339)

		if r.To != "" {
			log.Printf("Wrote the dump of %s (commit %x) and %d binlogs to %s; apply them with:", taken, result.DumpRef.Hash, len(result.Binlogs), r.To)

			for _, command := range commands {
				fmt.Println(command)
			}

			return
		}

		log.Printf("Restored the dump of %s (commit %x)", taken, result.DumpRef.Hash)

		switch {
		case r.BinlogSet == "":
		case result.Binlog == nil:
			log.Printf("The binlog set %s has no commits: the restore ends at the dump", r.BinlogSet)
		default:
			log.Printf("Replayed %d binlogs of commit %x", len(result.Binlogs), result.Binlog.Hash)
		}
	})

	return nil
}

package mysql

import (
	"fmt"
	"log"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/blobcache"
	"github.com/gobackio/goback/backup/mysql"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/storage/cache"

	"github.com/urfave/cli"
)

// treeCacheSize bounds the commits and trees kept under --cache-dir; the
// latest commit's are read every run, so they are never the ones let go.
const treeCacheSize = 256 << 20

var binlogCmd = cli.Command{
	Name:  "binlog",
	Usage: "rotate the binary log when it holds anything new, and commit the closed binlogs to the set named by --set",
	Description: "Run it every minute. It reads the binlog files directly, so it runs where they are, as a user\n" +
		"that can read them. After RESET MASTER the binlogs go on in a new chain of the same set.",
	Flags: withClient(
		cli.StringFlag{
			Name:  "dump-set",
			Usage: "set the server's dumps go to",
		},
		cli.StringFlag{
			Name:   "binlog-dir",
			Usage:  "directory the server writes its binlogs to; the directory of @@log_bin_basename, else @@datadir, when empty",
			EnvVar: "GOBACK_MYSQL_BINLOG_DIR",
		},
		cli.DurationFlag{
			Name:  "window",
			Usage: "how far back point-in-time recovery reaches; binlogs older than the oldest dump inside it are let go",
			Value: 14 * 24 * time.Hour,
		},
	),
	Action: binlogAction,
}

func binlogAction(c *cli.Context) error {
	if c.String("dump-set") == "" || c.GlobalString("set") == "" {
		return cli.NewExitError("--set and --dump-set are required", 2)
	}

	store := common.GetObjectStore(c)
	index := common.OpenIndex(c, store)

	defer func() {
		common.CloseAll(store, index)
	}()

	sessions, _ := store.(backup.SessionStore)

	objects := backup.ObjectStore(index)

	var trees *blobcache.Cache
	if dir := common.StoreCache(c, "backup"); dir != "" {
		var err error
		if trees, err = blobcache.Open(dir, "trees", treeCacheSize); err != nil {
			return fmt.Errorf("opening the tree cache: %w", err)
		}

		objects = cache.New(trees, index)
	}

	run := &mysql.BinlogBackup{
		Walker: &backup.Walker{
			Index:       index,
			Objects:     objects,
			Sessions:    sessions,
			Set:         c.GlobalString("set"),
			AgentID:     common.AgentID(c),
			Key:         common.StoreKey(c, store),
			Workers:     4,
			ReadRetries: 3,
		},
		Client:  client(c),
		Dir:     c.String("binlog-dir"),
		DumpSet: c.String("dump-set"),
		Window:  c.Duration("window"),
		Now:     time.Now,
	}

	result, err := run.Run(common.Context(c))
	if err != nil {
		return err
	}

	if trees != nil {
		if _, err := trees.Sweep(); err != nil {
			log.Printf("sweeping the tree cache: %v", err)
		}
	}

	if result == nil {
		common.Result(struct {
			Files int64 `json:"files"`
		}{}, func() { log.Print("No new closed binlog") })

		return nil
	}

	meta := result.Commit.Metadata

	common.Result(struct {
		views.WalkView
		Chain      string `json:"chain"`
		LastBinlog string `json:"last_binlog"`
	}{common.View.Walk(result), meta[mysql.MetaChain], meta[mysql.MetaLastBinlog]}, func() {
		log.Printf("Commit %x: binlogs of chain %s up to %s", result.Ref.Hash, meta[mysql.MetaChain], meta[mysql.MetaLastBinlog])
	})

	return nil
}

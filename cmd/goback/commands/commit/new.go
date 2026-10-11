package commit

import (
	"context"
	"errors"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/cmd/goback/commands/common"

	"github.com/urfave/cli"
)

// errDirty marks a run that committed but recorded torn or unreadable files.
var errDirty = errors.New("some files were torn or unreadable")

func newAction(c *cli.Context) {
	err := runNew(c)
	if errors.Is(err, errDirty) {
		os.Exit(1)
	}

	if err != nil {
		common.Fatalf("%+v", err)
	}
}

func runNew(c *cli.Context) error {
	dir := "."
	if c.Args().Present() {
		dir = c.Args().First()
	}

	env := common.Env(c)

	defer func() {
		common.CloseAll(env.Store, env.Index)
	}()

	if cacheDir := common.StoreCache(c, "backup"); cacheDir != "" {
		stats, err := job.OpenStatCache(cacheDir, env.Logger)
		if err != nil {
			return err
		}

		if stats != nil {
			defer stats.Close()

			env.StatCache = stats
		}

		objects, err := job.OpenObjectCache(cacheDir, env.Logger)
		if err != nil {
			return err
		}

		if objects != nil {
			defer objects.Close()

			env.ObjectCache = objects
		}
	}

	if c.GlobalBoolT("blob-cache-on-backup") {
		env.BlobCache = common.BlobCache(c, env.Key)
	}

	ctx := common.Context(c)
	if d := c.Duration("max-duration"); d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}

	result, err := job.Path(ctx, env, job.PathSettings{
		Set:                c.GlobalString("set"),
		Dir:                dir,
		Include:            c.StringSlice("include"),
		Exclude:            c.StringSlice("exclude"),
		Workers:            c.Int("workers"),
		ForceHash:          c.Int("force-hash"),
		CheckpointInterval: c.Duration("checkpoint-interval"),
		ScanWorkers:        c.Int("scan-workers"),
		ReadRetries:        c.Int("read-retries"),
		LostRetries:        c.Int("lost-retries"),
		ProgressInterval:   c.Duration("progress-interval"),
		Meta:               common.Metadata(c),
		PreHook:            c.String("pre-hook"),
		PostHook:           c.String("post-hook"),
		HookTimeout:        c.Duration("hook-timeout"),
		HookOptional:       c.Bool("hook-optional"),
	})
	if err != nil {
		return err
	}

	common.SweepBlobCache(env.BlobCache)

	common.Result(common.View.Walk(result), func() {
		log.Printf("Commit %x: %d files, %d reused, %d read, %d checkpoints", result.Ref.Hash, result.Files, result.Reused, result.Read, result.Checkpoints)

		if result.Torn > 0 || result.Unreadable > 0 || result.Skipped > 0 {
			log.Printf("%d files changed while being read, %d could not be read, %d irregular entries skipped", result.Torn, result.Unreadable, result.Skipped)
		}
	})

	if result.Dirty() {
		return errDirty
	}

	return nil
}

var newCmd = cli.Command{
	Name:      "new",
	Usage:     "Create a new commit by diffing the directory against the set's latest commit",
	ArgsUsage: "[directory]",
	Action:    newAction,
	Flags: []cli.Flag{
		cli.StringSliceFlag{
			Name:  "include, i",
			Usage: "back up paths matching this glob even when an --exclude matches them; paths start with / at the directory; repeatable",
			Value: new(cli.StringSlice),
		},
		cli.StringSliceFlag{
			Name:  "exclude, e",
			Usage: "skip paths matching this glob; paths start with / at the directory; repeatable",
			Value: new(cli.StringSlice),
		},
		cli.IntFlag{
			Name:  "workers, w",
			Usage: "how many files to read and upload at once",
			Value: runtime.NumCPU(),
		},
		cli.IntFlag{
			Name:  "force-hash",
			Usage: "percent of unchanged files to re-hash locally as a check on the change test",
		},
		cli.DurationFlag{
			Name:  "checkpoint-interval",
			Usage: "write a partial commit this often so an interrupted run keeps its progress; 0 disables",
			Value: 30 * time.Minute,
		},
		cli.IntFlag{
			Name:  "scan-workers",
			Usage: "how many directories to list ahead of the walk; negative lists every directory on the walk itself",
			Value: backup.DefaultScanWorkers,
		},
		cli.IntFlag{
			Name:  "read-retries",
			Usage: "how often to re-read a file that changes while it is being read",
			Value: 3,
		},
		cli.IntFlag{
			Name:  "lost-retries",
			Usage: "how often to walk again when the store lost objects of the run before its commit; 0 fails the run instead",
			Value: 1,
		},
		cli.DurationFlag{
			Name:  "progress-interval",
			Usage: "how often to report what the walk has covered so far; negative disables",
			Value: backup.DefaultProgressInterval,
		},
		common.MetaFlag,
		cli.StringFlag{
			Name:  "pre-hook",
			Usage: "shell command run before the walk, for example one that pauses the application's writes; see contrib/hooks",
		},
		cli.StringFlag{
			Name:  "post-hook",
			Usage: "shell command run after the walk on every exit the agent controls: success, failure, Ctrl-C and deadlines",
		},
		cli.DurationFlag{
			Name:  "hook-timeout",
			Usage: "deadline for each hook",
			Value: 5 * time.Minute,
		},
		cli.BoolFlag{
			Name:  "hook-optional",
			Usage: "back up anyway when the pre hook fails",
		},
		cli.DurationFlag{
			Name:  "max-duration",
			Usage: "cancel the walk after this long, still running the post hook; 0 disables",
		},
	},
}

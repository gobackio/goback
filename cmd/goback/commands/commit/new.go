package commit

import (
	"context"
	"errors"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/gobackio/goback"
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

	initial := common.InitialRetention(c)
	repo := common.OpenRepo(c)
	env := common.Env(c, repo, goback.Caches{Stats: true, Objects: true, Blobs: c.GlobalBoolT("blob-cache-on-backup")})

	defer func() {
		_ = env.Close()
		common.CloseRepo(repo)
	}()

	ctx := common.Context(c)
	if d := c.Duration("max-duration"); d > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}

	result, err := job.Path(ctx, env, job.PathSettings{
		Set:                  c.GlobalString("set"),
		Dir:                  dir,
		Include:              c.StringSlice("include"),
		Exclude:              c.StringSlice("exclude"),
		Workers:              c.Int("workers"),
		ForceHash:            c.Int("force-hash"),
		CheckpointInterval:   c.Duration("checkpoint-interval"),
		ScanWorkers:          c.Int("scan-workers"),
		ReadRetries:          c.Int("read-retries"),
		LostRetries:          c.Int("lost-retries"),
		ProgressInterval:     c.Duration("progress-interval"),
		Meta:                 common.Metadata(c),
		PreHook:              c.String("pre-hook"),
		PostHook:             c.String("post-hook"),
		HookTimeout:          c.Duration("hook-timeout"),
		HookOptional:         c.Bool("hook-optional"),
		VSS:                  c.Bool("vss"),
		DownloadPlaceholders: c.Bool("download-placeholders"),
		InitialRetention:     initial,
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

		if result.Placeholders > 0 {
			log.Printf("%d files only in the cloud recorded without content; --download-placeholders reads them", result.Placeholders)
		}
	})

	if result.Dirty() {
		return errDirty
	}

	return nil
}

var newCmd = cli.Command{
	Name:      "new",
	Usage:     "Back up a directory as a new commit; unchanged files are taken from the set's latest commit",
	ArgsUsage: "[directory]",
	Action:    newAction,
	Flags: []cli.Flag{
		cli.StringSliceFlag{
			Name:  "include, i",
			Usage: "back up paths that match this glob, even if an --exclude matches them; patterns start with / at the directory; repeatable",
			Value: new(cli.StringSlice),
		},
		cli.StringSliceFlag{
			Name:  "exclude, e",
			Usage: "skip paths that match this glob; patterns start with / at the directory; repeatable",
			Value: new(cli.StringSlice),
		},
		cli.IntFlag{
			Name:  "workers, w",
			Usage: "how many files to read and upload at once",
			Value: runtime.NumCPU(),
		},
		cli.IntFlag{
			Name:  "force-hash",
			Usage: "percentage of seemingly unchanged files to hash anyway, to check the change detection",
		},
		cli.DurationFlag{
			Name:  "checkpoint-interval",
			Usage: "save a partial commit this often so an interrupted run keeps its progress; 0 disables",
			Value: 30 * time.Minute,
		},
		cli.IntFlag{
			Name:  "scan-workers",
			Usage: "how many directories to list ahead of the walk; a negative value lists each directory when the walk reaches it",
			Value: backup.DefaultScanWorkers,
		},
		cli.IntFlag{
			Name:  "read-retries",
			Usage: "how many times to re-read a file that changes while it is being read",
			Value: 3,
		},
		cli.IntFlag{
			Name:  "lost-retries",
			Usage: "how many times to walk again when the store loses objects of the run before it commits; 0 fails the run instead",
			Value: 1,
		},
		cli.DurationFlag{
			Name:  "progress-interval",
			Usage: "how often to report the walk's progress; negative disables",
			Value: backup.DefaultProgressInterval,
		},
		common.MetaFlag,
		cli.StringFlag{
			Name:  "pre-hook",
			Usage: "shell command to run before the walk, for example to pause the application's writes; see contrib/hooks",
		},
		cli.StringFlag{
			Name:  "post-hook",
			Usage: "shell command to run after the walk, whether the walk succeeds, fails, is stopped with Ctrl-C or hits a deadline",
		},
		cli.DurationFlag{
			Name:  "hook-timeout",
			Usage: "time limit for each hook",
			Value: 5 * time.Minute,
		},
		cli.BoolFlag{
			Name:  "hook-optional",
			Usage: "back up even if the pre-hook fails",
		},
		cli.BoolFlag{
			Name:  "vss",
			Usage: "on Windows, read the directory from a shadow copy taken after the pre-hook, which needs an elevated administrator; without one the live files are read",
		},
		cli.BoolFlag{
			Name:  "download-placeholders",
			Usage: "on Windows, read files OneDrive and other sync clients keep only in the cloud, which downloads them; without it they are recorded without content",
		},
		cli.DurationFlag{
			Name:  "max-duration",
			Usage: "cancel the walk after this long; the post-hook still runs; 0 disables",
		},
		common.InitialRetentionFlag,
	},
}

package commit

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

// restoreProgress counts the files of a restore and the logical bytes they
// hold; a file is done once the restore is through with it, whether it was
// written, found unchanged or failed.
type restoreProgress struct {
	filesTotal, bytesTotal int64
	filesDone, bytesDone   atomic.Int64

	// downloaded, when set, is how many bytes the store has sent over the
	// wire so far
	downloaded func() int64
}

// wrote counts a part of a file into bytes_done as it lands.
func (p *restoreProgress) wrote(n int64) { p.bytesDone.Add(n) }

// done counts a file of size whose parts wrote written bytes, adding
// whatever of size they did not, as for a file found unchanged.
func (p *restoreProgress) done(size, written int64) {
	p.filesDone.Add(1)
	p.bytesDone.Add(max(size-written, 0))
}

func (p *restoreProgress) log() {
	files, bytes := p.filesDone.Load(), p.bytesDone.Load()

	attrs := []any{
		slog.Int64("files_done", files), slog.Int64("files_total", p.filesTotal),
		slog.Int64("bytes_done", bytes), slog.Int64("bytes_total", p.bytesTotal),
	}

	downloaded := ""
	if p.downloaded != nil {
		n := p.downloaded()
		attrs = append(attrs, slog.Int64("bytes_downloaded", n))
		downloaded = fmt.Sprintf(", %s downloaded", humanize.Bytes(uint64(n)))
	}

	if common.JSON() {
		slog.Info("progress", attrs...)

		return
	}

	log.Printf("%d of %d files (%s of %s) restored%s",
		files, p.filesTotal, humanize.Bytes(uint64(bytes)), humanize.Bytes(uint64(p.bytesTotal)), downloaded)
}

// report logs the progress every interval, and once more when the returned
// stop is called; a negative interval logs nothing.
func (p *restoreProgress) report(interval time.Duration) func() {
	if interval < 0 {
		return func() {}
	}

	if interval == 0 {
		interval = backup.DefaultProgressInterval
	}

	done, gone := make(chan struct{}), make(chan struct{})

	go func() {
		defer close(gone)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				p.log()
				return
			case <-ticker.C:
				p.log()
			}
		}
	}()

	return func() {
		close(done)
		<-gone
	}
}

// restoreTarget is the instant a restore picks the newest commit at or
// before; at most one of age, at and ref may be given.
func restoreTarget(now time.Time, age, at, ref string) (time.Time, error) {
	given := 0
	for _, s := range []string{age, at, ref} {
		if s != "" {
			given++
		}
	}

	if given > 1 {
		return time.Time{}, errors.New("<age>, --at and --ref are mutually exclusive")
	}

	if at != "" {
		t, err := time.Parse(time.RFC3339, at)
		if err != nil {
			return time.Time{}, errors.Wrap(err, "parsing --at")
		}

		return t, nil
	}

	if age == "" {
		return now, nil
	}

	d, err := time.ParseDuration(age)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "parsing <age>")
	}

	return now.Add(-d), nil
}

func (c *commit) restore() error {
	commit, err := backup.PickCommit(c.ctx, c.index, c.set, c.ref, c.when)
	if err != nil {
		return err
	}

	ref := proto.NewObject(commit).Ref()
	log.Printf("Restoring commit %x from %v", ref.Hash, commit.Timestamp)

	lost := c.lostHere(ref)

	progress := &restoreProgress{}
	if counted, ok := c.store.(interface{ Downloaded() int64 }); ok {
		progress.downloaded = counted.Downloaded
	}

	stopProgress := func() {}

	var unrestored atomic.Int64

	err = c.restorer.RestoreTree(c.ctx, commit, c.base, backup.TreeOptions{
		From:   c.from,
		Delete: c.delete,
		Listed: func(files, bytes int64) {
			progress.filesTotal, progress.bytesTotal = files, bytes
			stopProgress = progress.report(c.progressInterval)
		},
		Written: progress.wrote,
		Done: func(f backup.RestoredFile) error {
			progress.done(f.Size, f.Written)

			if err := f.Err; err != nil {
				if lost[string(f.Ref.GetHash())] {
					err = fmt.Errorf("%w (repair recorded this version as unrecoverable)", err)
				}

				// a salvage restores what it can and reports the rest, but
				// past the quota every other file would fail the same way
				if !c.restorer.Salvage || errors.Is(err, backup.ErrQuotaExceeded) {
					return err
				}

				unrestored.Add(1)
				log.Printf("cannot restore %s: %v", f.Path, err)

				return nil
			}

			if f.Outcome != backup.OutcomeUnchanged && f.Outcome != backup.OutcomeSkipped {
				log.Printf("%s: %s", f.Outcome, f.Path)
			}

			return nil
		},
		Removing: func(path string) {
			if c.restorer.DryRun {
				log.Printf("would remove: %s", path)
			} else {
				log.Printf("Removing %s", path)
			}
		},
	})

	stopProgress()

	if err != nil {
		return err
	}

	stats, missed := c.restorer.Stats(), unrestored.Load()

	common.Result(struct {
		Commit     views.CommitView    `json:"commit"`
		DryRun     bool                `json:"dry_run"`
		Stats      backup.RestoreStats `json:"stats"`
		Unrestored int64               `json:"unrestored"`
	}{common.View.Commit(commit), c.restorer.DryRun, stats, int64(missed)}, func() {
		logStats(stats)

		if missed > 0 {
			log.Printf("%d files could not be restored at all", missed)
		}
	})

	return nil
}

func logStats(stats backup.RestoreStats) {
	log.Printf("%d files: %d written, %d unchanged, %d skipped by stat; bytes from destination %s, seeds %s, cache %s, store %s",
		stats.Files, stats.Written, stats.Unchanged, stats.Skipped,
		humanize.Bytes(uint64(stats.BytesFromDestination)), humanize.Bytes(uint64(stats.BytesFromSeeds)),
		humanize.Bytes(uint64(stats.BytesFromCache)), humanize.Bytes(uint64(stats.BytesFromStore)))

	if stats.Salvaged > 0 {
		log.Printf("%d files salvaged with %s missing", stats.Salvaged, humanize.Bytes(uint64(stats.MissingBytes)))
	}
}

func overwriteMode(name string) backup.OverwriteMode {
	switch name {
	case "always":
		return backup.OverwriteAlways
	case "if-changed":
		return backup.OverwriteIfChanged
	}

	common.Fatalf("unknown --overwrite mode %q: use always or if-changed", name)

	return backup.OverwriteAlways
}

func restoreAction(c *cli.Context) {
	dst := c.Args().Get(0)

	when, err := restoreTarget(time.Now(), c.Args().Get(1), c.String("at"), c.String("ref"))
	if err != nil {
		common.Fatal(err)
	}

	var ref *proto.Ref
	if hex := c.String("ref"); hex != "" {
		ref = common.ParseRef(hex)
	}

	base := filepath.Clean(dst)

	if !c.Bool("force") {
		held, err := backup.LiveMarkers(base)
		if err != nil {
			common.Fatal(err)
		}

		if len(held) > 0 {
			common.Fatalf("%s looks live: %s is held by another process; stop the server or pass --force", base, held[0])
		}
	}

	repo := common.OpenRepo(c)
	store := repo.Store
	index := repo.Index
	key := common.StoreKey(c, repo)

	restorer := &backup.Restorer{
		Store:     store,
		Key:       key,
		Cache:     common.BlobCache(c, repo),
		Workers:   c.Int("workers"),
		Overwrite: overwriteMode(c.String("overwrite")),
		Verify:    c.Bool("verify"),
		DryRun:    c.Bool("dry-run"),
	}

	common.Salvage(c, restorer)
	restorer.Rechunk = c.Bool("rechunk")

	if seeds := c.StringSlice("seed"); len(seeds) > 0 {
		restorer.Seeds = backup.NewSeedMap(key)
		for _, seed := range seeds {
			if err := restorer.Seeds.Add(seed); err != nil {
				common.Fatalf("indexing seed %s: %v", seed, err)
			}
		}

		log.Printf("Indexed %d chunks from %d seeds", restorer.Seeds.Len(), len(seeds))
	}

	s := &commit{
		ctx:      common.Context(c),
		index:    index,
		base:     base,
		when:     when,
		ref:      ref,
		from:     c.String("from"),
		delete:   c.Bool("delete"),
		restorer: restorer,
		store:    store,
		set:      c.GlobalString("set"),

		progressInterval: c.Duration("progress-interval"),
	}

	err = s.restore()
	if err != nil {
		common.Fatal(err)
	}

	common.SweepBlobCache(restorer.Cache)

	common.CloseRepo(repo)
}

var restoreFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "overwrite",
		Usage: "always: hash every part of an existing file and rewrite what differs; if-changed: trust a size and mtime match",
		Value: "always",
	},
	cli.BoolFlag{
		Name:  "verify",
		Usage: "rehash every written file before it replaces the destination",
	},
	cli.BoolFlag{
		Name:  "dry-run",
		Usage: "report what would change without writing",
	},
	cli.StringSliceFlag{
		Name:  "seed",
		Usage: "file or directory whose chunks may be used instead of downloading; repeatable",
		Value: new(cli.StringSlice),
	},
	cli.IntFlag{
		Name:  "workers",
		Usage: "files and parts fetched at once",
		Value: backup.DefaultRestoreWorkers(),
	},
	common.SalvageFlag,
}

var restoreCmd = cli.Command{
	Name:      "restore",
	Usage:     "Restore a commit into a directory in place, downloading only the parts the directory does not already hold",
	ArgsUsage: "<directory> [age]",
	Action:    restoreAction,
	Flags: append([]cli.Flag{
		cli.StringFlag{
			Name:  "from",
			Usage: "restore only the subtree at this slash-separated path in the commit",
			Value: "",
		},
		cli.StringFlag{
			Name:  "ref",
			Usage: "restore exactly the commit with this hex ref",
		},
		cli.StringFlag{
			Name:  "at",
			Usage: "restore the newest commit at or before this RFC3339 time",
		},
		cli.BoolFlag{
			Name:  "delete",
			Usage: "remove files under the target that the commit does not contain",
		},
		cli.BoolFlag{
			Name:  "force",
			Usage: "restore even when a session.lock under the target is held by a running server",
		},
		cli.DurationFlag{
			Name:  "progress-interval",
			Usage: "how often to report the files and bytes restored so far; negative disables",
			Value: backup.DefaultProgressInterval,
		},
	}, restoreFlags...),
}

// lostLister is an index that knows which versions a repair could not
// keep.
type lostLister interface {
	LostVersions(ctx context.Context, set string) ([]sql.LostVersion, error)
}

// lostHere are the file versions this commit holds that a repair recorded
// as unrecoverable, by ref, so a failed restore can say why.
func (c *commit) lostHere(commit *proto.Ref) map[string]bool {
	lister, ok := c.index.(lostLister)
	if !ok {
		return nil
	}

	versions, err := lister.LostVersions(c.ctx, c.set)
	if err != nil {
		log.Printf("Reading the set's lost versions failed: %v", err)
		return nil
	}

	lost := map[string]bool{}
	for _, version := range versions {
		for _, held := range version.Commits {
			if held.Equal(commit) {
				lost[string(version.Ref.GetHash())] = true
				log.Printf("%s is unrecoverable in this commit: a repair could not keep its content", version.Path)
			}
		}
	}

	return lost
}

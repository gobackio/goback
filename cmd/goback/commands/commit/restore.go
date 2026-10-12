package commit

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

type restoredDir struct {
	path    string
	mode    os.FileMode
	modTime time.Time
}

type restoreEntry struct {
	path string
	info os.FileInfo
	ref  *proto.Ref
}

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

// file counts one file's parts into bytes_done as they are written; done
// then adds whatever of size its parts did not, as for a file found
// unchanged.
func (p *restoreProgress) file(size int64) (written func(int64), done func()) {
	var landed atomic.Int64

	written = func(n int64) {
		landed.Add(n)
		p.bytesDone.Add(n)
	}

	done = func() {
		p.filesDone.Add(1)
		p.bytesDone.Add(max(size-landed.Load(), 0))
	}

	return written, done
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

// pick returns the commit c.ref names, or else the set's newest at or
// before c.when.
func (c *commit) pick() (*proto.Commit, error) {
	if c.ref == nil {
		commits, err := c.index.CommitInfo(c.ctx, c.set, c.when, 1)
		if err != nil {
			return nil, err
		}

		if len(commits) != 1 {
			return nil, errors.Errorf("set %s has no commit at or before %s", c.set, c.when.Format(time.RFC3339))
		}

		return commits[0], nil
	}

	obj, err := c.index.Get(c.ctx, c.ref)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, errors.Errorf("set %s has no commit %x", c.set, c.ref.Hash)
	}
	if err != nil {
		return nil, err
	}

	commit := obj.GetCommit()
	if commit == nil {
		return nil, errors.Errorf("%x is not a commit", c.ref.Hash)
	}

	if commit.GetBackupSet() != c.set {
		return nil, errors.Errorf("commit %x belongs to set %s, not %s", c.ref.Hash, commit.GetBackupSet(), c.set)
	}

	return commit, nil
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
	commit, err := c.pick()
	if err != nil {
		return err
	}

	c.reader, err = c.reader.ForCommit(c.ctx, commit)
	if err != nil {
		return err
	}

	ref := proto.NewObject(commit).Ref()
	log.Printf("Restoring commit %x from %v", ref.Hash, commit.Timestamp)

	lost := c.lostHere(ref)

	ctx, done, err := common.RestoreSession(c.ctx, c.store, c.set, ref)
	if err != nil {
		return err
	}
	defer done()
	c.ctx = ctx

	tree := commit.Tree
	var parent []byte

	if c.from != "" {
		tree, parent, err = c.reader.GetTree(c.ctx, tree, strings.Split(c.from, "/"))
		if err != nil {
			return err
		}
	}

	// the whole tree is listed before anything is written, so the totals
	// progress reports against are known from the start
	var entries []restoreEntry
	progress := &restoreProgress{}
	if counted, ok := c.store.(interface{ Downloaded() int64 }); ok {
		progress.downloaded = counted.Downloaded
	}

	err = c.reader.WalkTree(c.ctx, tree, parent, func(path string, info os.FileInfo, ref *proto.Ref) error {
		entries = append(entries, restoreEntry{path: filepath.Join(c.base, path), info: info, ref: ref})

		if stat, _ := info.Sys().(*proto.FileInfo); !info.IsDir() && stat != nil && stat.Type != proto.NodeType_NODE_SYMLINK {
			progress.filesTotal++
			progress.bytesTotal += stat.Size
		}

		return nil
	})
	if err != nil {
		return err
	}

	stopProgress := progress.report(c.progressInterval)

	restored := map[string]bool{}
	var dirs []restoredDir
	var unrestored atomic.Int64

	// every directory and link is made before the files go to the
	// restorer, which takes several at a time
	var (
		files    []backup.FileToRestore
		finished []func()
	)

	for _, entry := range entries {
		path, info, ref := entry.path, entry.info, entry.ref
		restored[path] = true

		if info.IsDir() {
			// directory modes and times are set after the subtree is written:
			// a read-only directory could not take its children, and the
			// children would clobber its time
			dirs = append(dirs, restoredDir{path: path, mode: info.Mode(), modTime: info.ModTime()})

			if c.restorer.DryRun {
				continue
			}

			if err = restoreDir(path, info.Mode()); err != nil {
				break
			}

			continue
		}

		stat, _ := info.Sys().(*proto.FileInfo)
		if stat == nil {
			err = errors.Errorf("no stat for %s", path)
			break
		}

		if stat.Type == proto.NodeType_NODE_SYMLINK {
			if c.restorer.DryRun {
				continue
			}

			if err = restoreSymlink(path, string(stat.LinkTarget)); err != nil {
				break
			}

			continue
		}

		written, done := progress.file(stat.Size)
		files = append(files, backup.FileToRestore{Path: path, Stat: stat, Ref: ref, Written: written})
		finished = append(finished, done)
	}

	if err == nil {
		err = c.restorer.RestoreFiles(c.ctx, files, func(i int, outcome backup.Outcome, err error) error {
			defer finished[i]()

			path, ref := files[i].Path, files[i].Ref

			if err != nil {
				if lost[string(ref.GetHash())] {
					err = fmt.Errorf("%w (repair recorded this version as unrecoverable)", err)
				}

				// a salvage restores what it can and reports the rest, but
				// past the quota every other file would fail the same way
				if !c.restorer.Salvage || errors.Is(err, backup.ErrQuotaExceeded) {
					return err
				}

				unrestored.Add(1)
				log.Printf("cannot restore %s: %v", path, err)

				return nil
			}

			if outcome != backup.OutcomeUnchanged && outcome != backup.OutcomeSkipped {
				log.Printf("%s: %s", outcome, path)
			}

			return nil
		})
	}

	stopProgress()

	if err != nil {
		return err
	}

	if c.delete {
		err = removeUnrestored(c.base, restored, c.restorer.DryRun)
		if err != nil {
			return err
		}
	}

	if !c.restorer.DryRun {
		for i := len(dirs) - 1; i >= 0; i-- {
			if err = os.Chmod(dirs[i].path, dirs[i].mode.Perm()); err != nil {
				return err
			}

			if err = os.Chtimes(dirs[i].path, time.Now(), dirs[i].modTime); err != nil {
				return err
			}
		}
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

// restoreDir makes path the recorded directory, which its owner can write
// into until the restore sets mode: whatever else is there, a file or a
// symlink the restore must not write through, is removed.
func restoreDir(path string, mode os.FileMode) error {
	if existing, err := os.Lstat(path); err == nil && !existing.IsDir() {
		if err := os.Remove(path); err != nil {
			return err
		}
	}

	mode = mode.Perm() | 0o300

	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}

	return os.Chmod(path, mode)
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

// restoreSymlink recreates a link as recorded and never follows it.
func restoreSymlink(path, target string) error {
	if existing, err := os.Readlink(path); err == nil && existing == target {
		return nil
	}

	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return os.Symlink(target, path)
}

// removeUnrestored deletes everything under base that the restore did not
// write, deepest entries first, except the partial files a later restore
// of the same paths resumes from.
func removeUnrestored(base string, restored map[string]bool, dryRun bool) error {
	keep := make(map[string]bool, 2*len(restored))
	for path := range restored {
		keep[path] = true
		keep[backup.PartialPath(path)] = true
	}

	folded := make(map[string]string, len(keep))
	for path := range keep {
		folded[strings.ToLower(path)] = path
	}

	// kept reports whether the restore wrote path under any spelling the
	// filesystem treats as the same name
	kept := func(path string) bool {
		if keep[path] {
			return true
		}

		spelled, ok := folded[strings.ToLower(path)]
		if !ok {
			return false
		}

		found, err := os.Lstat(path)
		if err != nil {
			return false
		}

		written, err := os.Lstat(spelled)
		if err != nil {
			return false
		}

		return os.SameFile(found, written)
	}

	var stale []string

	err := filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if path == base || kept(path) {
			return nil
		}

		stale = append(stale, path)

		if d.IsDir() {
			return filepath.SkipDir
		}

		return nil
	})
	if err != nil {
		return err
	}

	for _, path := range stale {
		if dryRun {
			log.Printf("would remove: %s", path)
			continue
		}

		log.Printf("Removing %s", path)

		err = os.RemoveAll(path)
		if err != nil {
			return err
		}
	}

	return nil
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

	if !c.Bool("dry-run") {
		if err := os.MkdirAll(base, 0775); err != nil {
			common.Fatal(err)
		}
	}

	store := common.GetObjectStore(c)
	index := common.OpenIndex(c, store)
	key := common.StoreKey(c, store)

	restorer := &backup.Restorer{
		Store:     store,
		Key:       key,
		Cache:     common.BlobCache(c, key),
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
		reader:   backup.NewBackupReader(store).WithKey(key),
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

	common.CloseAll(store, index)
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

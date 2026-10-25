package job

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/hooks"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/vss"
	"github.com/gobackio/goback/storage/cache"

	"github.com/bmatcuk/doublestar"
	"github.com/dustin/go-humanize"
)

// PathSettings say how Path backs up a directory.
type PathSettings struct {
	Set string
	// Dir is the directory to back up; on the local disk, relative to the
	// working directory when not absolute.
	Dir string
	// FS is the tree Dir is read from; nil means the local disk, and on
	// any other FS Dir is taken as that FS's path.
	FS backup.FS
	// Filters decide what is backed up: a path is when every filter keeps
	// it. Globs makes one from patterns.
	Filters []Filter
	// Unchanged, when set, reports directories known to be as in the set's
	// latest commit, everything under them included, such as from a change
	// journal; the walk takes them from that commit without reading them.
	// Filters must be the ones that commit was made with.
	Unchanged func(rel string) bool
	Workers   int
	// ForceHash is the percent of unchanged files re-hashed as a check.
	ForceHash int
	// Verify reads every file, changed or not.
	Verify             bool
	CheckpointInterval time.Duration
	ScanWorkers        int
	ReadRetries        int
	LostRetries        int
	// ProgressInterval is how often the walk reports; negative never.
	ProgressInterval time.Duration
	// Progress, when set, hears the walk's counters every ProgressInterval,
	// the default when it is not positive, and once more when it ends,
	// in place of the log line.
	Progress func(backup.WalkResult)
	Meta             map[string]string
	PreHook          string
	PostHook         string
	HookTimeout      time.Duration
	// HookOptional backs up anyway when the pre hook fails.
	HookOptional bool
	// VSS reads Dir from a shadow copy taken after the pre hook, on
	// Windows; when none can be taken, the live files are read.
	VSS bool
	// DownloadPlaceholders reads files a cloud sync client keeps only in
	// the cloud, which downloads them; otherwise they are recorded without
	// content.
	DownloadPlaceholders bool
	// InitialRetention, when set, is the retention policy the set starts
	// with if this run creates it; a set that exists keeps its own.
	InitialRetention *retention.Policy
}

// Path commits the directory to the set as a diff against its latest
// commit, wrapped in the hooks. The post hook runs however the walk ends.
// A result that is Dirty committed with torn or unreadable files.
func Path(ctx context.Context, env *Env, s PathSettings) (*backup.WalkResult, error) {
	root, err := rootOf(s.FS, s.Dir)
	if err != nil {
		return nil, err
	}

	logger := env.logger()

	objects := backup.ObjectStore(env.Index)
	if env.ObjectCache != nil {
		objects = cache.New(cache.InStore(env.ObjectCache), env.Index)
	}

	walker := &backup.Walker{
		Index:                env.Index,
		Objects:              objects,
		Sessions:             env.sessions(),
		InitialRetention:     s.InitialRetention,
		Set:                  s.Set,
		AgentID:              env.AgentID,
		Metadata:             pathMeta(s.Meta),
		Key:                  env.Key,
		Logger:               logger,
		Root:                 root,
		FS:                   s.FS,
		Include:              all(s.Filters),
		Unchanged:            s.Unchanged,
		Workers:              s.Workers,
		ForceHashPercent:     s.ForceHash,
		Verify:               s.Verify,
		DownloadPlaceholders: s.DownloadPlaceholders,
		CheckpointInterval:   s.CheckpointInterval,
		ReadRetries:          s.ReadRetries,
		LostRetries:          s.LostRetries,
		PrefetchDepth:        2,
		ProgressInterval:     s.ProgressInterval,
		ScanWorkers:          s.ScanWorkers,
		BlobCache:            env.BlobCache,
	}

	switch {
	case s.Progress != nil:
		walker.Progress = s.Progress
	case walker.ProgressInterval >= 0:
		walker.Progress = logProgress(logger, time.Now())
	}

	if env.StatCache != nil {
		walker.Cache = env.StatCache
	}

	stderr, flush := env.stderr("hook")
	defer flush()

	runner := &hooks.Runner{
		Pre:     s.PreHook,
		Post:    s.PostHook,
		Timeout: s.HookTimeout,
		Stderr:  stderr,
		Logger:  logger,
	}

	preOut, err := runner.RunPre(ctx)
	flush()

	// the pre hook may have quiesced the application, even when it failed,
	// so the post hook runs however Path ends, a panic included
	post := sync.OnceFunc(func() {
		if postErr := runner.RunPost(ctx); postErr != nil {
			logger.Info(postErr.Error())
		}
	})
	defer post()

	if err != nil {
		if !s.HookOptional {
			return nil, err
		}

		logger.Info(fmt.Sprintf("%v; continuing because --hook-optional is set", err))
	}

	walker.Quiesced = runner.Pre != "" && err == nil

	if dir := hooks.Redirect(preOut); dir != "" {
		walker.Root, err = rootOf(s.FS, dir)
		if err != nil {
			return nil, err
		}

		logger.Info("Pre hook redirected the walk to " + walker.Root)
	}

	if s.VSS {
		release := shadow(ctx, logger, walker, s.FS)
		defer release()
	}

	result, walkErr := walker.Run(ctx)
	post()

	if walkErr != nil {
		return nil, walkErr
	}

	return result, nil
}

// shadow points walker at a shadow copy of its root and marks the run
// quiesced, or leaves it on the live files when no copy can be taken. The
// returned func releases the copy.
func shadow(ctx context.Context, logger *slog.Logger, walker *backup.Walker, fsys backup.FS) func() {
	if fsys != nil {
		logger.Warn("not taking a shadow copy: the directory is not on the local disk")
		return func() {}
	}

	snapshot, err := vss.Take(ctx, walker.Root)
	if err != nil {
		logger.Warn("taking a shadow copy failed, reading the live files", "err", err)
		return func() {}
	}

	walker.FS = snapshot.FS()
	walker.Quiesced = true

	return func() {
		if err := snapshot.Release(); err != nil {
			logger.Warn("releasing the shadow copy failed", "err", err)
		}
	}
}

// rootOf is dir as a walk's root: absolute on the local disk, and as given
// on any other FS, whose paths are its own.
func rootOf(fsys backup.FS, dir string) (string, error) {
	switch fsys.(type) {
	case nil, backup.LocalFS, *backup.LocalFS:
		return filepath.Abs(dir)
	}

	return dir, nil
}

// Filter reports whether the slash-separated path rel, relative to the
// backed-up directory, is backed up. A run asks once per path it lists, and
// a directory it keeps out is skipped whole.
type Filter func(rel string) bool

// all keeps what every filter keeps; nil keeps everything.
func all(filters []Filter) func(string) bool {
	if len(filters) == 0 {
		return nil
	}

	return func(rel string) bool {
		for _, keep := range filters {
			if !keep(rel) {
				return false
			}
		}

		return true
	}
}

// Globs is a Filter of doublestar patterns over the path with a leading
// slash: a path an include matches is kept even when an exclude matches
// it too, and one no exclude matches is kept. A malformed pattern matches
// nothing and is reported to logger, nil meaning slog.Default.
func Globs(logger *slog.Logger, include, exclude []string) Filter {
	if logger == nil {
		logger = slog.Default()
	}

	match := func(patterns []string, name string) bool {
		return slices.ContainsFunc(patterns, func(pat string) bool {
			ok, err := doublestar.Match(pat, name)
			if err != nil {
				logger.Warn("malformed pattern", "pattern", pat, "err", err)
			}

			return ok
		})
	}

	return func(rel string) bool {
		name := "/" + rel

		return match(include, name) || !match(exclude, name)
	}
}

// logProgress reports what a walk has covered so far. The files it has
// covered are not a share of anything yet, because the walk only learns
// what a directory holds once it reads it.
func logProgress(logger *slog.Logger, started time.Time) func(backup.WalkResult) {
	return func(r backup.WalkResult) {
		logger.Info(fmt.Sprintf("%s elapsed: %d files (%s) covered, %d read, %d reused, %s uploaded",
			time.Since(started).Round(time.Second), r.Files, humanize.Bytes(uint64(r.Bytes)),
			r.Read, r.Reused, humanize.Bytes(uint64(r.Uploaded))))
	}
}

// pathMeta is meta with the path kind, unless it names a kind of its own.
func pathMeta(meta map[string]string) map[string]string {
	meta = maps.Clone(meta)
	if meta == nil {
		meta = map[string]string{}
	}

	if meta[backup.MetaKind] == "" {
		meta[backup.MetaKind] = backup.KindPath
	}

	return meta
}

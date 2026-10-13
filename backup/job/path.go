package job

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/hooks"
	"github.com/gobackio/goback/backup/retention"
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
	// Include and Exclude are globs over paths that start with / at Dir;
	// an included path is backed up even when an exclude matches it.
	Include []string
	Exclude []string
	Workers int
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
	Meta             map[string]string
	PreHook          string
	PostHook         string
	HookTimeout      time.Duration
	// HookOptional backs up anyway when the pre hook fails.
	HookOptional bool
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
		Index:              env.Index,
		Objects:            objects,
		Sessions:           env.sessions(),
		InitialRetention:   s.InitialRetention,
		Set:                s.Set,
		AgentID:            env.AgentID,
		Metadata:           pathMeta(s.Meta),
		Key:                env.Key,
		Logger:             logger,
		Root:               root,
		FS:                 s.FS,
		Include:            includeFilter(logger, s.Include, s.Exclude),
		Workers:            s.Workers,
		ForceHashPercent:   s.ForceHash,
		Verify:             s.Verify,
		CheckpointInterval: s.CheckpointInterval,
		ReadRetries:        s.ReadRetries,
		LostRetries:        s.LostRetries,
		PrefetchDepth:      2,
		ProgressInterval:   s.ProgressInterval,
		ScanWorkers:        s.ScanWorkers,
		BlobCache:          env.BlobCache,
	}

	if walker.ProgressInterval >= 0 {
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

	if err != nil {
		if !s.HookOptional {
			// the hook may have quiesced the application before failing
			if postErr := runner.RunPost(ctx); postErr != nil {
				logger.Info(postErr.Error())
			}

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

	result, walkErr := walker.Run(ctx)

	if err := runner.RunPost(ctx); err != nil {
		logger.Info(err.Error())
	}

	if walkErr != nil {
		return nil, walkErr
	}

	return result, nil
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

// includeFilter applies the include and exclude patterns to a path relative
// to the backup root, written with a leading slash.
func includeFilter(logger *slog.Logger, includes, excludes []string) func(string) bool {
	match := func(patterns []string, name string) bool {
		for _, pat := range patterns {
			ok, err := doublestar.Match(pat, name)
			if err != nil {
				logger.Info(fmt.Sprintf("Malformed pattern: \"%s\" %v", pat, err))
			}

			if ok {
				return true
			}
		}

		return false
	}

	return func(rel string) bool {
		name := "/" + rel

		if match(includes, name) {
			return true
		}

		return !match(excludes, name)
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

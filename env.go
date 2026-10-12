package goback

import (
	"context"

	"github.com/gobackio/goback/backup/job"
)

// Caches picks the caches Env opens under Settings.CacheDir; without a
// cache directory it opens none.
type Caches struct {
	// Stats and Objects let a path backup skip unchanged files and trees.
	Stats, Objects bool
	// Trees keeps the commits and trees a log capture reads every commit.
	Trees bool
	// Blobs keeps every blob a path backup uploads, for restores to read.
	Blobs bool
}

// Env is the job environment of the repo: its store, index and key, the
// agent id, and the caches c names. Close it before the repo.
func (r *Repo) Env(ctx context.Context, c Caches) (env *job.Env, err error) {
	key, err := r.Key(ctx)
	if err != nil {
		return nil, err
	}

	logger := r.settings.logger()

	env = &job.Env{Store: r.Store, Index: r.Index, Key: key, AgentID: r.settings.AgentID, Logger: logger}

	defer func() {
		if err != nil {
			_ = env.Close()
		}
	}()

	if dir := r.StoreCache("backup"); dir != "" {
		if c.Stats {
			if env.StatCache, err = job.OpenStatCache(dir, logger); err != nil {
				return nil, err
			}
		}

		if c.Objects {
			if env.ObjectCache, err = job.OpenObjectCache(dir, logger); err != nil {
				return nil, err
			}
		}

		if c.Trees {
			if env.TreeCache, err = job.OpenTreeCache(dir); err != nil {
				return nil, err
			}
		}
	}

	if c.Blobs {
		if env.BlobCache, err = r.BlobCache(ctx); err != nil {
			return nil, err
		}
	}

	return env, nil
}

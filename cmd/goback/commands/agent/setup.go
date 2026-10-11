package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"

	"github.com/gobackio/goback/agent"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/storage/pack"
)

// globals reads the global flags, as *cli.Context does.
type globals interface {
	GlobalIsSet(name string) bool
	GlobalString(name string) string
	GlobalBoolT(name string) bool
}

// override names a store setting that a flag or variable set.
type override struct {
	Key    string `json:"key"`
	Source string `json:"source"`
}

// setup is a config file merged with the global flags.
type setup struct {
	path         string
	cfg          *agent.Config
	store        common.StoreSettings
	blobOnBackup bool
	stateDir     string
	overrides    []override
}

// storeKeys pairs each string key of the store section with its global
// flag and the flag's environment variable, if it has one.
var storeKeys = []struct {
	key, flag, env string
	file           func(*agent.Store) string
	set            func(*common.StoreSettings, string)
}{
	{"storage", "storage", "GOBACK_STORAGE", func(s *agent.Store) string { return s.Storage }, func(s *common.StoreSettings, v string) { s.Storage = v }},
	{"index", "index", "GOBACK_INDEX", func(s *agent.Store) string { return s.Index }, func(s *common.StoreSettings, v string) { s.Index = v }},
	{"store-key", "store-key", "", func(s *agent.Store) string { return s.StoreKey }, func(s *common.StoreSettings, v string) { s.StoreKey = v }},
	{"at-rest-key", "at-rest-key", "", func(s *agent.Store) string { return s.AtRestKey }, func(s *common.StoreSettings, v string) { s.AtRestKey = v }},
	{"agent-id", "agent-id", "", func(s *agent.Store) string { return s.AgentID }, func(s *common.StoreSettings, v string) { s.AgentID = v }},
	{"cache-dir", "cache-dir", "GOBACK_CACHE_DIR", func(s *agent.Store) string { return s.CacheDir }, func(s *common.StoreSettings, v string) { s.CacheDir = v }},
	{"metadata-cache-size", "metadata-cache-size", "", func(s *agent.Store) string { return s.MetadataCacheSize }, func(s *common.StoreSettings, v string) { s.MetadataCacheSize = v }},
	{"blob-cache-size", "blob-cache-size", "", func(s *agent.Store) string { return s.BlobCacheSize }, func(s *common.StoreSettings, v string) { s.BlobCacheSize = v }},
}

// source names where a flag's value came from: its variable when that is
// set to the value, else the flag.
func source(flag, env, value string) string {
	if env != "" && os.Getenv(env) == value {
		return env
	}

	return "--" + flag
}

// load reads the config file at path and merges its store section with
// the global flags: a flag or variable that is set wins over the file, and
// a key the file leaves out takes the flag's default.
func load(path string, g globals) (*setup, error) {
	if path == "" {
		return nil, errors.New("--config is required")
	}

	if g.GlobalString("set") != "" {
		return nil, errors.New("the agent takes no --set; each backup in the config names its sets")
	}

	cfg, err := agent.Load(path)
	if err != nil {
		return nil, err
	}

	s := &setup{path: path, cfg: cfg}
	file := &cfg.Store

	for _, k := range storeKeys {
		value := k.file(file)

		if g.GlobalIsSet(k.flag) || value == "" {
			flagValue := g.GlobalString(k.flag)
			if g.GlobalIsSet(k.flag) {
				s.overrides = append(s.overrides, override{k.key, source(k.flag, k.env, flagValue)})
			}

			value = flagValue
		}

		k.set(&s.store, value)
	}

	s.blobOnBackup = g.GlobalBoolT("blob-cache-on-backup")
	if g.GlobalIsSet("blob-cache-on-backup") {
		s.overrides = append(s.overrides, override{"blob-cache-on-backup", "--blob-cache-on-backup"})
	} else if file.BlobCacheOnBackup != nil {
		s.blobOnBackup = *file.BlobCacheOnBackup
	}

	switch {
	case g.GlobalIsSet("passphrase"):
		s.store.Passphrase = g.GlobalString("passphrase")
		s.overrides = append(s.overrides, override{"passphrase-file", source("passphrase", "GOBACK_PASSPHRASE", s.store.Passphrase)})
	case file.PassphraseFile != "":
		data, err := os.ReadFile(file.PassphraseFile)
		if err != nil {
			return nil, fmt.Errorf("reading store.passphrase-file: %w", err)
		}

		s.store.Passphrase = strings.TrimSpace(string(data))
	}

	if s.store.AgentID == "" {
		s.store.AgentID, _ = os.Hostname()
	}

	var errs []error

	if s.store.Storage == "" {
		errs = append(errs, errors.New("store.storage or --storage is required"))
	}

	if _, err := s.store.MetadataCacheLimit(); err != nil {
		errs = append(errs, err)
	}

	if _, err := s.store.BlobCacheLimit(); err != nil {
		errs = append(errs, err)
	}

	s.stateDir, err = cfg.StateDir(s.store.CacheDir)
	if err != nil {
		errs = append(errs, err)
	}

	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return s, nil
}

// reload reads the config file again and returns its tasks. The store and
// schedule sections stay as they were; restart names the ones that changed.
func (s *setup) reload() (tasks []*agent.Task, restart []string, err error) {
	cfg, err := agent.Load(s.path)
	if err != nil {
		return nil, nil, err
	}

	if !reflect.DeepEqual(cfg.Store, s.cfg.Store) {
		restart = append(restart, "store")
	}

	if !reflect.DeepEqual(cfg.Schedule, s.cfg.Schedule) {
		restart = append(restart, "schedule")
	}

	return cfg.Tasks(), restart, nil
}

// open opens the store, its index, its key and the caches every run
// shares. A cache another process holds is left out.
func (s *setup) open(ctx context.Context) (env *job.Env, err error) {
	logger := slog.Default()

	store, err := s.store.OpenStore()
	if err != nil {
		return nil, err
	}

	env = &job.Env{Store: store, AgentID: s.store.AgentID}

	defer func() {
		if err != nil {
			closeEnv(env)
		}
	}()

	if env.Index, err = s.store.OpenIndex(store); err != nil {
		return nil, err
	}

	if env.Key, err = s.store.Key(ctx, store); err != nil {
		return nil, err
	}

	if dir := s.store.StoreCache("backup"); dir != "" {
		if env.StatCache, err = job.OpenStatCache(dir, logger); err != nil {
			return nil, err
		}

		if env.ObjectCache, err = job.OpenObjectCache(dir, logger); err != nil {
			return nil, err
		}

		if env.TreeCache, err = job.OpenTreeCache(dir); err != nil {
			return nil, err
		}
	}

	if s.blobOnBackup {
		if env.BlobCache, err = s.store.BlobCache(env.Key); err != nil {
			return nil, err
		}
	}

	return env, nil
}

// closeEnv closes what open opened.
func closeEnv(env *job.Env) {
	if env.StatCache != nil {
		_ = env.StatCache.Close()
	}

	if env.ObjectCache != nil {
		_ = env.ObjectCache.Close()
	}

	if env.Index != nil {
		common.CloseAll(env.Store, env.Index)
		return
	}

	common.CloseStore(env.Store)
}

// afterRun does what a one-shot command does on exit, short of closing:
// compacts a pack store and trims the blob and tree caches.
func afterRun(ctx context.Context, env *job.Env) {
	if c, ok := env.Store.(interface {
		Compact(context.Context) (*pack.CompactReport, error)
	}); ok {
		report, err := c.Compact(ctx)

		switch {
		case err != nil:
			env.Logger.Warn("compaction failed", "error", err)
		case report.Rewritten > 0:
			env.Logger.Info(report.Summary())
		}
	}

	if env.BlobCache != nil {
		if removed, err := env.BlobCache.Sweep(); err != nil {
			env.Logger.Warn("sweeping the blob cache failed", "error", err)
		} else if removed > 0 {
			env.Logger.Info("evicted blobs from the cache", "count", removed)
		}
	}

	if env.TreeCache != nil {
		if _, err := env.TreeCache.Sweep(); err != nil {
			env.Logger.Warn("sweeping the tree cache failed", "error", err)
		}
	}
}

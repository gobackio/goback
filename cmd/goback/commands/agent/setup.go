package agent

import (
	"errors"
	"fmt"
	"os"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/agent"
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
	path      string
	cfg       *agent.Config
	store     agent.Store
	settings  goback.Settings
	stateDir  string
	overrides []override
}

// storeKeys pairs each string key of the store section with its global
// flag and the flag's environment variable, if it has one.
var storeKeys = []struct {
	key, flag, env string
	field          func(*agent.Store) *string
}{
	{"storage", "storage", "GOBACK_STORAGE", func(s *agent.Store) *string { return &s.Storage }},
	{"index", "index", "GOBACK_INDEX", func(s *agent.Store) *string { return &s.Index }},
	{"store-key", "store-key", "", func(s *agent.Store) *string { return &s.StoreKey }},
	{"at-rest-key", "at-rest-key", "", func(s *agent.Store) *string { return &s.AtRestKey }},
	{"agent-id", "agent-id", "", func(s *agent.Store) *string { return &s.AgentID }},
	{"cache-dir", "cache-dir", "GOBACK_CACHE_DIR", func(s *agent.Store) *string { return &s.CacheDir }},
	{"metadata-cache-size", "metadata-cache-size", "", func(s *agent.Store) *string { return &s.MetadataCacheSize }},
	{"blob-cache-size", "blob-cache-size", "", func(s *agent.Store) *string { return &s.BlobCacheSize }},
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

	s := &setup{path: path, cfg: cfg, store: cfg.Store}

	for _, k := range storeKeys {
		value := k.field(&s.store)

		if g.GlobalIsSet(k.flag) {
			*value = g.GlobalString(k.flag)
			s.overrides = append(s.overrides, override{k.key, source(k.flag, k.env, *value)})
		} else if *value == "" {
			*value = g.GlobalString(k.flag)
		}
	}

	if g.GlobalIsSet("blob-cache-on-backup") || s.store.BlobCacheOnBackup == nil {
		on := g.GlobalBoolT("blob-cache-on-backup")
		s.store.BlobCacheOnBackup = &on
	}

	if g.GlobalIsSet("blob-cache-on-backup") {
		s.overrides = append(s.overrides, override{"blob-cache-on-backup", "--blob-cache-on-backup"})
	}

	passphrase := g.GlobalIsSet("passphrase")
	if passphrase {
		s.store.PassphraseFile = ""
	}

	var errs []error

	if s.store.Storage == "" {
		errs = append(errs, errors.New("store.storage or --storage is required"))
	}

	if s.settings, err = s.store.Settings(); err != nil {
		errs = append(errs, err)
	}

	if passphrase {
		s.settings.Passphrase = g.GlobalString("passphrase")
		s.overrides = append(s.overrides, override{"passphrase-file", source("passphrase", "GOBACK_PASSPHRASE", s.settings.Passphrase)})
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

	return cfg.Tasks(), cfg.RestartSections(s.cfg), nil
}

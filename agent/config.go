// Package agent runs a machine's backups on their schedules in one
// long-running process: it reads a config file listing the backups,
// derives when each last ran from the store, and runs each kind's job
// functions against one shared, open store.
package agent

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gobackio/goback"
	"github.com/gobackio/goback/backup/retention"
	"go.yaml.in/yaml/v3"
)

// Config is a goback agent config file.
type Config struct {
	Store    Store    `yaml:"store"`
	Schedule Schedule `yaml:"schedule"`
	Backups  []Backup `yaml:"backups"`
}

// Store names the store and how to open it, under the global flags' names.
// A flag or its environment variable overrides a value set here.
type Store struct {
	Storage           string `yaml:"storage"`
	Index             string `yaml:"index"`
	StoreKey          string `yaml:"store-key"`
	AtRestKey         string `yaml:"at-rest-key"`
	PassphraseFile    string `yaml:"passphrase-file"`
	AgentID           string `yaml:"agent-id"`
	CacheDir          string `yaml:"cache-dir"`
	MetadataCacheSize string `yaml:"metadata-cache-size"`
	BlobCacheSize     string `yaml:"blob-cache-size"`
	BlobCacheOnBackup *bool  `yaml:"blob-cache-on-backup"`
}

// Schedule says how the agent runs its backups.
type Schedule struct {
	// Concurrency is the most full backups running at once; log capture
	// doesn't count.
	Concurrency int `yaml:"concurrency"`
	// ShutdownGrace is how long running backups get after SIGTERM.
	ShutdownGrace time.Duration `yaml:"shutdown-grace"`
	// StateDir holds the status file and the lock; <cache-dir>/schedule
	// when empty.
	StateDir string `yaml:"state-dir"`
}

// Backup is one thing to protect: exactly one of Path, Postgres and MySQL
// is set.
type Backup struct {
	// Name is the set a path backup writes, and the group a database
	// backup's sets go under; slashes nest it in groups, as in srv1/db.
	Name string `yaml:"name"`
	// Every and Cron say when full backups run; the kind's default when
	// both are empty.
	Every time.Duration `yaml:"every"`
	Cron  string        `yaml:"cron"`
	// Jitter moves every full backup of this agent's backup by the same
	// offset inside [0, Jitter).
	Jitter time.Duration `yaml:"jitter"`
	// Timeout cancels a full backup run after this long; zero never does.
	Timeout time.Duration `yaml:"timeout"`
	// Retention is the retention policy a path backup's set, or a database
	// backup's base or dump set, starts with when the backup's first run
	// creates it, written as goback set retention shows one:
	// "keep-last=1 keep=daily=30 keep=monthly". A set that exists keeps its
	// own. Without it a path set starts with none and a base or dump set
	// with retention.DatabaseBase; a WAL or binlog set always starts with
	// retention.DatabaseLog.
	Retention string `yaml:"retention"`

	Path     *PathBackup     `yaml:"path"`
	Postgres *PostgresBackup `yaml:"postgres"`
	MySQL    *MySQLBackup    `yaml:"mysql"`
}

// Load reads and validates the config file at path.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cfg, err := Decode(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	return cfg, nil
}

// Decode reads a config strictly, refusing unknown keys, fills in the
// defaults and validates it, reporting every problem it finds.
func Decode(r io.Reader) (*Config, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	cfg := &Config{Schedule: Schedule{Concurrency: 2, ShutdownGrace: 30 * time.Second}}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	var errs []error

	errs = append(errs, c.Store.validate()...)

	if c.Schedule.Concurrency < 1 {
		errs = append(errs, errors.New("schedule.concurrency must be at least 1"))
	}

	if c.Schedule.ShutdownGrace < 0 {
		errs = append(errs, errors.New("schedule.shutdown-grace must not be negative"))
	}

	if len(c.Backups) == 0 {
		errs = append(errs, errors.New("backups lists nothing to back up"))
	}

	names := map[string]bool{}

	for i := range c.Backups {
		b := &c.Backups[i]

		label := b.Name
		if label == "" {
			label = fmt.Sprintf("backups[%d]", i)
		}

		for _, err := range b.validate() {
			errs = append(errs, fmt.Errorf("backup %s: %w", label, err))
		}

		if b.Name == "" {
			continue
		}

		if names[b.Name] {
			errs = append(errs, fmt.Errorf("backup %s: the name is used twice", b.Name))
		}

		names[b.Name] = true
	}

	if len(errs) == 0 {
		errs = append(errs, c.checkSets()...)
	}

	return errors.Join(errs...)
}

func validName(name string) bool {
	if strings.TrimSpace(name) != name || strings.Contains(name, `\`) {
		return false
	}

	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}

	return true
}

// checkSets refuses two backups writing one set, and a set named under a
// WAL or binlog set, whose spool directory would hold the other's.
func (c *Config) checkSets() []error {
	var errs []error

	tasks := c.Tasks()
	by := map[string]*Task{}

	for _, t := range tasks {
		if o, ok := by[t.Set]; ok {
			errs = append(errs, fmt.Errorf("backups %s and %s both write the set %s", o.Backup, t.Backup, t.Set))
		}

		by[t.Set] = t
	}

	for _, logTask := range tasks {
		if logTask.Serve == nil {
			continue
		}

		for _, t := range tasks {
			if strings.HasPrefix(t.Set, logTask.Set+"/") {
				errs = append(errs, fmt.Errorf("backup %s: the set %s lies under %s, the log set of backup %s", t.Backup, t.Set, logTask.Set, logTask.Backup))
			}
		}
	}

	return errs
}

func (s *Store) validate() []error {
	var errs []error

	if s.Storage != "" {
		u, err := url.Parse(s.Storage)
		if err == nil && strings.HasPrefix(u.Scheme, "goback") && u.User != nil {
			errs = append(errs, errors.New("store.storage carries an api key; put it in a file and name it with ?key-file="))
		}
	}

	return errs
}

func (b *Backup) validate() []error {
	var errs []error

	if b.Name == "" {
		errs = append(errs, errors.New("name is required"))
	} else if !validName(b.Name) {
		errs = append(errs, fmt.Errorf("name %q must be parts joined by single slashes, none empty, . or .., without backslashes or surrounding spaces", b.Name))
	}

	kinds := b.kinds()
	switch len(kinds) {
	case 0:
		errs = append(errs, fmt.Errorf("names no kind; set one of %s", strings.Join(kindKeys, ", ")))
	case 1:
		errs = append(errs, kinds[0].validate(b)...)
	default:
		errs = append(errs, fmt.Errorf("names %d kinds; set exactly one of %s", len(kinds), strings.Join(kindKeys, ", ")))
	}

	if b.Every != 0 && b.Cron != "" {
		errs = append(errs, errors.New("every and cron are both set; set one"))
	}

	if b.Every < 0 {
		errs = append(errs, errors.New("every must be positive"))
	}

	if b.Cron != "" {
		if _, err := Cron(b.Cron, time.Local); err != nil {
			errs = append(errs, err)
		}
	}

	if b.Jitter < 0 || b.Timeout < 0 {
		errs = append(errs, errors.New("jitter and timeout must not be negative"))
	}

	if b.Retention != "" {
		if _, err := retention.ParsePolicy(b.Retention); err != nil {
			errs = append(errs, fmt.Errorf("retention: %w", err))
		}
	}

	return errs
}

// initialRetention is the policy b's path, base or dump set starts with:
// its Retention, else fallback.
func (b *Backup) initialRetention(fallback *retention.Policy) *retention.Policy {
	if b.Retention == "" {
		return fallback
	}

	p, err := retention.ParsePolicy(b.Retention)
	if err != nil {
		return fallback
	}

	return &p
}

// kind is the one kind b names, or nil.
func (b *Backup) kind() kind {
	if kinds := b.kinds(); len(kinds) == 1 {
		return kinds[0]
	}

	return nil
}

// Settings is the store section as goback.Settings, with the passphrase
// read from PassphraseFile. A cache size of 0 means no bound.
func (s Store) Settings() (goback.Settings, error) {
	settings := goback.Settings{
		Storage:   s.Storage,
		Index:     s.Index,
		AgentID:   s.AgentID,
		StoreKey:  s.StoreKey,
		AtRestKey: s.AtRestKey,
		CacheDir:  s.CacheDir,
	}

	if settings.AgentID == "" {
		settings.AgentID, _ = os.Hostname()
	}

	var errs []error

	if s.PassphraseFile != "" {
		data, err := os.ReadFile(s.PassphraseFile)
		if err != nil {
			errs = append(errs, fmt.Errorf("reading store.passphrase-file: %w", err))
		}

		settings.Passphrase = strings.TrimSpace(string(data))
	}

	var err error

	if settings.MetadataCacheSize, err = cacheSize("metadata-cache-size", s.MetadataCacheSize); err != nil {
		errs = append(errs, err)
	}

	if settings.BlobCacheSize, err = cacheSize("blob-cache-size", s.BlobCacheSize); err != nil {
		errs = append(errs, err)
	}

	return settings, errors.Join(errs...)
}

// cacheSize parses a size such as 4GB into what goback.Settings takes:
// 0 when unset, -1 for no bound.
func cacheSize(key, value string) (int64, error) {
	if value == "" {
		return 0, nil
	}

	size, err := humanize.ParseBytes(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}

	if size == 0 {
		return -1, nil
	}

	return int64(size), nil
}

// Caches is the caches backups use: a path backup's stat, object and blob
// caches, the blob cache unless BlobCacheOnBackup turns it off, and a PITR
// database backup's tree cache.
func (s Store) Caches(backups []Backup) goback.Caches {
	var c goback.Caches

	for _, b := range backups {
		switch {
		case b.Path != nil:
			c.Stats, c.Objects = true, true
			c.Blobs = s.BlobCacheOnBackup == nil || *s.BlobCacheOnBackup
		case b.Postgres != nil && b.Postgres.PITR, b.MySQL != nil && b.MySQL.PITR:
			c.Trees = true
		}
	}

	return c
}

// RestartSections names the sections of c that differ from old and that
// a running agent keeps until it restarts; caches when c's backups use
// other caches.
func (c *Config) RestartSections(old *Config) []string {
	var sections []string

	if !reflect.DeepEqual(c.Store, old.Store) {
		sections = append(sections, "store")
	}

	if !reflect.DeepEqual(c.Schedule, old.Schedule) {
		sections = append(sections, "schedule")
	}

	if c.Store.Caches(c.Backups) != old.Store.Caches(old.Backups) {
		sections = append(sections, "caches")
	}

	return sections
}

// StateDir is where the status file and the lock go: the config's
// state-dir, else <cacheDir>/schedule.
func (c *Config) StateDir(cacheDir string) (string, error) {
	if c.Schedule.StateDir != "" {
		return c.Schedule.StateDir, nil
	}

	if cacheDir == "" {
		return "", errors.New("schedule.state-dir is required without a cache-dir")
	}

	return filepath.Join(cacheDir, "schedule"), nil
}

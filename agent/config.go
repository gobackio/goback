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
	"strings"
	"time"

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
	sets := map[string]string{}

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
			continue
		}

		names[b.Name] = true

		k := b.kind()
		if k == nil {
			continue
		}

		for _, t := range k.tasks(b) {
			if other, ok := sets[t.Set]; ok {
				errs = append(errs, fmt.Errorf("backup %s: set %s is also backup %s's; one backup per set", b.Name, t.Set, other))
				continue
			}

			sets[t.Set] = b.Name
		}
	}

	return errors.Join(errs...)
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
	} else if strings.ContainsAny(b.Name, "/\\") || strings.TrimSpace(b.Name) != b.Name {
		errs = append(errs, fmt.Errorf("name %q must not hold slashes or surrounding spaces", b.Name))
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

	return errs
}

// kind is the one kind b names, or nil.
func (b *Backup) kind() kind {
	if kinds := b.kinds(); len(kinds) == 1 {
		return kinds[0]
	}

	return nil
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

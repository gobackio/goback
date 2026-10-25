package agent

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// flags are global flags as set on a command line; a name with no value
// is unset and reads as its default.
type flags struct {
	set      map[string]string
	defaults map[string]string
}

func (f flags) GlobalIsSet(name string) bool {
	_, ok := f.set[name]
	return ok
}

func (f flags) GlobalString(name string) string {
	if v, ok := f.set[name]; ok {
		return v
	}

	return f.defaults[name]
}

func (f flags) GlobalBoolT(name string) bool {
	v, ok := f.set[name]

	return !ok || v == "true"
}

func cliDefaults(set map[string]string) flags {
	return flags{set: set, defaults: map[string]string{"metadata-cache-size": "1GiB", "blob-cache-size": "4GB"}}
}

func writeConfig(t *testing.T, dir, doc string) string {
	t.Helper()

	path := filepath.Join(dir, "goback.yaml")
	require.NoError(t, os.WriteFile(path, []byte(doc), 0o600))

	return path
}

const twoPaths = `
store:
  storage: /from/file
  index: /index/from/file
  cache-dir: /cache
  metadata-cache-size: 2GiB
backups:
  - {name: a, every: 1h, path: {dir: /a}}
  - {name: b, every: 1h, path: {dir: /b}}
`

func TestAFlagOverridesTheFileAndIsNamed(t *testing.T) {
	path := writeConfig(t, t.TempDir(), twoPaths)

	s, err := load(path, cliDefaults(map[string]string{"storage": "/from/flag", "agent-id": "node-1"}))
	require.NoError(t, err)

	require.Equal(t, "/from/flag", s.store.Storage)
	require.Equal(t, "/index/from/file", s.store.Index)
	require.Equal(t, "node-1", s.store.AgentID)
	require.Equal(t, "2GiB", s.store.MetadataCacheSize)
	require.Equal(t, "4GB", s.store.BlobCacheSize)
	require.True(t, s.store.Caches(s.cfg.Backups).Blobs)
	require.Equal(t, filepath.Join("/cache", "schedule"), s.stateDir)

	var keys []string
	for _, o := range s.overrides {
		keys = append(keys, o.Key)
	}

	require.ElementsMatch(t, []string{"storage", "agent-id"}, keys)
}

func TestAVariableIsNamedAsTheSource(t *testing.T) {
	path := writeConfig(t, t.TempDir(), twoPaths)
	t.Setenv("GOBACK_STORAGE", "/from/env")

	s, err := load(path, cliDefaults(map[string]string{"storage": "/from/env"}))
	require.NoError(t, err)
	require.Equal(t, []override{{"storage", "GOBACK_STORAGE"}}, s.overrides)
}

func TestThePassphraseFlagOverridesThePassphraseFile(t *testing.T) {
	dir := t.TempDir()
	secret := filepath.Join(dir, "passphrase")
	require.NoError(t, os.WriteFile(secret, []byte("from file\n"), 0o600))

	path := writeConfig(t, dir, `
store: {storage: /s, cache-dir: /c, passphrase-file: `+filepath.ToSlash(secret)+`}
backups: [{name: a, every: 1h, path: {dir: /a}}]
`)

	s, err := load(path, cliDefaults(nil))
	require.NoError(t, err)
	require.Equal(t, "from file", s.settings.Passphrase)

	s, err = load(path, cliDefaults(map[string]string{"passphrase": "from flag"}))
	require.NoError(t, err)
	require.Equal(t, "from flag", s.settings.Passphrase)
	require.Len(t, s.overrides, 1)

	other := filepath.Join(dir, "other")
	require.NoError(t, os.WriteFile(other, []byte("from the flag's file\n"), 0o600))

	s, err = load(path, cliDefaults(map[string]string{"passphrase-file": other}))
	require.NoError(t, err)
	require.Equal(t, "from the flag's file", s.settings.Passphrase)
	require.Equal(t, []override{{"passphrase-file", "--passphrase-file"}}, s.overrides)

	s, err = load(path, cliDefaults(map[string]string{"passphrase-file": other, "passphrase": "from flag"}))
	require.NoError(t, err)
	require.Equal(t, "from flag", s.settings.Passphrase)
	require.Len(t, s.overrides, 1)
}

func TestTheFileSetsBlobCacheOnBackupUnlessTheFlagDoes(t *testing.T) {
	path := writeConfig(t, t.TempDir(), `
store: {storage: /s, cache-dir: /c, blob-cache-on-backup: false}
backups: [{name: a, every: 1h, path: {dir: /a}}]
`)

	s, err := load(path, cliDefaults(nil))
	require.NoError(t, err)
	require.False(t, s.store.Caches(s.cfg.Backups).Blobs)

	s, err = load(path, cliDefaults(map[string]string{"blob-cache-on-backup": "true"}))
	require.NoError(t, err)
	require.True(t, s.store.Caches(s.cfg.Backups).Blobs)
}

func TestLoadRefuses(t *testing.T) {
	dir := t.TempDir()

	for name, c := range map[string]struct {
		doc   string
		flags map[string]string
	}{
		"the global --set": {twoPaths, map[string]string{"set": "x"}},
		"no storage": {`
backups: [{name: a, every: 1h, path: {dir: /a}}]
schedule: {state-dir: /state}`, nil},
		"no state dir": {`
store: {storage: /s}
backups: [{name: a, every: 1h, path: {dir: /a}}]`, nil},
		"a bad cache size": {twoPaths, map[string]string{"blob-cache-size": "lots"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := load(writeConfig(t, dir, c.doc), cliDefaults(c.flags))
			require.Error(t, err)
		})
	}
}

func TestReloadKeepsTheStoreAndNamesWhatNeedsARestart(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, dir, twoPaths)

	s, err := load(path, cliDefaults(nil))
	require.NoError(t, err)

	tasks, restart, err := s.reload()
	require.NoError(t, err)
	require.Empty(t, restart)
	require.Len(t, tasks, 2)

	writeConfig(t, dir, `
store: {storage: /elsewhere, cache-dir: /cache}
schedule: {concurrency: 4}
backups:
  - {name: a, every: 1h, path: {dir: /a}}
`)

	tasks, restart, err = s.reload()
	require.NoError(t, err)
	require.Equal(t, []string{"store", "schedule"}, restart)
	require.Len(t, tasks, 1)
	require.Equal(t, "/from/file", s.store.Storage)

	writeConfig(t, dir, "backups: [{name: a}]")

	_, _, err = s.reload()
	require.Error(t, err)
}

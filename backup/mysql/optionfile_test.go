package mysql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeOptionFile(t *testing.T, path, content string) string {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

func TestOptionFilesReadTheClientGroupWithQuotesEscapesAndComments(t *testing.T) {
	dir := t.TempDir()
	extra := writeOptionFile(t, filepath.Join(dir, "extra.cnf"), `
# a comment
; another
[mysqld]
user = mysql

[Client]
host = db.example # the database
port=3307
user = "goback"
password = 'p#ss\sword\\x\S'
loose_ssl_mode = required
socket
`)

	o, err := readOptions(extra, "")
	require.NoError(t, err)
	require.Equal(t, &Options{Host: "db.example", Port: 3307, User: "goback", Password: `p#ss word\x\S`, SSLMode: "REQUIRED"}, o)
}

func TestTheUsersOptionFileOverridesTheExtraOne(t *testing.T) {
	home := t.TempDir()
	extra := writeOptionFile(t, filepath.Join(t.TempDir(), "extra.cnf"), "[client]\nuser=extra\npassword=secret\n")
	writeOptionFile(t, filepath.Join(home, ".my.cnf"), "[client]\nuser=home\n")

	o, err := readOptions(extra, home)
	require.NoError(t, err)
	require.Equal(t, "home", o.User)
	require.Equal(t, "secret", o.Password)
}

func TestOptionFilesFollowIncludes(t *testing.T) {
	dir := t.TempDir()
	writeOptionFile(t, filepath.Join(dir, "conf.d", "a.cnf"), "[client]\nhost=from-dir\n")
	writeOptionFile(t, filepath.Join(dir, "conf.d", "ignored.txt"), "[client]\nhost=ignored\n")
	writeOptionFile(t, filepath.Join(dir, "password.cnf"), "[mysql]\nuser=other\n[client]\npassword=included\n")
	extra := writeOptionFile(t, filepath.Join(dir, "extra.cnf"), "[client]\nuser=goback\n!include "+filepath.Join(dir, "password.cnf")+"\n!includedir "+filepath.Join(dir, "conf.d")+"\n")

	o, err := readOptions(extra, "")
	require.NoError(t, err)
	require.Equal(t, &Options{Host: "from-dir", User: "goback", Password: "included"}, o)
}

func TestAnIncludeLoopFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loop.cnf")
	writeOptionFile(t, path, "!include "+path+"\n")

	_, err := readOptions(path, "")
	require.Error(t, err)
}

func TestAMissingOptionFile(t *testing.T) {
	home := t.TempDir()

	o, err := readOptions("", home)
	require.NoError(t, err, "a missing ~/.my.cnf is no error")
	require.Equal(t, &Options{}, o)

	_, err = readOptions(filepath.Join(home, "missing.cnf"), home)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestAnUnclosedQuoteFails(t *testing.T) {
	extra := writeOptionFile(t, filepath.Join(t.TempDir(), "extra.cnf"), "[client]\npassword=\"open\n")

	_, err := readOptions(extra, "")
	require.Error(t, err)
}

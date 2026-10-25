package postgres

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
)

func TestTheRestoreCommandReadsTheFlagsWithAVariableFromTheEnvFile(t *testing.T) {
	for _, env := range []string{"GOBACK_STORAGE", "GOBACK_INDEX", "GOBACK_PASSPHRASE", "GOBACK_JSON"} {
		t.Setenv(env, "")
	}

	line := []string{"goback", "--agent-id", "pg", "--storage", "goback://s3cr3t@backup:7000", "--passphrase", "correct horse", "--json",
		"postgres", "restore", "--base-set", "postgres", "postgres"}

	var (
		command string
		env     map[string]string
	)

	restore := restoreCmd
	restore.Action = func(c *cli.Context) error {
		var err error
		command, env, err = invocation(c, "/srv/pg.goback.env")

		return err
	}

	cmd := Command
	cmd.Subcommands = []cli.Command{restore}

	app := cli.NewApp()
	app.Commands = []cli.Command{cmd}
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "env-file", EnvVar: "GOBACK_ENV_FILE"},
		cli.BoolFlag{Name: "json", EnvVar: "GOBACK_JSON"},
		cli.StringFlag{Name: "storage", EnvVar: "GOBACK_STORAGE"},
		cli.StringFlag{Name: "index", EnvVar: "GOBACK_INDEX"},
		cli.StringFlag{Name: "set, s"},
		cli.StringFlag{Name: "agent-id"},
		cli.StringFlag{Name: "passphrase", EnvVar: "GOBACK_PASSPHRASE"},
		cli.StringFlag{Name: "blob-cache-size", Value: "4GB"},
	}

	require.NoError(t, app.Run(line))
	require.Equal(t, map[string]string{"GOBACK_STORAGE": "goback://s3cr3t@backup:7000", "GOBACK_PASSPHRASE": "correct horse", "GOBACK_JSON": "true"}, env)
	require.NotContains(t, command, "s3cr3t")
	require.NotContains(t, command, "horse")
	require.True(t, strings.HasSuffix(command, " --env-file '/srv/pg.goback.env' '--agent-id=pg'"), command)
}

func TestWALGetStopsRecoveryWhenItCannotReadTheCommit(t *testing.T) {
	walGetRetries = nil

	app := cli.NewApp()
	app.Commands = []cli.Command{Command}
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "storage"},
		cli.StringFlag{Name: "index"},
	}
	app.ExitErrHandler = func(*cli.Context, error) {}

	dir := t.TempDir()
	err := app.Run([]string{"goback", "--storage", filepath.Join(dir, "store"), "--index", filepath.Join(dir, "index"),
		"postgres", "wal-get", "--wal-set", "db-wal", "--commit", strings.Repeat("00", 32), "000000010000000000000001", filepath.Join(dir, "RECOVERYXLOG")})

	var coded cli.ExitCoder
	require.ErrorAs(t, err, &coded)
	require.Greater(t, coded.ExitCode(), 125)
}

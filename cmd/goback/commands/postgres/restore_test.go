package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
)

func TestGlobalArgsEndWhereTheCommandStarts(t *testing.T) {
	for _, args := range [][]string{
		{"--agent-id", "postgres", "--storage", "/srv/store", "--json"},
		{"--agent-id=postgres", "-s", "postgres", "--index", "postgres://db/index"},
		{"--storage", "postgres"},
		{},
	} {
		line := append(append([]string{"goback"}, args...), "postgres", "restore", "--base-set", "postgres", "postgres")

		got := []string{"not run"}

		restore := restoreCmd
		restore.Action = func(c *cli.Context) error {
			got = globalArgs(c, line)
			return nil
		}

		cmd := Command
		cmd.Subcommands = []cli.Command{restore}

		app := cli.NewApp()
		app.Commands = []cli.Command{cmd}
		app.Flags = []cli.Flag{
			cli.BoolFlag{Name: "json"},
			cli.StringFlag{Name: "storage"},
			cli.StringFlag{Name: "index"},
			cli.StringFlag{Name: "set, s"},
			cli.StringFlag{Name: "agent-id"},
		}

		require.NoError(t, app.Run(line))
		require.Equal(t, args, append([]string{}, got...), "%q", args)
	}
}

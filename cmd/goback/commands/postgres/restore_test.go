package postgres

import (
	"context"
	"testing"

	coresql "github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage"
	"github.com/gobackio/goback/storage/pack"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
	"gocloud.dev/blob/memblob"
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

func TestLatestByIDReadsThatSetsHead(t *testing.T) {
	ctx := context.Background()

	index := coresql.NewMemory(t.Name(), nil)
	packs, err := pack.NewPackStorage(pack.WithArchiveStorage(storage.NewBucketStore(memblob.OpenBucket(nil))), pack.WithArchiveIndex(index))
	require.NoError(t, err)
	index.ObjectStore = packs
	require.NoError(t, index.Open())
	require.NoError(t, packs.Open())
	t.Cleanup(func() { _ = packs.Close() })

	tree := proto.NewObject(&proto.Tree{})
	require.NoError(t, packs.Put(ctx, tree))

	commit := func(setID uint64, received int64) *proto.Ref {
		obj := proto.NewObject(&proto.Commit{Tree: tree.Ref(), BackupSet: "control-base", SetId: setID, ReceivedAtNs: received})
		require.NoError(t, packs.Put(ctx, obj))
		return obj.Ref()
	}

	ours := commit(3, 10)
	commit(4, 20)

	ref, err := latest(index, packs)(ctx, "3")
	require.NoError(t, err)
	require.Equal(t, ours, ref)

	_, err = latest(index, packs)(ctx, "5")
	require.Error(t, err)
}

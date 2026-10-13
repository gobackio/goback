package file

import (
	"log"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/proto"

	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

func (f *file) list() error {
	lister, ok := f.index.(backup.DirLister)
	if !ok {
		return errors.Errorf("index %T lists no directories", f.index)
	}

	entries, err := backup.ListDir(f.ctx, lister, f.key, f.set, f.src, f.when)
	if err != nil {
		return err
	}

	out := make([]views.NodeView, len(entries))
	for i, entry := range entries {
		out[i] = common.View.Node(entry)
	}

	if common.JSON() {
		common.Result(out, nil)

		return nil
	}

	if len(entries) == 0 {
		log.Print("Nothing there at that time")

		return nil
	}

	for _, entry := range entries {
		info := entry.Stat

		switch info.GetType() {
		case proto.NodeType_NODE_DIRECTORY:
			log.Printf("%s/", info.GetName())
		case proto.NodeType_NODE_SYMLINK:
			log.Printf("%s -> %s", info.GetName(), info.GetLinkTarget())
		default:
			log.Printf("%s size: %d timestamp: %v", info.GetName(), info.GetSize(), info.ModTime())
		}
	}

	return nil
}

func lsAction(c *cli.Context) error {
	age := c.Args().Get(1)
	if age == "" {
		age = "0"
	}

	d, err := time.ParseDuration(age)
	if err != nil {
		return errors.Wrap(err, "Failed parsing <age> parameter: %v")
	}

	repo := common.OpenRepo(c)
	idx := repo.Index

	f := &file{
		ctx:   common.Context(c),
		key:   common.StoreKey(c, repo),
		src:   c.Args().Get(0),
		index: idx,
		when:  time.Now().Add(-d),
		set:   c.GlobalString("set"),
	}

	if err := f.list(); err != nil {
		common.Fatal(err)
	}

	if err := idx.Close(); err != nil {
		common.Fatal(err)
	}

	return nil
}

var lsCmd = cli.Command{
	Name:      "ls",
	Usage:     "List a directory as it is in the set now, or as it was <age> ago",
	ArgsUsage: "<directory> [age]",
	Action:    lsAction,
}

package file

import (
	"log"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"

	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

func (f *file) show() error {
	nodes, err := backup.FindFile(f.ctx, f.index, f.key, f.set, f.src, f.when, 10)
	if err != nil {
		return err
	}

	if common.JSON() {
		out := make([]views.NodeView, len(nodes))
		for i, node := range nodes {
			out[i] = common.View.Node(node)
		}

		common.Result(out, nil)

		return nil
	}

	if len(nodes) == 0 {
		log.Print("File not found in index")

		return nil
	}

	log.Println("Listing all versions for file:", nodes[0].Stat.Name)

	for _, node := range nodes {
		info := node.Stat
		log.Printf("dir: %v size: %d timestamp: %v", info.IsDir(), info.Size, info.ModTime())
	}

	return nil
}

func showAction(c *cli.Context) error {
	src := c.Args().Get(0)
	age := c.Args().Get(1)
	if age == "" {
		age = "0"
	}

	d, err := time.ParseDuration(age)
	if err != nil {
		return errors.Wrap(err, "Failed parsing <age> parameter: %v")
	}

	when := time.Now().Add(-d)

	repo := common.OpenRepo(c)
	idx := repo.Index

	f := &file{
		ctx:   common.Context(c),
		key:   common.StoreKey(c, repo),
		src:   src,
		index: idx,
		when:  when,
		set:   c.GlobalString("set"),
	}

	if err := f.show(); err != nil {
		common.Fatal(err)
	}

	if err := idx.Close(); err != nil {
		common.Fatal(err)
	}

	return nil
}

var showCmd = cli.Command{
	Name:      "show",
	Usage:     "Show information about a file",
	ArgsUsage: "<path> [age]",
	Action:    showAction,
}

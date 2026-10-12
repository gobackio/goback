package object

import (
	"context"
	"fmt"
	"log"

	"github.com/gobackio/goback/backup"

	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/proto"

	"github.com/urfave/cli"
)

func (o *object) count() {
	if counter, ok := o.store.(backup.Counter); ok {
		total, unique, err := counter.Count()
		if err == nil {
			common.Result(struct {
				Total  uint64 `json:"total"`
				Unique uint64 `json:"unique"`
			}{total, unique}, func() { log.Printf("Counted %d total and %d unique objects", total, unique) })

			return
		}

		log.Println(fmt.Errorf("couldn't count objects, using fallback: %w", err))
	}

	var count uint64

	err := o.store.Walk(context.Background(), false, proto.ObjectType_INVALID, func(p *proto.Object) error {
		count++

		return nil
	})

	if err != nil {
		common.Fatal(err)
	}

	common.Result(struct {
		Total uint64 `json:"total"`
	}{count}, func() { log.Printf("Counted %d objects", count) })
}

func countAction(c *cli.Context) {
	repo := common.OpenRepo(c)
	store := repo.Store

	o := &object{
		store: store,
	}

	o.count()
}

var countCmd = cli.Command{
	Name:   "count",
	Usage:  "Count all objects",
	Action: countAction,
}

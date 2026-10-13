package repair

import (
	"context"
	"errors"
	"log"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/cmd/goback/commands/common"
	"github.com/gobackio/goback/cmd/goback/commands/common/views"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/maintenance"

	"github.com/urfave/cli"
)

// Command is the repair command.
var Command = cli.Command{
	Name:   "repair",
	Usage:  "Rewrite each archive that holds corrupt objects, without the corrupt objects",
	Action: repairAction,
}

func repairAction(c *cli.Context) {
	ctx := common.Context(c)

	repo := common.OpenRepo(c)
	defer common.CloseRepo(repo)

	report, damage, err := maintenance.Repair(ctx, repo.Store, repo.Index)
	if errors.Is(err, errors.ErrUnsupported) {
		common.Fatalf("storage %s cannot be repaired", c.GlobalString("storage"))
	}

	if err != nil {
		common.Fatal(err)
	}

	logDamage(ctx, repo.Index, report.Lost, damage)

	hexes := func(refs []*proto.Ref) []string {
		out := make([]string, len(refs))
		for i, ref := range refs {
			out[i] = views.Hex(ref)
		}

		return out
	}

	common.Result(struct {
		Objects   uint64   `json:"objects"`
		Archives  uint64   `json:"archives"`
		Rewrote   uint64   `json:"rewrote"`
		Recovered []string `json:"recovered"`
		Lost      []string `json:"lost"`
	}{report.Scrub.Objects, report.Scrub.Archives, report.Archives, hexes(report.Recovered), hexes(report.Lost)}, func() {
		log.Printf("Scrubbed %d objects in %d archives, rewrote %d", report.Scrub.Objects, report.Scrub.Archives, report.Archives)

		for _, ref := range report.Recovered {
			log.Printf("recovered %x from another archive", ref.GetHash())
		}

		for _, ref := range report.Lost {
			log.Printf("lost %x", ref.GetHash())
		}
	})
}

// logDamage says what recording the damage of the lost objects did.
func logDamage(ctx context.Context, idx backup.Index, lost []*proto.Ref, damage *backup.DamageReport) {
	if len(lost) == 0 {
		return
	}

	if damage == nil {
		log.Printf("index %T cannot record damage, so nothing will read the lost objects again", idx)
		return
	}

	names := map[int64]string{}
	if lister, ok := idx.(maintenance.SetLister); ok {
		sets, _ := lister.ListSets(ctx)
		for _, s := range sets {
			names[s.ID] = s.Name
		}
	}

	for set, paths := range damage.Paths {
		log.Printf("set %s: %d paths will be read again by the next backup", names[set], len(paths))
	}

	for _, at := range damage.Lost {
		if at.Open {
			continue
		}

		log.Printf("set %s: %s is unrecoverable in the snapshots that hold this version", at.Set, at.Path)
	}

	if len(damage.Rescan) > 0 {
		log.Printf("%d objects could not be placed at a path, so %d sets will be read in full", len(damage.Unplaced), len(damage.Rescan))
	}
}

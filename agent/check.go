package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gobackio/goback/backup"
)

// Planned is one task as Check finds it.
type Planned struct {
	Name   string `json:"name"`
	Backup string `json:"backup"`
	Set    string `json:"set"`
	Kind   string `json:"kind"`
	// Plan is the task's schedule, "always" for one that runs all the time.
	Plan    string    `json:"plan"`
	LastRun time.Time `json:"last_run,omitzero"`
	NextDue time.Time `json:"next_due,omitzero"`
}

// DatabaseCheck is what a backup's database lacks for the backup.
type DatabaseCheck struct {
	Backup   string   `json:"backup"`
	Problems []string `json:"problems"`
}

// Checked is what Check found.
type Checked struct {
	Jobs      []Planned       `json:"jobs"`
	Databases []DatabaseCheck `json:"databases"`
}

// Check reads when each of the config's tasks last ran from idx and works
// out when it is next due for agent, and connects to each backup's
// database to list what it lacks. The error joins every task it could not
// read and every problem found; what was found is returned either way.
func (c *Config) Check(ctx context.Context, idx backup.Index, agent string, now time.Time) (Checked, error) {
	var (
		checked Checked
		errs    []error
	)

	for _, t := range c.Tasks() {
		p := Planned{Name: t.Name, Backup: t.Backup, Set: t.Set, Kind: t.Kind}

		switch {
		case t.Plan != nil:
			p.Plan = t.Plan.String()
		case t.Serve != nil:
			p.Plan = "always"
		}

		last, _, err := LastRun(ctx, idx, t.Set, now)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: reading the last run: %w", t.Name, err))
		} else {
			p.LastRun = last
			p.NextDue = t.NextDue(agent, last, now)
		}

		checked.Jobs = append(checked.Jobs, p)
	}

	for i := range c.Backups {
		b := &c.Backups[i]

		problems := b.CheckDatabase(ctx)
		if problems == nil {
			continue
		}

		checked.Databases = append(checked.Databases, DatabaseCheck{Backup: b.Name, Problems: problems})

		for _, p := range problems {
			errs = append(errs, fmt.Errorf("%s: %s", b.Name, p))
		}
	}

	return checked, errors.Join(errs...)
}

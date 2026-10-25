package agent

import (
	"context"
	"time"

	"github.com/gobackio/goback/backup/schedule"
)

// Expectations is what tasks expect of their sets, as of now: a task on a
// plan a snapshot within its plan's LongestGap and Jitter, of kind every or
// cron, and a Serve task with a CommitEvery a receiver's commits. A task
// with neither expects nothing.
func Expectations(tasks []*Task, now time.Time) []schedule.Expectation {
	var out []schedule.Expectation

	for _, t := range tasks {
		switch {
		case t.Plan != nil:
			kind := schedule.Every
			if t.Plan.cron != nil {
				kind = schedule.Cron
			}

			out = append(out, schedule.Expectation{Set: t.Set, Kind: kind, LongestGap: t.Plan.LongestGap(now), Jitter: t.Jitter})
		case t.Serve != nil && t.CommitEvery > 0:
			out = append(out, schedule.Expectation{Set: t.Set, Kind: schedule.Receiver, CommitEvery: t.CommitEvery})
		}
	}

	return out
}

// ReportSchedule reports to the store what tasks expect of their sets as
// agent's, replacing what agent reported before. A store that takes no
// reports answers backup.ErrNotImplemented.
func ReportSchedule(ctx context.Context, to schedule.Reporter, agent string, tasks []*Task) error {
	return to.ReportSchedule(ctx, schedule.Report{Agent: agent, Sets: Expectations(tasks, time.Now())})
}

package agent

import (
	"context"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/retention"
)

// RunFunc runs a task once. A nil result with a nil error means there was
// nothing to commit.
type RunFunc func(ctx context.Context, env *job.Env) (*backup.WalkResult, error)

// ServeFunc runs a task that runs all the time, committing on its own
// timer, until ctx ends or c.Stop is closed. After Stop it makes its final
// commit and returns; ctx ends when the shutdown grace runs out.
type ServeFunc func(ctx context.Context, env *job.Env, c *Control) error

// Task is one thing a backup does on its own timing: its full backups, or
// its log capture. Exactly one of Run, with a Plan, and Serve is set.
type Task struct {
	// Name is unique among an agent's tasks; it names the task in logs,
	// the status file and metrics.
	Name   string
	Set    string
	Kind   string
	Backup string
	Plan   *Plan
	// Jitter is the window the task's slots move by a fixed offset in.
	Jitter time.Duration
	// Timeout cancels a run after this long; zero never does.
	Timeout time.Duration
	// StaleAfter is how long a Serve task may go without reporting itself
	// healthy before it counts as unhealthy; zero never judges it.
	StaleAfter time.Duration
	// Log marks log capture, which runs outside the concurrency limit.
	Log bool
	// After names a set that must hold a complete commit before the task
	// starts.
	After string
	// InitialRetention is the retention policy Set starts with if the
	// task's run creates it; nil for none.
	InitialRetention *retention.Policy
	// Spec is what the task was built from; a reload replaces a task whose
	// Spec changed.
	Spec  string
	Run   RunFunc
	Serve ServeFunc
	// Watch, set on a task with a Run, follows a directory for changes:
	// the agent runs the task through Watch.Run, reading only what changed
	// where it can, and also once something changed and Watch.MinGap has
	// passed since its last run started.
	Watch *Watch
}

// Watch is how a task follows its directory for changes.
type Watch struct {
	Dir string
	// MinGap is the least time from a run's start to a run on change.
	MinGap time.Duration
	// FullReadEvery is how often a run reads everything anyway; zero never.
	FullReadEvery time.Duration
	// Settings fingerprints what besides the tree decides what a run reads,
	// such as its filters.
	Settings string
	// Run runs the task once, taking the directories unchanged reports
	// from the set's latest commit, as job.PathSettings.Unchanged; a nil
	// unchanged reads everything.
	Run func(ctx context.Context, env *job.Env, unchanged func(rel string) bool) (*backup.WalkResult, error)
}

// NextDue is when the task runs next on agent, given when its last run
// started (zero for never) and the time now.
func (t *Task) NextDue(agent string, last, now time.Time) time.Time {
	if t.Plan == nil {
		return now
	}

	return t.Plan.Due(JitterOffset(agent, t.Name, t.Jitter), last, now)
}

// Control is what the agent hands a ServeFunc.
type Control struct {
	stop      <-chan struct{}
	stateDir  string
	committed func(*backup.WalkResult)
	report    func(details any, healthy bool)
}

// Stop is closed when the agent wants the task to make its last commit
// and return.
func (c *Control) Stop() <-chan struct{} {
	return c.stop
}

// Committed reports a commit the task made, for the status file and the
// metrics.
func (c *Control) Committed(result *backup.WalkResult) {
	c.committed(result)
}

// Report puts details under the task in the status file. healthy says
// the task works as it should now; the task's staleness is judged by the
// last healthy report.
func (c *Control) Report(details any, healthy bool) {
	c.report(details, healthy)
}

// StateDir is the agent's state dir, where a task may keep local state
// under a name of its own; empty when the agent has none.
func (c *Control) StateDir() string {
	return c.stateDir
}

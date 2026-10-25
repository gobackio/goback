package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// StatusFile is the name of the status file in the state dir.
const StatusFile = "status.json"

// Status is what the status file reports: the agent and each of its tasks.
// The agent writes it and never reads it.
type Status struct {
	PID       int       `json:"pid"`
	State     string    `json:"state"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	// OpenError is why the store last failed to open, until it opens.
	OpenError string      `json:"open_error,omitempty"`
	Jobs      []JobStatus `json:"jobs"`
}

// The states of an agent.
const (
	AgentStarting = "starting"
	AgentRunning  = "running"
	AgentPaused   = "paused"
	AgentStopping = "stopping"
	AgentStopped  = "stopped"
)

// JobStatus is one task in the status file.
type JobStatus struct {
	Name   string `json:"name"`
	Backup string `json:"backup"`
	Set    string `json:"set"`
	Kind   string `json:"kind"`
	// State is planning, blocked, waiting, idle or running.
	State      string    `json:"state"`
	LastStart  time.Time `json:"last_start,omitzero"`
	LastResult string    `json:"last_result,omitempty"`
	LastError  string    `json:"last_error,omitempty"`
	// PlanError is why reading the set's last run from the store last
	// failed, until it is read.
	PlanError string `json:"plan_error,omitempty"`
	// LastSuccess is when the last clean run started, and LastDirty the
	// last dirty one, which committed but left out torn or unreadable
	// files; DirtyRuns counts the dirty runs since the last clean one.
	LastSuccess time.Time `json:"last_success,omitzero"`
	LastDirty   time.Time `json:"last_dirty,omitzero"`
	DirtyRuns   int       `json:"dirty_runs,omitempty"`
	LastCommit  string    `json:"last_commit,omitempty"`
	Failures    int       `json:"failures"`
	NextDue     time.Time `json:"next_due,omitzero"`
	// StaleAfter is how long without a success makes the job unhealthy.
	StaleAfter int64 `json:"stale_after_seconds,omitempty"`
	// Details are what a long-running task last reported about itself.
	Details any `json:"details,omitempty"`
}

// The states of a task.
const (
	JobPlanning = "planning"
	// JobBlocked waits for the first complete commit of the set it runs
	// after.
	JobBlocked = "blocked"
	// JobWaiting is due and waits for a free slot.
	JobWaiting = "waiting"
	JobIdle    = "idle"
	JobRunning = "running"
)

// The results of a run.
const (
	ResultOK        = "ok"
	ResultDirty     = "dirty"
	ResultFailed    = "failed"
	ResultCancelled = "cancelled"
)

// heartbeatStale is how old the status file may be before the agent counts
// as hung.
const heartbeatStale = 3 * time.Minute

// dirtyRunsUnhealthy is how many dirty runs in a row make a job unhealthy.
const dirtyRunsUnhealthy = 3

// writeStatus replaces the status file in dir atomically.
func writeStatus(dir string, s *Status) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".status-*.json")
	if err != nil {
		return err
	}

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())

		return err
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}

	if err := rename(tmp.Name(), filepath.Join(dir, StatusFile)); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}

	return nil
}

// renameTries is how often rename tries before it gives up.
const renameTries = 10

// rename renames from to to, trying again for a while when to is denied:
// on Windows a file someone is reading can't be replaced until they close
// it.
func rename(from, to string) error {
	var err error

	for try := range renameTries {
		if err = os.Rename(from, to); !errors.Is(err, fs.ErrPermission) {
			return err
		}

		time.Sleep(time.Duration(try+1) * 10 * time.Millisecond)
	}

	return err
}

// ReadStatus reads the status file in dir.
func ReadStatus(dir string) (*Status, error) {
	data, err := os.ReadFile(filepath.Join(dir, StatusFile))
	if err != nil {
		return nil, err
	}

	s := &Status{}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, fmt.Errorf("reading %s: %w", StatusFile, err)
	}

	return s, nil
}

// Problems lists what makes the agent unhealthy at now: having stopped, a
// heartbeat older than three minutes, every job whose last success is
// older than its bound and every job whose last three runs or more were
// dirty. A job that never succeeded is stale once the agent has been up
// for its bound; a blocked one is not judged.
func (s *Status) Problems(now time.Time) []string {
	var problems []string

	if s.State == AgentStopped {
		problems = append(problems, "the agent stopped")
	}

	if age := now.Sub(s.UpdatedAt); age > heartbeatStale {
		problems = append(problems, fmt.Sprintf("the agent last wrote its status %s ago", age.Round(time.Second)))
	}

	for _, j := range s.Jobs {
		if j.DirtyRuns >= dirtyRunsUnhealthy {
			problems = append(problems, fmt.Sprintf("%s left out torn or unreadable files in its last %d runs", j.Name, j.DirtyRuns))
		}

		if j.StaleAfter <= 0 || j.State == JobBlocked {
			continue
		}

		bound := time.Duration(j.StaleAfter) * time.Second

		switch {
		case !j.LastSuccess.IsZero() && now.Sub(j.LastSuccess) > bound:
			problems = append(problems, fmt.Sprintf("%s last succeeded %s ago", j.Name, now.Sub(j.LastSuccess).Round(time.Second)))
		case j.LastSuccess.IsZero() && now.Sub(s.StartedAt) > bound:
			problems = append(problems, fmt.Sprintf("%s has not succeeded in the %s the agent has been up", j.Name, now.Sub(s.StartedAt).Round(time.Second)))
		}
	}

	return problems
}

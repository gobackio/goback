package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/gobackio/goback"
	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/job"
	"github.com/gobackio/goback/backup/postgres"
)

// Agent runs tasks on their plans against one store it opens once. It
// keeps no schedule of its own: each task's last run is the scan start of
// its set's newest complete commit.
type Agent struct {
	// ID is the agent id runs commit as; it also picks each task's jitter.
	ID string
	// Settings and Caches name the store and the caches every run shares,
	// which the agent opens when Open is nil; ID, when set, is the agent id.
	Settings goback.Settings
	Caches   goback.Caches
	// Open opens the store, the index, the key and the caches that every
	// run shares. The agent retries it with backoff until it works. When
	// Open is nil, the agent opens Settings, and a nil Close and AfterRun
	// close it and run job.AfterRun.
	Open func(ctx context.Context) (*job.Env, error)
	// Close closes what Open opened, once every run has ended.
	Close func(env *job.Env)
	// AfterRun runs after each run, never two at once: compaction and cache
	// sweeps.
	AfterRun func(ctx context.Context, env *job.Env)
	// Concurrency is the most tasks running at once, log tasks aside.
	Concurrency int
	// Grace is how long running tasks get once Run's ctx ends, before they
	// are cancelled.
	Grace time.Duration
	// StateDir is where the status file goes; empty writes none.
	StateDir string
	Logger   *slog.Logger
	// Now is the wall clock; nil is time.Now.
	Now func() time.Time

	once      sync.Once
	repo      *goback.Repo
	reloadMu  sync.Mutex
	reloads   chan []*Task
	abort     chan struct{}
	abortOnce sync.Once

	controlMu sync.Mutex
	paused    bool
	runNow    map[string]bool
	wake      chan struct{}
}

func (a *Agent) init() {
	a.once.Do(func() {
		a.reloads = make(chan []*Task, 1)
		a.abort = make(chan struct{})
		a.runNow = map[string]bool{}
		a.wake = make(chan struct{}, 1)

		if a.Open == nil {
			a.Open = a.openSettings

			if a.Close == nil {
				a.Close = a.closeSettings
			}

			if a.AfterRun == nil {
				a.AfterRun = job.AfterRun
			}
		}
	})
}

func (a *Agent) openSettings(ctx context.Context) (*job.Env, error) {
	settings := a.Settings
	if a.ID != "" {
		settings.AgentID = a.ID
	}

	if settings.Logger == nil {
		settings.Logger = a.logger()
	}

	repo, err := goback.Open(ctx, settings)
	if err != nil {
		return nil, err
	}

	env, err := repo.Env(ctx, a.Caches)
	if err != nil {
		_ = repo.Close()
		return nil, err
	}

	a.repo = repo

	return env, nil
}

func (a *Agent) closeSettings(env *job.Env) {
	if err := errors.Join(env.Close(), a.repo.Close()); err != nil {
		a.logger().Warn("closing the store failed", "error", err)
	}
}

func (a *Agent) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}

	return time.Now()
}

func (a *Agent) logger() *slog.Logger {
	if a.Logger != nil {
		return a.Logger
	}

	return slog.Default()
}

// Reload replaces the tasks Run schedules. A task whose Spec is unchanged
// keeps its timing; a new or changed one is planned from the store; a run
// of a removed or changed one already going finishes as it was.
func (a *Agent) Reload(tasks []*Task) {
	a.init()

	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()

	select {
	case <-a.reloads:
	default:
	}

	a.reloads <- tasks
}

// Abort cancels running tasks at once instead of waiting for the grace,
// as a second signal does.
func (a *Agent) Abort() {
	a.init()
	a.abortOnce.Do(func() { close(a.abort) })
}

// Pause keeps Run from starting scheduled runs until Resume. Runs already
// going finish, tasks that run all the time go on, and RunNow still starts
// a run.
func (a *Agent) Pause() {
	a.control(func() { a.paused = true })
}

// Resume undoes Pause; a run that fell due meanwhile starts at once.
func (a *Agent) Resume() {
	a.control(func() { a.paused = false })
}

// RunNow starts a run of the named task at once, or as soon as the run
// already going ends. It does nothing for a task that runs all the time or
// that Run does not have.
func (a *Agent) RunNow(name string) {
	a.control(func() { a.runNow[name] = true })
}

func (a *Agent) control(change func()) {
	a.init()

	a.controlMu.Lock()
	change()
	a.controlMu.Unlock()

	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// controls returns whether the agent is paused and the tasks asked to run
// now.
func (a *Agent) controls() (bool, map[string]bool) {
	a.controlMu.Lock()
	defer a.controlMu.Unlock()

	asked := make(map[string]bool, len(a.runNow))
	for name := range a.runNow {
		asked[name] = true
	}

	return a.paused, asked
}

func (a *Agent) ranNow(name string) {
	a.controlMu.Lock()
	delete(a.runNow, name)
	a.controlMu.Unlock()
}

// Run schedules the tasks until ctx ends, then starts nothing new, gives
// running tasks the grace, cancels what is left, closes the store and
// returns. Failed runs never end it.
func (a *Agent) Run(ctx context.Context, tasks []*Task) error {
	a.init()

	l := &loop{
		a:       a,
		started: a.now(),
		done:    make(chan finished),
		events:  make(chan commitEvent),
	}

	l.runCtx, l.cancelRuns = context.WithCancel(auth.WithPrincipal(context.WithoutCancel(ctx), &auth.Principal{AgentID: a.ID}))
	defer l.cancelRuns()

	for _, t := range tasks {
		l.entries = append(l.entries, newEntry(t))
	}

	if err := l.writeStatus(); err != nil {
		return fmt.Errorf("writing the status file: %w", err)
	}

	return l.run(ctx)
}

// entry is a task and what the loop knows about it.
type entry struct {
	task *Task

	// known is set once the set's last run has been read from the store.
	known       bool
	planAt      time.Time
	planBackoff time.Duration

	// committed is set once the set holds a complete commit.
	committed bool
	// lastEnd is when the last run of this process ended.
	lastEnd time.Time
	due     time.Time

	running bool
	waiting bool
	blocked bool
	retired bool
	stop    chan struct{}
	stopped bool

	failures       int
	restartBackoff time.Duration

	status JobStatus
}

func newEntry(t *Task) *entry {
	return &entry{task: t, status: JobStatus{Name: t.Name, Backup: t.Backup, Set: t.Set, Kind: t.Kind}}
}

func (e *entry) closeStop() {
	if e.stop != nil && !e.stopped {
		e.stopped = true
		close(e.stop)
	}
}

type finished struct {
	e       *entry
	started time.Time
	ended   time.Time
	result  *backup.WalkResult
	err     error
	logger  *slog.Logger
}

// commitEvent is a Serve task's commit, or, with report set, its report.
type commitEvent struct {
	e      *entry
	at     time.Time
	result *backup.WalkResult

	report  bool
	details any
	healthy bool
}

type loop struct {
	a       *Agent
	started time.Time

	env         *job.Env
	openAt      time.Time
	openBackoff time.Duration

	entries []*entry
	// retired are entries a reload removed while they ran.
	retired []*entry

	// active counts running tasks, full the ones the limit applies to.
	active, full int
	stopping     bool
	stopped      bool

	runCtx     context.Context
	cancelRuns context.CancelFunc
	afterMu    sync.Mutex

	done   chan finished
	events chan commitEvent

	dirty     bool
	lastWrite time.Time
}

// Store backoff: 1s doubling to at most 5 minutes.
const (
	storeRetryFirst = time.Second
	storeRetryMax   = 5 * time.Minute
)

// Run backoff: 1 minute doubling to at most an hour.
const (
	runRetryFirst = time.Minute
	runRetryMax   = time.Hour
)

// Serve restart backoff: 1s doubling to at most a minute, reset by a minute
// of running.
const (
	serveRetryFirst = time.Second
	serveRetryMax   = time.Minute
)

// maxSleep bounds every wait of the loop, so a suspend or a clock step is
// noticed within a minute.
const maxSleep = time.Minute

func double(d, first, limit time.Duration) time.Duration {
	if d == 0 {
		return first
	}

	return min(2*d, limit)
}

func (l *loop) run(ctx context.Context) error {
	stopC := ctx.Done()
	abortC := (<-chan struct{})(l.a.abort)

	var graceC <-chan time.Time

	for {
		now := l.a.now()

		if !l.stopping {
			l.connect(now)
			l.plan(now)
			l.start(now)
		}

		if l.stopping && l.active == 0 {
			break
		}

		if l.dirty || now.Sub(l.lastWrite) >= maxSleep {
			if err := l.writeStatus(); err != nil {
				l.a.logger().Warn("writing the status file failed", "error", err)
			}
		}

		timer := time.NewTimer(l.wait(now))

		select {
		case f := <-l.done:
			l.finish(f)
		case c := <-l.events:
			l.committed(c)
		case <-l.a.wake:
			l.dirty = true
		case tasks := <-l.a.reloads:
			if !l.stopping {
				l.reload(tasks)
			}
		case <-stopC:
			stopC = nil
			graceC = l.stop()
		case <-graceC:
			graceC = nil

			l.a.logger().Warn("the shutdown grace ran out; cancelling the running tasks", "running", l.active)
			l.cancelRuns()
		case <-abortC:
			abortC = nil

			if !l.stopping {
				l.stop()
			}

			l.a.logger().Warn("cancelling the running tasks", "running", l.active)
			l.cancelRuns()
		case <-timer.C:
		}

		timer.Stop()
	}

	// the work after a Serve task's last commit may still run
	l.afterMu.Lock()
	l.afterMu.Unlock()

	if l.env != nil && l.a.Close != nil {
		l.a.Close(l.env)
	}

	l.stopped = true

	if err := l.writeStatus(); err != nil {
		l.a.logger().Warn("writing the status file failed", "error", err)
	}

	return nil
}

// stop starts the shutdown and returns the grace's timer.
func (l *loop) stop() <-chan time.Time {
	l.stopping = true
	l.dirty = true

	for _, e := range append(l.entries, l.retired...) {
		e.closeStop()
	}

	l.a.logger().Info("stopping", "running", l.active, "grace", l.a.Grace)

	return time.After(l.a.Grace)
}

// connect opens the store when it isn't open and its retry is due.
func (l *loop) connect(now time.Time) {
	if l.env != nil || now.Before(l.openAt) {
		return
	}

	env, err := l.a.Open(l.runCtx)
	if err != nil {
		l.openBackoff = double(l.openBackoff, storeRetryFirst, storeRetryMax)
		l.openAt = now.Add(l.openBackoff)
		l.a.logger().Warn("opening the store failed", "error", err, "retry", l.openBackoff)

		return
	}

	l.env = env
	l.dirty = true
}

// plan reads each new task's last run from the store, retrying with
// backoff, and works out when it is due.
func (l *loop) plan(now time.Time) {
	if l.env == nil {
		return
	}

	for _, e := range l.entries {
		if e.known || now.Before(e.planAt) {
			continue
		}

		ctx, cancel := context.WithTimeout(l.runCtx, time.Minute)
		last, commit, err := LastRun(ctx, l.env.Index, e.task.Set, now)
		cancel()

		if err != nil {
			e.planBackoff = double(e.planBackoff, storeRetryFirst, storeRetryMax)
			e.planAt = now.Add(e.planBackoff)
			l.a.logger().Warn("reading the last run failed", "job", e.task.Name, "set", e.task.Set, "error", err, "retry", e.planBackoff)

			continue
		}

		e.known = true
		e.committed = commit != nil
		e.lastEnd = last
		e.status.LastSuccess = last
		e.due = e.task.NextDue(l.a.ID, last, now)
		l.dirty = true

		if commit != nil && commit.GetAgentId() != l.a.ID {
			l.a.logger().Warn("another agent wrote the set's newest commit; one agent per set", "job", e.task.Name, "set", e.task.Set, "agent", commit.GetAgentId())
		}
	}
}

// ready reports whether e's After set holds a complete commit.
func (l *loop) ready(e *entry) bool {
	if e.task.After == "" {
		return true
	}

	for _, other := range l.entries {
		if other.task.Set == e.task.After {
			return other.committed
		}
	}

	return true
}

// overlaps reports whether a retired run of the same task still runs.
func (l *loop) overlaps(e *entry) bool {
	for _, r := range l.retired {
		if r.task.Name == e.task.Name {
			return true
		}
	}

	return false
}

// start launches every task that is due, oldest due first, within the
// concurrency limit.
func (l *loop) start(now time.Time) {
	if l.env == nil {
		return
	}

	var due []*entry

	paused, asked := l.a.controls()

	for _, e := range l.entries {
		e.waiting, e.blocked = false, false
		wanted := asked[e.task.Name] && e.task.Serve == nil
		delete(asked, e.task.Name)

		if !e.known || e.running || l.overlaps(e) {
			continue
		}

		if !l.ready(e) {
			e.blocked = true
			continue
		}

		if !wanted && (e.due.After(now) || paused && e.task.Serve == nil) {
			continue
		}

		if e.task.Serve != nil {
			l.serve(e, now)
			continue
		}

		due = append(due, e)
	}

	for name := range asked {
		l.a.ranNow(name)
	}

	sort.SliceStable(due, func(i, j int) bool { return due[i].due.Before(due[j].due) })

	for _, e := range due {
		if !e.task.Log && l.full >= l.a.Concurrency {
			e.waiting = true
			continue
		}

		l.launch(e, now)
		l.a.ranNow(e.task.Name)
	}
}

func (l *loop) runLogger(t *Task) *slog.Logger {
	id := make([]byte, 4)
	_, _ = rand.Read(id)

	return l.a.logger().With("job", t.Name, "set", t.Set, "kind", t.Kind, "run", hex.EncodeToString(id))
}

// launch runs e once in the background.
func (l *loop) launch(e *entry, now time.Time) {
	e.running = true
	l.active++

	if !e.task.Log {
		l.full++
	}

	e.status.LastStart = now
	l.dirty = true

	logger := l.runLogger(e.task)

	env := *l.env
	env.Logger = logger
	env.Stderr = nil

	ctx, cancel := l.runCtx, context.CancelFunc(func() {})
	if e.task.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, e.task.Timeout)
	}

	lastEnd := e.lastEnd

	go func() {
		defer cancel()

		result, err := l.runOnce(ctx, &env, e.task, lastEnd)
		ended := l.a.now()

		l.afterRun(&env)

		l.done <- finished{e: e, started: now, ended: ended, result: result, err: err, logger: logger}
	}()
}

// runOnce runs t, turning a panic into an error. It first warns when a
// commit started after this process's last run ended, which only another
// writer makes.
func (l *loop) runOnce(ctx context.Context, env *job.Env, t *Task, lastEnd time.Time) (result *backup.WalkResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			env.Logger.Error("the run panicked", "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	newest, commit, err := LastRun(ctx, env.Index, t.Set, l.a.now())
	if err != nil {
		return nil, fmt.Errorf("reading the last run: %w", err)
	}

	if commit != nil && !lastEnd.IsZero() && newest.After(lastEnd) {
		env.Logger.Warn("another writer committed to the set since this agent's last run; one agent per set", "agent", commit.GetAgentId())
	}

	return t.Run(ctx, env)
}

// afterCommit runs AfterRun after a Serve task's commit, unless one still
// runs from an earlier commit.
func (l *loop) afterCommit(env *job.Env) {
	if l.a.AfterRun == nil || !l.afterMu.TryLock() {
		return
	}

	go func() {
		defer l.afterMu.Unlock()
		l.afterRunLocked(env)
	}()
}

func (l *loop) afterRun(env *job.Env) {
	if l.a.AfterRun == nil {
		return
	}

	l.afterMu.Lock()
	defer l.afterMu.Unlock()

	l.afterRunLocked(env)
}

func (l *loop) afterRunLocked(env *job.Env) {
	defer func() {
		if r := recover(); r != nil {
			env.Logger.Error("the work after the run panicked", "panic", r, "stack", string(debug.Stack()))
		}
	}()

	l.a.AfterRun(l.runCtx, env)
}

// serve starts e, a task that runs all the time.
func (l *loop) serve(e *entry, now time.Time) {
	e.running = true
	e.stop = make(chan struct{})
	e.stopped = false
	l.active++
	e.status.LastStart = now
	l.dirty = true

	logger := l.runLogger(e.task)

	env := *l.env
	env.Logger = logger
	env.Stderr = nil

	control := &Control{
		stop:     e.stop,
		stateDir: l.a.StateDir,
		committed: func(result *backup.WalkResult) {
			l.events <- commitEvent{e: e, at: l.a.now(), result: result}

			l.afterCommit(&env)
		},
		report: func(details any, healthy bool) {
			l.events <- commitEvent{e: e, at: l.a.now(), report: true, details: details, healthy: healthy}
		},
	}

	go func() {
		err := func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("the task panicked", "panic", r, "stack", string(debug.Stack()))
					err = fmt.Errorf("panic: %v", r)
				}
			}()

			return e.task.Serve(l.runCtx, &env, control)
		}()

		l.done <- finished{e: e, started: now, ended: l.a.now(), err: err, logger: logger}
	}()
}

// outcome names how a run ended.
func (l *loop) outcome(f finished) string {
	switch {
	case f.err != nil && l.runCtx.Err() != nil:
		return ResultCancelled
	case f.err != nil:
		return ResultFailed
	case f.result != nil && f.result.Dirty():
		return ResultDirty
	default:
		return ResultOK
	}
}

func (l *loop) finish(f finished) {
	e := f.e
	now := l.a.now()

	e.running = false
	l.active--
	l.dirty = true

	if e.task.Serve != nil {
		l.finishServe(f, now)
		return
	}

	if !e.task.Log {
		l.full--
	}

	result := l.outcome(f)
	recordRun(e.task.Name, result, f.started, f.ended.Sub(f.started))

	attrs := []any{"result", result, "duration", f.ended.Sub(f.started).Round(time.Millisecond)}
	if f.err != nil {
		attrs = append(attrs, "error", f.err.Error())
	}

	if r := f.result; r != nil {
		attrs = append(attrs, "commit", hex.EncodeToString(r.Ref.GetHash()), "files", r.Files, "bytes", r.Bytes, "uploaded", r.Uploaded)
	}

	level := slog.LevelInfo
	if result == ResultFailed {
		level = slog.LevelError
	}

	f.logger.Log(context.Background(), level, "run finished", attrs...)

	e.status.LastResult = result
	e.status.LastError = ""

	if f.err != nil {
		e.status.LastError = f.err.Error()
	}

	if e.retired {
		l.dropRetired(e)
		return
	}

	switch result {
	case ResultOK, ResultDirty:
		e.failures = 0
		e.lastEnd = f.ended
		e.status.LastSuccess = f.started
		e.due = e.task.NextDue(l.a.ID, f.started, now)

		if f.result != nil {
			e.committed = true
			e.status.LastCommit = hex.EncodeToString(f.result.Ref.GetHash())
		}
	case ResultFailed:
		e.failures++
		e.lastEnd = f.ended

		e.due = now.Add(min(runRetryFirst<<min(e.failures-1, 6), runRetryMax))
		if slot := e.task.NextDue(l.a.ID, f.started, now); slot.Before(e.due) {
			e.due = slot
		}
	}

	e.status.Failures = e.failures
}

func (l *loop) finishServe(f finished, now time.Time) {
	e := f.e

	if e.retired {
		l.dropRetired(e)
		return
	}

	if l.stopping {
		return
	}

	if f.err == nil {
		f.err = errors.New("returned before it was stopped")
	}

	if errors.Is(f.err, postgres.ErrServerShutDown) {
		e.restartBackoff = serveRetryFirst
		e.due = now.Add(e.restartBackoff)
		f.logger.Info("the database shut down; streaming goes on once it is back", "retry", e.restartBackoff)

		return
	}

	e.failures++
	e.status.Failures = e.failures
	e.status.LastResult = ResultFailed
	e.status.LastError = f.err.Error()
	recordRun(e.task.Name, ResultFailed, f.started, 0)

	if now.Sub(f.started) >= serveRetryMax {
		e.restartBackoff = 0
	}

	e.restartBackoff = double(e.restartBackoff, serveRetryFirst, serveRetryMax)
	e.due = now.Add(e.restartBackoff)

	f.logger.Error("the task ended; restarting it", "error", f.err.Error(), "retry", e.restartBackoff)
}

func (l *loop) committed(c commitEvent) {
	e := c.e
	l.dirty = true

	if c.report {
		e.status.Details = c.details

		if c.healthy {
			e.status.LastSuccess = c.at
		}

		return
	}

	e.committed = true
	e.failures = 0
	e.status.Failures = 0
	e.status.LastResult = ResultOK
	e.status.LastError = ""

	// a task judged by its reports is healthy when it says so, not when it
	// commits
	if e.task.StaleAfter == 0 {
		e.status.LastSuccess = c.at
	}

	if c.result != nil {
		e.status.LastCommit = hex.EncodeToString(c.result.Ref.GetHash())
	}

	recordRun(e.task.Name, ResultOK, c.at, 0)
}

func (l *loop) dropRetired(e *entry) {
	for i, r := range l.retired {
		if r == e {
			l.retired = append(l.retired[:i], l.retired[i+1:]...)
			return
		}
	}
}

// reload swaps in a new list of tasks.
func (l *loop) reload(tasks []*Task) {
	old := map[string]*entry{}
	for _, e := range l.entries {
		old[e.task.Name] = e
	}

	var next []*entry

	var added, changed, removed []string

	for _, t := range tasks {
		e, ok := old[t.Name]
		if ok {
			delete(old, t.Name)

			if e.task.Spec == t.Spec {
				next = append(next, e)
				continue
			}

			changed = append(changed, t.Name)
			l.retire(e)
		} else {
			added = append(added, t.Name)
		}

		next = append(next, newEntry(t))
	}

	for _, e := range old {
		removed = append(removed, e.task.Name)
		l.retire(e)
	}

	sort.Strings(removed)

	l.entries = next
	l.dirty = true

	l.a.logger().Info("reloaded the config", "added", added, "changed", changed, "removed", removed)
}

func (l *loop) retire(e *entry) {
	e.retired = true

	if e.running {
		e.closeStop()
		l.retired = append(l.retired, e)
	}
}

// wait is how long the loop may sleep before something is due.
func (l *loop) wait(now time.Time) time.Duration {
	d := maxSleep

	consider := func(t time.Time) {
		if t.After(now) {
			d = min(d, t.Sub(now))
		}
	}

	if l.stopping {
		return d
	}

	if l.env == nil {
		consider(l.openAt)
		return d
	}

	for _, e := range l.entries {
		switch {
		case !e.known:
			consider(e.planAt)
		case !e.running:
			consider(e.due)
		}
	}

	return d
}

func (l *loop) state() string {
	switch {
	case l.stopped:
		return AgentStopped
	case l.stopping:
		return AgentStopping
	case l.env == nil:
		return AgentStarting
	}

	if paused, _ := l.a.controls(); paused {
		return AgentPaused
	}

	for _, e := range l.entries {
		if !e.known {
			return AgentStarting
		}
	}

	return AgentRunning
}

func (l *loop) writeStatus() error {
	now := l.a.now()
	l.dirty = false
	l.lastWrite = now

	if l.a.StateDir == "" {
		return nil
	}

	s := &Status{PID: os.Getpid(), State: l.state(), StartedAt: l.started, UpdatedAt: now}

	for _, e := range l.entries {
		j := e.status

		switch {
		case !e.known:
			j.State = JobPlanning
		case e.running:
			j.State = JobRunning
		case e.blocked:
			j.State = JobBlocked
		case e.waiting:
			j.State = JobWaiting
		default:
			j.State = JobIdle
		}

		if e.known && e.task.Plan != nil {
			since := j.LastSuccess
			if since.IsZero() {
				since = l.started
			}

			j.NextDue = e.due
			j.StaleAfter = int64(e.task.Plan.staleAfter(since) / time.Second)
		}

		if e.task.Serve != nil {
			j.StaleAfter = int64(e.task.StaleAfter / time.Second)
		}

		s.Jobs = append(s.Jobs, j)
	}

	return writeStatus(l.a.StateDir, s)
}

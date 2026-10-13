package postgres

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/lockfile"

	"github.com/jackc/pgx/v5/pgconn"
)

// The receiver's commit cadence.
const (
	// DefaultCommitEvery is how often the receiver commits what is new.
	DefaultCommitEvery = time.Minute
	// DefaultCommitBytes is how much new WAL makes it commit early.
	DefaultCommitBytes = 256 << 20
	// MinCommitEvery is the shortest interval it takes, and the least time
	// between two commits.
	MinCommitEvery = 10 * time.Second
)

const (
	// statusInterval is how often the receiver tells the server where it
	// stands, as pg_receivewal does by default.
	statusInterval  = 10 * time.Second
	monitorInterval = time.Minute
	// receiveWait bounds each wait for a message, so a finished commit is
	// noticed within it.
	receiveWait = time.Second
	// maxCommitBackoff bounds the wait after failed commits.
	maxCommitBackoff = 5 * time.Minute
)

// Receiver streams a cluster's WAL through a physical replication slot into
// a spool, and commits it to the WAL set on a timer: whole segments as they
// fill, and the segment being received up to its last whole record as
// <segment>.partial. It reports to the server as flushed only the end of
// what a stored commit holds, so the slot keeps everything else, and
// losing the receiver or its spool loses nothing the server has dropped.
type Receiver struct {
	// ConnString is the libpq connection string of the replication
	// connection and of the monitoring one, which goes to the database it
	// names, postgres by default; empty leaves both to the PG* variables.
	ConnString string
	// Slot is the physical replication slot; SlotName of the WAL set when
	// empty. The receiver creates it when missing.
	Slot string
	// AllowUncappedSlot streams even when max_slot_wal_keep_size doesn't
	// bound what the slot keeps.
	AllowUncappedSlot bool
	// Spool is where WAL waits for its commit; one receiver per spool.
	Spool Spool
	// Walker returns the walker of one commit: its Set is the WAL set and
	// its Root the spool's directory.
	Walker func() *backup.Walker
	// BaseSet is the set the cluster's base backups go to.
	BaseSet string
	// Window is how far back point-in-time recovery reaches.
	Window time.Duration
	// CommitEvery is how often new WAL is committed; DefaultCommitEvery
	// when zero, and at least MinCommitEvery.
	CommitEvery time.Duration
	// CommitBytes of new WAL commit early; DefaultCommitBytes when zero.
	CommitBytes int64
	// Logger is where the receiver reports; nil means slog.Default.
	Logger *slog.Logger
	// Committed, when set, hears of each commit once it is stored.
	Committed func(*backup.WalkResult)
	// Report, when set, hears how the receiver stands every ten seconds.
	Report func(ReceiverStatus)

	// floor is the least time between two commits; MinCommitEvery when
	// zero.
	floor time.Duration
	now   func() time.Time
	// hook, when set, is called at named points of a commit; an error
	// from it ends Run there, as a crash would.
	hook func(point string) error
}

// ReceiverStatus is how a receiver stands.
type ReceiverStatus struct {
	Timeline uint32 `json:"timeline"`
	// Received is where the WAL written to the spool ends.
	Received string `json:"received"`
	// Committed is where the WAL the newest stored commit holds ends.
	Committed string `json:"committed,omitempty"`
	// LagBytes is how much WAL the server has written that no commit
	// holds; LagSeconds how long the oldest of it has waited.
	LagBytes   int64   `json:"lag_bytes"`
	LagSeconds float64 `json:"lag_seconds"`
	SpoolBytes int64   `json:"spool_bytes"`
	// WALStatus and SafeWALSize are the slot's, as pg_replication_slots
	// shows them; SafeWALSize is -1 when nothing bounds it.
	WALStatus   string `json:"wal_status,omitempty"`
	SafeWALSize int64  `json:"safe_wal_size"`
	// RetainedBytes is how much WAL the slot keeps on the server.
	RetainedBytes int64 `json:"retained_bytes"`
	// Healthy is false while WAL has waited for a commit longer than
	// three commit intervals.
	Healthy bool `json:"-"`
}

func (r *Receiver) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}

	return slog.Default()
}

func (r *Receiver) setDefaults() error {
	if r.CommitEvery == 0 {
		r.CommitEvery = DefaultCommitEvery
	}

	if r.CommitBytes == 0 {
		r.CommitBytes = DefaultCommitBytes
	}

	if r.floor == 0 {
		r.floor = MinCommitEvery
	}

	if r.now == nil {
		r.now = time.Now
	}

	if r.CommitEvery < r.floor {
		return fmt.Errorf("committing every %s is more often than every %s", r.CommitEvery, r.floor)
	}

	return nil
}

// Run receives and commits until stop is closed, then commits what it
// received and returns nil. When the server shuts down, it commits what
// the server sent up to its shutdown at once, so the shutdown doesn't wait
// for CommitEvery, and returns ErrServerShutDown. It returns early with an
// error that needs a
// person (ErrGap, ErrForeignCluster, ErrUncappedSlot,
// ErrSynchronousStandby, ErrSlotActive) or, on any other failure, one a
// restart may clear. When ctx ends, it stops without the last commit.
func (r *Receiver) Run(ctx context.Context, stop <-chan struct{}) error {
	if err := r.setDefaults(); err != nil {
		return err
	}

	set := r.Walker().Set

	slot := r.Slot
	if slot == "" {
		slot = SlotName(set)
	}

	if err := checkSlotName(slot); err != nil {
		return err
	}

	if err := os.MkdirAll(r.Spool.Dir, 0o700); err != nil {
		return err
	}

	unlock, err := lockfile.Lock(filepath.Join(r.Spool.Dir, ".lock"))
	if errors.Is(err, lockfile.ErrLocked) {
		return fmt.Errorf("another receiver uses the spool %s", r.Spool.Dir)
	}

	if err != nil {
		return err
	}
	defer unlock()

	mon, err := connectMonitor(ctx, r.ConnString, "goback")
	if err != nil {
		return err
	}
	defer mon.close()

	settings, err := mon.settings(ctx)
	if err != nil {
		return err
	}

	if settings.version < minReceiveVersion {
		return fmt.Errorf("Postgres %s is older than 14, the oldest replication capture supports", versionString(settings.version))
	}

	if settings.keepSize < 0 {
		if !r.AllowUncappedSlot {
			return ErrUncappedSlot
		}

		r.logger().Warn("max_slot_wal_keep_size is -1: while the receiver is down or the store unreachable, the slot keeps WAL until the database's disk is full")
	}

	held, committed, err := r.setState(ctx, settings.segSize)
	if err != nil {
		return err
	}

	starts, err := r.baseStarts(ctx)
	if err != nil {
		return err
	}

	run := &receiveRun{r: r, mon: mon, slot: slot, settings: settings, held: held, committed: committed}

	err = run.session(ctx, stop, nil)

	var gap *gapError
	if !errors.As(err, &gap) {
		return err
	}

	timeline, from, ok := gapStart(starts, gap.from, gap.server, settings.segSize)
	if !ok {
		return fmt.Errorf("%w: %s; take a base backup, and streaming continues where it starts", ErrGap, gap.what)
	}

	r.logger().Warn("the WAL set can't go on where it ends; streaming from the oldest base backup after that",
		"why", gap.what, "from", FormatLSN(from))

	err = run.session(ctx, stop, &position{timeline: timeline, lsn: from})
	if errors.As(err, &gap) {
		return fmt.Errorf("%w: the server no longer has the WAL from %s on, where the next base backup starts; take a base backup", ErrGap, FormatLSN(gap.from))
	}

	return err
}

// setState reads the WAL set's newest commit: the files it holds, and
// where its WAL ends.
func (r *Receiver) setState(ctx context.Context, segSize uint64) (*heldSet, uint64, error) {
	b := &WALBackup{Walker: r.Walker()}

	held, err := b.previous(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("reading the WAL set: %w", err)
	}

	if held.commit == nil {
		return held, 0, nil
	}

	meta := held.commit.GetMetadata()

	if end, ok := meta[MetaEndLSN]; ok {
		lsn, err := ParseLSN(end)
		return held, lsn, err
	}

	if last := meta[MetaLastWALFile]; isSegment(last) {
		return held, (segmentNumber(last, segSize) + 1) * segSize, nil
	}

	return held, 0, nil
}

// baseStarts lists the segments the base set's backups started in.
func (r *Receiver) baseStarts(ctx context.Context) ([]string, error) {
	w := r.Walker()

	bases, err := w.Index.CommitInfo(ctx, r.BaseSet, r.now().Add(100*365*24*time.Hour), maxBases)
	if err != nil {
		return nil, fmt.Errorf("listing the base backups: %w", err)
	}

	var starts []string
	for _, base := range bases {
		starts = append(starts, base.GetMetadata()[MetaStartWALFile])
	}

	return starts, nil
}

func (r *Receiver) commit(ctx context.Context, c *cut) (*backup.WalkResult, error) {
	b := &WALBackup{
		Walker:  r.Walker(),
		Spool:   r.Spool,
		BaseSet: r.BaseSet,
		Window:  r.Window,
		Now:     time.Now,
		Files:   c.files,
		Open:    c.open,
	}

	return b.Run(ctx)
}

func (r *Receiver) at(point string) error {
	if r.hook == nil {
		return nil
	}

	return r.hook(point)
}

type position struct {
	timeline uint32
	lsn      uint64
}

// gapError is a session finding that the WAL set can't go on from where it
// ends: the server no longer has the WAL from there, or its history leaves
// the set's before the WAL the set holds.
type gapError struct {
	what   string
	from   uint64
	server history
}

func (e *gapError) Error() string { return e.what }

// receiveRun is what a Run's sessions share.
type receiveRun struct {
	r         *Receiver
	mon       *monitor
	slot      string
	settings  settings
	held      *heldSet
	committed uint64
}

// session streams from the resume point, or from at, until stop.
func (run *receiveRun) session(ctx context.Context, stop <-chan struct{}, at *position) error {
	r := run.r

	conn, err := connectReplication(ctx, r.ConnString, run.slot)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	id, err := identify(ctx, conn)
	if err != nil {
		return err
	}

	if run.held.systemID != 0 && id.systemID != run.held.systemID {
		return fmt.Errorf("%w: the server is %d, the WAL set of %d", ErrForeignCluster, id.systemID, run.held.systemID)
	}

	spooled, err := r.Spool.Files()
	if err != nil {
		return err
	}

	names := append(slices.Clone(run.held.names), spooled...)

	slot, err := run.ensureSlot(ctx, conn, len(names) > 0)
	if err != nil {
		return err
	}

	stream := &pgStream{conn: conn, slot: run.slot, spool: r.Spool, lacks: func(name string) bool {
		_, err := os.Stat(r.Spool.path(name))
		return !slices.Contains(run.held.names, name) && errors.Is(err, fs.ErrNotExist)
	}}

	server, err := stream.serverHistory(ctx, id.timeline)
	if err != nil {
		return err
	}

	segSize := run.settings.segSize

	from := at
	if from == nil {
		if from, err = run.resume(ctx, names, spooled, server); err != nil {
			var gap *gapError
			if errors.As(err, &gap) {
				gap.server = server
			}

			return err
		}

		if from == nil {
			from = &position{timeline: id.timeline, lsn: slot.restartLSN - slot.restartLSN%segSize}
		}
	}

	lo := from.timeline
	if newest := newestTimeline(names); newest != 0 {
		lo = min(lo, newest)
	}

	for t := min(lo+1, id.timeline); t <= id.timeline; t++ {
		if err := stream.fetchHistory(ctx, t); err != nil {
			return err
		}
	}

	w, err := newSegmentWriter(r.Spool, segSize, from.timeline, from.lsn)
	if err != nil {
		return err
	}
	defer w.stop()

	s := &session{
		r:         r,
		stream:    stream,
		w:         w,
		mon:       run.mon,
		slot:      run.slot,
		keepSize:  run.settings.keepSize,
		floor:     slot.restartLSN,
		flushed:   run.committed,
		start:     from.lsn,
		retry:     len(spooled) > 0,
		walStatus: slot.walStatus,
		safe:      slot.safeWALSize,
		restart:   slot.restartLSN,
		commit:    r.commit,
	}

	r.logger().Info("streaming WAL", "slot", run.slot, "timeline", from.timeline, "from", FormatLSN(from.lsn))

	end, err := stream.start(ctx, from.timeline, from.lsn)
	if err != nil {
		return err
	}

	if end != nil {
		if err := s.follow(ctx, *end); err != nil {
			return err
		}
	}

	s.flushing = func(ctx context.Context, flush uint64) error { return run.mon.checkAsync(ctx, conn.PID(), flush) }

	down, unwatch := watchShutdown(ctx, r.ConnString)
	defer unwatch()

	s.down = down

	err = s.run(ctx, stop)
	run.committed = max(run.committed, s.flushed)

	var gap *gapError
	if errors.As(err, &gap) {
		gap.server = server
	}

	return err
}

// resume is where streaming starts again after what names, the WAL set's
// files and the spool's, hold; nil when they hold no segment.
func (run *receiveRun) resume(ctx context.Context, names, spooled []string, server history) (*position, error) {
	set, err := walHistory(names, func(name string) ([]byte, error) {
		if slices.Contains(spooled, name) {
			return os.ReadFile(run.r.Spool.path(name))
		}

		return run.held.read(ctx, name)
	})
	if err != nil {
		return nil, err
	}

	res, err := resumeAt(names, set, server, run.settings.segSize)
	if err != nil || res == nil {
		return nil, err
	}

	if res.fork {
		run.r.logger().Warn("the server's history leaves the WAL set's, as after a restore to an earlier moment; streaming from where they part",
			"timeline", res.timeline, "from", FormatLSN(res.lsn))

		// the set's end lies past the fork, on a history the server
		// doesn't have; as a flush position it would release WAL no
		// commit holds
		run.committed = min(run.committed, res.lsn)
	}

	return &res.position, nil
}

// ensureSlot checks the receiver's slot, creating it when it is missing
// and creating it anew when the server lost it.
func (run *receiveRun) ensureSlot(ctx context.Context, conn *pgconn.PgConn, resuming bool) (slotState, error) {
	log := run.r.logger()

	slot, ok, err := run.mon.slot(ctx, run.slot)
	if err != nil {
		return slot, err
	}

	// the server releases the slot a moment after the connection that held
	// it ends, which may be this receiver's previous one
	for i := 0; ok && slot.active && i < 20; i++ {
		select {
		case <-ctx.Done():
			return slot, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}

		if slot, ok, err = run.mon.slot(ctx, run.slot); err != nil {
			return slot, err
		}
	}

	action, err := slotAction(run.slot, slot, ok)
	if err != nil {
		return slot, err
	}

	switch action {
	case slotKeep:
		return slot, nil
	case slotRecreate:
		log.Warn("the server lost the replication slot's WAL; creating the slot anew", "slot", run.slot)

		if err := dropSlot(ctx, conn, run.slot); err != nil {
			return slot, err
		}
	case slotCreate:
		if resuming {
			log.Warn("the replication slot is missing; creating it", "slot", run.slot)
		}
	}

	if err := createSlot(ctx, conn, run.slot); err != nil {
		return slot, err
	}

	slot, ok, err = run.mon.slot(ctx, run.slot)
	if err == nil && !ok {
		err = fmt.Errorf("the replication slot %s is missing right after creating it", run.slot)
	}

	return slot, err
}

// What the receiver does with its slot before streaming.
const (
	slotKeep = iota
	slotCreate
	slotRecreate
)

// slotAction decides what to do with the slot named name in state slot,
// which exists when ok: create it when missing, drop and create it when
// the server lost its WAL, and refuse one that is logical or in use.
func slotAction(name string, slot slotState, ok bool) (int, error) {
	switch {
	case !ok:
		return slotCreate, nil
	case !slot.physical:
		return 0, fmt.Errorf("the replication slot %s is not a physical one", name)
	case slot.active:
		return 0, fmt.Errorf("%w: %s", ErrSlotActive, name)
	case slot.walStatus == "lost":
		return slotRecreate, nil
	}

	return slotKeep, nil
}

// session is one replication connection's streaming.
type session struct {
	r      *Receiver
	stream walStream
	w      *segmentWriter
	mon    *monitor
	slot   string
	commit func(context.Context, *cut) (*backup.WalkResult, error)
	// flushing, when set, runs once the server has heard the first flush
	// position.
	flushing func(ctx context.Context, flush uint64) error
	// down is closed once the server starts shutting down; its walsender
	// then waits for a flush position at the end of what it sent.
	down <-chan struct{}
	// ended is set once the stream is gone and hears no more status.
	ended bool

	keepSize int64
	// floor is the slot's position when streaming started: a flush
	// position before it would move the slot back onto WAL the server
	// may have removed, so none is reported until a commit passes it.
	floor uint64
	// flushed is where the WAL the newest stored commit holds ends.
	flushed uint64
	// start is where streaming started.
	start uint64
	// retry is set while the spool holds files no stored commit took.
	retry    bool
	received bool

	serverEnd uint64
	walStatus string
	safe      int64
	restart   uint64
	alert     string

	// oldest is when the oldest WAL no stored commit holds arrived;
	// sinceCut when the first WAL after the last cut did.
	oldest   time.Time
	sinceCut time.Time
}

type commitDone struct {
	cut    *cut
	result *backup.WalkResult
	err    error
}

func (s *session) run(ctx context.Context, stop <-chan struct{}) error {
	r := s.r
	now := r.now()

	var (
		inflight    chan commitDone
		failures    int
		lastStatus  time.Time
		lastMonitor = now
		lastCommit  = now
		cutEnd      = s.w.written
	)

	defer func() {
		if inflight != nil {
			<-inflight
		}
	}()

	for {
		select {
		case d := <-inflight:
			inflight = nil

			if err := s.done(ctx, d, &failures); err != nil {
				return err
			}

			lastStatus = r.now()
		default:
		}

		select {
		case <-stop:
			if err := s.drain(ctx, &inflight, &failures); err != nil {
				return err
			}

			return s.final(ctx, s.w.written > cutEnd || s.retry)
		default:
		}

		now = r.now()

		waiting := s.w.written > cutEnd || s.retry
		if inflight == nil && s.due(now.Sub(lastCommit), waiting, failures) {
			c, err := s.w.cut()
			if err != nil {
				return err
			}

			if err := r.at("cut"); err != nil {
				return err
			}

			lastCommit, cutEnd, s.sinceCut = now, c.end, time.Time{}

			ch := make(chan commitDone, 1)
			go func() {
				result, err := s.commit(ctx, c)
				ch <- commitDone{cut: c, result: result, err: err}
			}()

			inflight = ch
		}

		if now.Sub(lastStatus) >= statusInterval {
			if err := s.status(false); err != nil {
				return err
			}

			lastStatus = now
		}

		if s.mon != nil && now.Sub(lastMonitor) >= monitorInterval {
			s.watch(ctx)
			lastMonitor = now
		}

		msg, err := s.stream.next(ctx, now.Add(receiveWait))
		if err != nil {
			if isRemovedWAL(err) && !s.received {
				return &gapError{what: fmt.Sprintf("the server no longer has the WAL from %s on, where the WAL set goes on", FormatLSN(s.start)), from: s.start}
			}

			if !s.shuttingDown() && !errors.Is(err, errStreamEnded) {
				return err
			}

			s.ended = true

			if err := s.drain(ctx, &inflight, &failures); err != nil {
				return err
			}

			if err := s.final(ctx, s.w.written > cutEnd || s.retry); err != nil {
				return err
			}

			return ErrServerShutDown
		}

		switch m := msg.(type) {
		case xlogData:
			if err := s.w.write(m.start, m.data); err != nil {
				return err
			}

			s.received = true
			s.serverEnd = max(s.serverEnd, m.serverEnd, s.w.written)

			if s.oldest.IsZero() {
				s.oldest = r.now()
			}

			if s.sinceCut.IsZero() {
				s.sinceCut = r.now()
			}
		case keepalive:
			s.serverEnd = max(s.serverEnd, m.serverEnd)

			if m.reply {
				if err := s.status(false); err != nil {
					return err
				}

				lastStatus = r.now()
			}
		case timelineEnd:
			if err := s.follow(ctx, m); err != nil {
				return err
			}
		}
	}
}

// due reports whether a commit starts now, since after the last one
// started: every CommitEvery while new WAL waits, after the floor once
// CommitBytes of it waits, at once while the server shuts down, and backing
// off after failed commits.
func (s *session) due(since time.Duration, waiting bool, failures int) bool {
	if !waiting {
		return false
	}

	if s.shuttingDown() && failures == 0 {
		return true
	}

	r := s.r

	wait := r.CommitEvery
	if int64(s.w.written-max(s.flushed, s.start)) >= r.CommitBytes {
		wait = r.floor
	}

	if failures > 0 {
		wait = max(wait, min(r.floor<<min(failures-1, 16), maxCommitBackoff))
	}

	return since >= max(wait, r.floor)
}

// done takes in a finished commit: the segments that filled meanwhile move
// into the spool, and once the commit is stored the server hears the new
// flush position.
func (s *session) done(ctx context.Context, d commitDone, failures *int) error {
	if err := s.w.release(); err != nil {
		return err
	}

	if d.err != nil {
		*failures++
		s.retry = true

		if ctx.Err() != nil || errors.Is(d.err, ErrGap) || errors.Is(d.err, ErrForeignSegment) {
			return d.err
		}

		s.r.logger().Warn("the commit failed; the WAL waits in the spool", "error", d.err, "failures", *failures)

		return nil
	}

	*failures = 0
	s.retry = false

	if err := s.r.at("committed"); err != nil {
		return err
	}

	s.flushed = max(s.flushed, d.cut.end)
	s.oldest = s.sinceCut

	if err := s.status(false); err != nil {
		return err
	}

	if d.result != nil && s.r.Committed != nil {
		s.r.Committed(d.result)
	}

	return nil
}

// shuttingDown reports whether the server started shutting down.
func (s *session) shuttingDown() bool {
	select {
	case <-s.down:
		return true
	default:
		return false
	}
}

// drain waits for the commit in flight, if any, and takes it in.
func (s *session) drain(ctx context.Context, inflight *chan commitDone, failures *int) error {
	if *inflight == nil {
		return nil
	}

	select {
	case d := <-*inflight:
		*inflight = nil

		return s.done(ctx, d, failures)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// final makes the last commit when anything waits for one, and tells the
// server.
func (s *session) final(ctx context.Context, waiting bool) error {
	if waiting {
		c, err := s.w.cut()
		if err != nil {
			return err
		}

		if err := s.r.at("cut"); err != nil {
			return err
		}

		result, err := s.commit(ctx, c)

		var failures int
		if err := s.done(ctx, commitDone{cut: c, result: result, err: err}, &failures); err != nil {
			return err
		}

		if err != nil {
			return fmt.Errorf("the last commit: %w", err)
		}
	}

	return s.status(false)
}

func (s *session) follow(ctx context.Context, end timelineEnd) error {
	s.r.logger().Info("the timeline ended; following the next one", "timeline", end.next, "from", FormatLSN(end.start))

	if err := s.w.switchTimeline(end.next, end.start); err != nil {
		return err
	}

	return s.stream.follow(ctx, end.next, s.w.written)
}

// flush is the flush position the server hears.
func (s *session) flush() uint64 {
	if s.flushed < s.floor {
		return 0
	}

	return s.flushed
}

func (s *session) status(reply bool) error {
	flush := s.flush()

	if !s.ended {
		if err := s.stream.status(s.w.written, flush, reply); err != nil {
			return fmt.Errorf("sending the standby status: %w", err)
		}
	}

	if flush != 0 && s.flushing != nil && !s.ended {
		if err := s.flushing(context.Background(), flush); err != nil {
			return err
		}

		s.flushing = nil
	}

	if s.r.Report != nil {
		s.r.Report(s.report())
	}

	return nil
}

func (s *session) report() ReceiverStatus {
	now := s.r.now()

	st := ReceiverStatus{
		Timeline:    s.w.timeline,
		Received:    FormatLSN(s.w.written),
		WALStatus:   s.walStatus,
		SafeWALSize: s.safe,
		SpoolBytes:  s.spoolBytes(),
		Healthy:     true,
	}

	if s.flushed != 0 {
		st.Committed = FormatLSN(s.flushed)
	}

	if s.serverEnd > s.flushed {
		st.LagBytes = int64(s.serverEnd - max(s.flushed, s.start))
	}

	if s.serverEnd > s.restart && s.restart != 0 {
		st.RetainedBytes = int64(s.serverEnd - s.restart)
	}

	if !s.oldest.IsZero() {
		age := now.Sub(s.oldest)
		st.LagSeconds = age.Seconds()
		st.Healthy = age <= 3*s.r.CommitEvery
	}

	return st
}

func (s *session) spoolBytes() int64 {
	var total int64

	for _, dir := range []string{s.r.Spool.Dir, s.r.Spool.receiving()} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}

		for _, e := range entries {
			if info, err := e.Info(); err == nil && info.Mode().IsRegular() {
				total += info.Size()
			}
		}
	}

	return total
}

// watch reads the slot's state and warns as it approaches the cap.
func (s *session) watch(ctx context.Context) {
	log := s.r.logger()

	slot, ok, err := s.mon.slot(ctx, s.slot)
	if err != nil {
		log.Warn("reading the replication slot failed", "error", err)
		return
	}

	if !ok {
		s.alert = "missing"
		log.Error("the replication slot is gone", "slot", s.slot)

		return
	}

	s.walStatus, s.safe, s.restart = slot.walStatus, slot.safeWALSize, slot.restartLSN

	if current, err := s.mon.currentLSN(ctx); err == nil {
		s.serverEnd = max(s.serverEnd, current)
	}

	alert := slotAlert(slot, s.keepSize)
	if alert == s.alert {
		return
	}

	s.alert = alert

	switch alert {
	case "":
		log.Info("the replication slot's WAL is reserved again", "slot", s.slot)
	case "unreserved", "lost":
		log.Error("the replication slot is about to lose WAL, or has: the server keeps no more for it than max_slot_wal_keep_size", "slot", s.slot, "wal_status", slot.walStatus)
	default:
		log.Warn("the replication slot nears max_slot_wal_keep_size", "slot", s.slot, "wal_status", slot.walStatus, "safe_wal_size", strconv.FormatInt(slot.safeWALSize, 10))
	}
}

// slotAlert names what is wrong with a slot whose cap is keepSize: its
// wal_status once it leaves reserved, or low when less than a quarter of
// the cap is left before the slot is lost; empty when nothing is.
func slotAlert(slot slotState, keepSize int64) string {
	switch {
	case slot.walStatus != "reserved" && slot.walStatus != "":
		return slot.walStatus
	case keepSize > 0 && slot.safeWALSize >= 0 && slot.safeWALSize < keepSize/4:
		return "low"
	}

	return ""
}

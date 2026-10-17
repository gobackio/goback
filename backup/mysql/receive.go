package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/lockfile"
	"github.com/gobackio/goback/proto"
)

// The receiver's commit cadence.
const (
	// DefaultCommitEvery is how often the receiver commits what is new.
	DefaultCommitEvery = time.Minute
	// DefaultCommitBytes is how much new binlog makes it commit early.
	DefaultCommitBytes = 256 << 20
	// MinCommitEvery is the shortest interval it takes, and the least time
	// between two commits.
	MinCommitEvery = 10 * time.Second
)

const (
	// reportInterval is how often the receiver reports how it stands.
	reportInterval  = 10 * time.Second
	monitorInterval = time.Minute
	// receiveWait bounds each wait for an event, so a finished commit is
	// noticed within it.
	receiveWait = time.Second
	// maxCommitBackoff bounds the wait after failed commits.
	maxCommitBackoff = 5 * time.Minute
	// maxReconnectBackoff bounds the wait between reconnects, which starts
	// at a second and doubles.
	maxReconnectBackoff = time.Minute
	// quickDisconnect is how soon after the dump started a disconnect
	// counts towards the kick loop two replicas with one server id make;
	// kickLoop of them within kickWindow end the run. A replaced dump
	// notices only with its next event, a heartbeat at the latest.
	quickDisconnect = readTimeout
	kickLoop        = 3
	kickWindow      = time.Minute
)

// Receiver receives a server's binary log as a replica does, through the
// binlog dump protocol, into a spool, and commits it to the binlog set on
// a timer: whole binlogs as the server moves on from them, and the binlog
// being received up to its last event as <binlog>.partial. It sends the
// server nothing back: MySQL keeps its binlogs by its own expiry.
type Receiver struct {
	// DefaultsExtraFile is the option file read before ~/.my.cnf for the
	// connection and its password.
	DefaultsExtraFile string
	// ServerID is the replica server id the receiver connects with;
	// ReplicaServerID of the set when zero.
	ServerID uint32
	// Spool is where binlogs wait for their commit; one receiver per spool.
	Spool Spool
	// Walker returns the walker of one commit: its Set is the binlog set
	// and its Root the spool's directory.
	Walker func() *backup.Walker
	// DumpSet is the set the server's dumps go to.
	DumpSet string
	// Window is how far back point-in-time recovery reaches at least.
	Window time.Duration
	// CommitEvery is how often new binlog is committed; DefaultCommitEvery
	// when zero, and at least MinCommitEvery.
	CommitEvery time.Duration
	// CommitBytes of new binlog commit early; DefaultCommitBytes when zero.
	CommitBytes int64
	// Logger is where the receiver reports; nil means slog.Default.
	Logger *slog.Logger
	// Committed, when set, hears of each commit once it is stored.
	Committed func(*backup.WalkResult)
	// Report, when set, hears how the receiver stands every ten seconds.
	Report func(ReceiverStatus)
	// UntilCaughtUp makes Run stop on its own once it has received the
	// binlog the server had written when it started: it commits that and
	// returns nil.
	UntilCaughtUp bool

	floor time.Duration
	now   func() time.Time
	// hook, when set, is called at named points of a commit; an error from
	// it ends Run there, as a crash would.
	hook func(point string) error
	// server and dial, when set, replace the connections to the server.
	server Querier
	dial   func(serverID uint32, name string, pos int64) (binlogStream, error)
	// wait, when set, replaces waiting before a reconnect
	wait func(time.Duration)
}

// ReceiverStatus is how a receiver stands.
type ReceiverStatus struct {
	Chain     int  `json:"chain"`
	Connected bool `json:"connected"`
	// Received is the binlog and position the spool's binlogs end at.
	Received string `json:"received,omitempty"`
	// Committed is where the binlogs the newest stored commit holds end.
	Committed string `json:"committed,omitempty"`
	// LagBytes is how much binlog the server holds that no commit holds,
	// as of the last minute; LagSeconds how long the oldest received byte
	// no commit holds has waited.
	LagBytes   int64   `json:"lag_bytes"`
	LagSeconds float64 `json:"lag_seconds"`
	SpoolBytes int64   `json:"spool_bytes"`
	ServerID   uint32  `json:"server_id"`
	// Healthy is false while a received byte has waited for a commit, or
	// the receiver for a connection, longer than three commit intervals.
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

func (r *Receiver) at(point string) error {
	if r.hook == nil {
		return nil
	}

	return r.hook(point)
}

// Run receives and commits until stop is closed, or with UntilCaughtUp
// until it is caught up, then commits what it received and returns nil.
// It reconnects after the connection fails. It returns early with an error
// that needs a person (ErrGap, ErrNoBinlog, ErrForeignBinlog,
// ErrBinlogChanged, ErrServerIDInUse), or with one a restart may clear.
// When ctx ends, it stops without the last commit.
func (r *Receiver) Run(ctx context.Context, stop <-chan struct{}) error {
	if err := r.setDefaults(); err != nil {
		return err
	}

	set := r.Walker().Set

	serverID := r.ServerID
	if serverID == 0 {
		serverID = ReplicaServerID(set)
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

	run := &receiveRun{r: r, serverID: serverID, server: r.server, dial: r.dial}

	if run.server == nil || run.dial == nil {
		opts, err := ReadOptions(r.DefaultsExtraFile)
		if err != nil {
			return fmt.Errorf("reading the option files: %w", err)
		}

		if run.server == nil {
			conn := newServerConn(opts)
			defer conn.Close()

			run.server = conn
		}

		if run.dial == nil {
			run.dial = func(serverID uint32, name string, pos int64) (binlogStream, error) {
				return startDump(opts, serverID, name, pos)
			}
		}
	}

	defer func() {
		if run.w != nil {
			run.w.stop()
		}
	}()

	if err := run.start(ctx); err != nil {
		return err
	}

	if r.UntilCaughtUp {
		if err := run.target(ctx); err != nil {
			return err
		}
	}

	return run.loop(ctx, stop)
}

// receiveRun is one Run's state.
type receiveRun struct {
	r        *Receiver
	serverID uint32
	server   Querier
	dial     func(serverID uint32, name string, pos int64) (binlogStream, error)

	srv   *Server
	id    Identity
	w     *binlogWriter
	chain int
	// from is where the next dump starts
	from     string
	fromPos  int64
	hostname string
	// partialAt, when set, is the time of the newest event of the partial
	// binlog the dump starts at, which the set holds and the spool lacks
	partialAt time.Time

	stream    binlogStream
	connected time.Time
	// down is when the receiver last lost its connection
	down    time.Time
	backoff time.Duration
	retryAt time.Time
	quick   []time.Time

	inflight   chan commitDone
	failures   int
	retry      bool
	lastCommit time.Time
	lastReport time.Time
	lastWatch  time.Time
	// received counts the bytes written to the spool; cutAt and flushed
	// what the last cut and the last stored commit hold of them
	received int64
	cutAt    int64
	flushed  int64
	// oldest is when the oldest byte no stored commit holds arrived;
	// sinceCut when the first byte after the last cut did
	oldest    time.Time
	sinceCut  time.Time
	committed string
	commitPos int64
	lagBytes  int64
	// until, when set, is the binlog and size the run stops after
	until *Binlog
}

type commitDone struct {
	cut    *cut
	result *backup.WalkResult
	err    error
}

// start reads the server and the set, and finds where to receive from.
func (run *receiveRun) start(ctx context.Context) error {
	r := run.r

	if err := run.identify(ctx); err != nil {
		return err
	}

	run.hostname, _ = os.Hostname()

	registered, err := replicas(ctx, run.server, run.srv)
	if err != nil {
		return fmt.Errorf("listing the server's replicas: %w", err)
	}

	if err := checkServerID(run.serverID, run.srv, registered, run.hostname); err != nil {
		return err
	}

	held, err := run.held(ctx)
	if err != nil {
		return err
	}

	if held != nil && held.hasID && !held.identity.Same(run.id) {
		return fmt.Errorf("%w: the server is %s, the set's %s", ErrForeignBinlog, run.id, held.identity)
	}

	if run.w, err = openBinlogWriter(r.Spool); err != nil {
		return err
	}

	run.chain = 1
	if held != nil {
		run.chain = held.chain
		run.committed, run.commitPos = held.meta[MetaEndBinlog], parseInt(held.meta[MetaEndPos])
	}

	if n, ok, err := r.Spool.chain(); err != nil {
		return err
	} else if ok {
		run.chain = n
	} else if err := r.Spool.setChain(run.chain); err != nil {
		return err
	}

	dumps, err := run.dumps(ctx)
	if err != nil {
		return err
	}

	// a dump that saw the server's binlogs start over opened a newer chain
	if len(dumps) > 0 {
		if n := chainOf(dumps[0].GetMetadata()); n > run.chain {
			r.logger().Warn("the newest dump starts a new chain; the server's binlogs started over", "chain", n)
			return run.switchChain(ctx, n, dumps[0].GetMetadata()[MetaBinlogFile])
		}
	}

	var names []string
	if held != nil {
		for _, node := range held.chains[run.chain] {
			names = append(names, string(node.Stat.Name))
		}
	}

	spooled, err := r.Spool.Files()
	if err != nil {
		return err
	}

	open, end := run.w.position()

	from, pos, ok := resumePoint(open, end, append(names, spooled...), newestStart(dumps, run.chain))
	if !ok {
		return fmt.Errorf("the dump set %s holds no dump with a binlog position to receive from; take a dump first", r.DumpSet)
	}

	run.from, run.fromPos = from, pos
	run.retry = len(spooled) > 0 || open != ""

	// a run that only catches up commits nothing the newest commit holds
	if r.UntilCaughtUp && len(spooled) == 0 && held != nil && held.chain == run.chain && open == run.committed && end == run.commitPos {
		run.retry = false
	}

	if open == "" && held != nil {
		for _, node := range held.chains[run.chain] {
			if string(node.Stat.Name) == from+partialSuffix {
				run.partialAt = time.Unix(0, node.Stat.GetMtimeNs())
			}
		}
	}

	return nil
}

// identify reads the server and checks it logs its binlogs.
func (run *receiveRun) identify(ctx context.Context) error {
	srv, err := ReadServer(ctx, run.server)
	if err != nil {
		return err
	}

	if !srv.LogBin {
		return ErrNoBinlog
	}

	base := ""
	if srv.BinlogBasename != "" {
		base = filepath.Base(srv.BinlogBasename)
	} else if logs, err := BinaryLogs(ctx, run.server); err == nil && len(logs) > 0 {
		base, _, _ = splitBinlog(logs[0].Name)
	}

	id := Identity{ServerID: srv.ServerID, ServerUUID: srv.ServerUUID, Base: base}
	if run.srv != nil && !run.id.Same(id) {
		return fmt.Errorf("%w: the server is now %s, it was %s", ErrForeignBinlog, id, run.id)
	}

	run.srv, run.id = srv, id

	return nil
}

func (run *receiveRun) held(ctx context.Context) (*binlogCommit, error) {
	w := run.r.Walker()

	ref, err := w.Index.LatestCommit(ctx, w.Set)
	if errors.Is(err, backup.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return readBinlogCommit(ctx, w.Objects, w.Key, ref)
}

// dumps lists the server's dumps with a position, newest first.
func (run *receiveRun) dumps(ctx context.Context) ([]*proto.Commit, error) {
	w := run.r.Walker()

	commits, err := w.Index.CommitInfo(ctx, run.r.DumpSet, run.r.now().Add(100*365*24*time.Hour), maxDumps)
	if err != nil {
		return nil, fmt.Errorf("listing the dumps: %w", err)
	}

	return dumpCommits(commits, run.id), nil
}

func (run *receiveRun) loop(ctx context.Context, stop <-chan struct{}) error {
	r := run.r
	now := r.now()
	run.lastCommit, run.lastWatch, run.down = now, now, now

	defer func() {
		if run.inflight != nil {
			<-run.inflight
		}

		run.disconnect()
	}()

	for {
		if err := run.collect(ctx, false); err != nil {
			return err
		}

		select {
		case <-stop:
			return run.final(ctx)
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if run.caughtUp() {
			if err := run.final(ctx); err != nil {
				return err
			}

			r.logger().Info("caught up with the server", "at", fmt.Sprintf("%s:%d", run.until.Name, run.until.Size))

			return nil
		}

		now = r.now()

		if err := run.tick(ctx, now); err != nil {
			return err
		}

		if run.stream == nil {
			if now.Before(run.retryAt) {
				run.sleep(ctx, stop, min(receiveWait, run.retryAt.Sub(now)))
				continue
			}

			if err := run.connect(ctx); err != nil {
				if err := run.failed(ctx, err); err != nil {
					return err
				}
			}

			continue
		}

		ev, err := run.stream.next(ctx, now.Add(receiveWait))
		if err == nil && ev != nil {
			err = run.handle(ev)
		}

		if err != nil {
			if err := run.failed(ctx, err); err != nil {
				return err
			}
		}
	}
}

// target makes where the server's binlogs end now the point the run stops
// after.
func (run *receiveRun) target(ctx context.Context) error {
	logs, err := BinaryLogs(ctx, run.server)
	if err != nil {
		return fmt.Errorf("listing the server's binlogs: %w", err)
	}

	if len(logs) == 0 {
		return errors.New("the server lists no binary logs")
	}

	run.until = &logs[len(logs)-1]

	return nil
}

// caughtUp reports whether the run has received all of until.
func (run *receiveRun) caughtUp() bool {
	if run.until == nil {
		return false
	}

	name, pos, ok := run.w.resume()
	if !ok {
		name, pos = run.from, run.fromPos
	}

	c := compareBinlogs(name, run.until.Name)

	return c > 0 || c == 0 && pos >= run.until.Size
}

// sleep waits for d, or until stop or ctx ends.
func (run *receiveRun) sleep(ctx context.Context, stop <-chan struct{}, d time.Duration) {
	if run.r.wait != nil {
		run.r.wait(d)
		return
	}

	select {
	case <-stop:
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// connect checks the server is the set's and starts the dump.
func (run *receiveRun) connect(ctx context.Context) error {
	if err := run.identify(ctx); err != nil {
		return err
	}

	stream, err := run.dial(run.serverID, run.from, run.fromPos)
	if err != nil {
		return err
	}

	run.r.logger().Info("receiving binlogs", "chain", run.chain, "from", fmt.Sprintf("%s:%d", run.from, run.fromPos), "server_id", run.serverID)

	run.stream, run.connected = stream, run.r.now()

	return nil
}

func (run *receiveRun) disconnect() {
	if run.stream != nil {
		run.stream.close()
		run.stream = nil
		run.down = run.r.now()
	}
}

// failed deals with err from connecting or receiving: it ends the run for
// errors that need a person, moves past a reset or a gap a dump bridges,
// and otherwise reconnects with backoff.
func (run *receiveRun) failed(ctx context.Context, err error) error {
	r := run.r
	log := r.logger()

	started := run.connected
	wasStreaming := run.stream != nil
	run.disconnect()

	if name, pos, ok := run.w.resume(); ok {
		run.from, run.fromPos = name, pos
	}

	var reset *resetError

	switch code := serverErrorCode(err); {
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(err, ErrForeignBinlog), errors.Is(err, ErrNoBinlog), errors.Is(err, ErrBinlogChanged),
		errors.Is(err, ErrGap), errors.Is(err, ErrServerIDInUse):
		return err
	case errors.As(err, &reset):
		return run.reset(ctx, reset.Error())
	case code == errNoBinlogFile:
		return run.lost(ctx, err)
	case code == errAccessDenied, code == errDBAccessDenied, code == errNeedPrivilege:
		return err
	}

	now := r.now()

	if wasStreaming && now.Sub(started) < quickDisconnect {
		run.quick = append(run.quick, now)

		recent := run.quick[:0]
		for _, at := range run.quick {
			if now.Sub(at) < kickWindow {
				recent = append(recent, at)
			}
		}

		run.quick = recent

		if len(recent) >= kickLoop {
			return fmt.Errorf("%w: the server ended the dump %d times within a minute; another replica uses server id %d: %v", ErrServerIDInUse, len(recent), run.serverID, err)
		}
	}

	if wasStreaming && now.Sub(started) > maxReconnectBackoff {
		run.backoff = 0
	}

	run.backoff = min(max(2*run.backoff, time.Second), maxReconnectBackoff)
	run.retryAt = now.Add(run.backoff)

	log.Warn("the connection to the server failed; reconnecting", "error", err, "in", run.backoff)

	return nil
}

// lost deals with the server refusing to send from the resume point.
func (run *receiveRun) lost(ctx context.Context, err error) error {
	logs, listErr := BinaryLogs(ctx, run.server)
	if listErr != nil {
		return fmt.Errorf("%w; listing the server's binlogs: %w", err, listErr)
	}

	switch classifyLost(logs, run.from, run.fromPos) {
	case lostReset:
		return run.reset(ctx, fmt.Sprintf("the server's binlogs start over at %s", logs[0].Name))
	case lostTail:
		return fmt.Errorf("%w: the server's %s is shorter than the %d bytes received of it; set sync_binlog = 1", ErrBinlogChanged, run.from, run.fromPos)
	case lostPurged:
		dumps, dumpErr := run.dumps(ctx)
		if dumpErr != nil {
			return dumpErr
		}

		if start := newestStart(dumps, run.chain); start != "" && compareBinlogs(start, run.from) > 0 && compareBinlogs(start, logs[0].Name) >= 0 {
			run.r.logger().Warn("the server purged binlogs before they were received; receiving from the newest dump after them", "missing", run.from, "from", start)

			return run.restartAt(ctx, start)
		}

		return fmt.Errorf("%w: the server purged %s before goback received it, and %s on; take a dump, and receiving goes on from it", ErrGap, run.from, logs[0].Name)
	}

	return err
}

// restartAt drops the binlog being received, whose end the server no
// longer has, and receives from the start of name next.
func (run *receiveRun) restartAt(ctx context.Context, name string) error {
	if err := run.flush(ctx); err != nil {
		return fmt.Errorf("committing what was received of %s: %w", run.from, err)
	}

	// the commit holds the open binlog as partial
	run.w.stop()

	if err := os.RemoveAll(run.r.Spool.receiving()); err != nil {
		return err
	}

	w, err := openBinlogWriter(run.r.Spool)
	if err != nil {
		return err
	}

	run.w = w
	run.from, run.fromPos, run.partialAt = name, binlogHeaderBytes, time.Time{}

	return nil
}

// resetError is the server's binlogs having started over.
type resetError struct{ reason string }

func (e *resetError) Error() string { return e.reason }

// reset commits what the spool holds to the current chain and goes on in
// the next one from the server's first binlog.
func (run *receiveRun) reset(ctx context.Context, reason string) error {
	logs, err := BinaryLogs(ctx, run.server)
	if err != nil {
		return err
	}

	if len(logs) == 0 {
		return errors.New("the server lists no binary logs")
	}

	run.r.logger().Warn("the server's binlogs started over; going on in a new chain", "reason", reason, "chain", run.chain+1)

	return run.switchChain(ctx, run.chain+1, logs[0].Name)
}

// switchChain commits what the spool holds to the current chain, then
// receives chain n from the start of first.
func (run *receiveRun) switchChain(ctx context.Context, n int, first string) error {
	if err := run.flush(ctx); err != nil {
		return fmt.Errorf("committing chain %d before chain %d: %w", run.chain, n, err)
	}

	run.w.stop()

	if err := run.r.Spool.clear(); err != nil {
		return err
	}

	if err := run.r.Spool.setChain(n); err != nil {
		return err
	}

	w, err := openBinlogWriter(run.r.Spool)
	if err != nil {
		return err
	}

	run.w, run.chain = w, n
	run.from, run.fromPos, run.partialAt = first, binlogHeaderBytes, time.Time{}
	run.committed, run.commitPos = "", 0

	if run.until != nil {
		return run.target(ctx)
	}

	return nil
}

// flush waits for the commit in flight and commits what waits.
func (run *receiveRun) flush(ctx context.Context) error {
	if run.inflight != nil {
		d := <-run.inflight
		run.inflight = nil

		if err := run.done(d); err != nil {
			return err
		}

		if d.err != nil {
			return d.err
		}
	}

	if !run.waiting() {
		return nil
	}

	c, err := run.cut()
	if err != nil {
		return err
	}

	result, err := run.commit(ctx, c)
	d := commitDone{cut: c, result: result, err: err}

	if err := run.done(d); err != nil {
		return err
	}

	return err
}

// handle takes one event of the stream.
func (run *receiveRun) handle(ev *streamEvent) error {
	h := ev.header

	switch {
	case h.kind == heartbeatEvent || h.kind == heartbeatEventV2:
		return nil
	case h.kind == rotateEvent && (h.logPos == 0 || h.flags&artificialEvent != 0):
		// the server names the binlog it sends from
		if current, _ := run.w.position(); current == "" && run.from != "" && compareBinlogs(ev.next, run.from) < 0 ||
			current != "" && compareBinlogs(ev.next, current) < 0 {
			return &resetError{reason: fmt.Sprintf("the server went back from %s to %s", max(current, run.from), ev.next)}
		}

		return run.w.begin(ev.next, int64(ev.pos))
	case h.kind == formatDescriptionEvent && h.logPos == 0:
		// resuming inside a binlog, the server repeats its format
		// description; another one means the binlog was created anew
		if run.w.created != 0 && h.timestamp != run.w.created {
			return &resetError{reason: fmt.Sprintf("the server's %s was created anew", run.w.name)}
		}

		return nil
	case h.logPos == 0:
		return nil
	}

	if h.kind == formatDescriptionEvent && run.w.written == binlogHeaderBytes && !run.partialAt.IsZero() && run.w.name == run.from {
		if time.Unix(int64(h.timestamp), 0).After(run.partialAt) {
			return &resetError{reason: fmt.Sprintf("the server's %s was created after the set's partial copy of it", run.w.name)}
		}
	}

	if err := run.w.write(h, ev.data); err != nil {
		return err
	}

	run.received += int64(len(ev.data))

	now := run.r.now()
	if run.oldest.IsZero() {
		run.oldest = now
	}

	if run.sinceCut.IsZero() {
		run.sinceCut = now
	}

	if h.kind == rotateEvent {
		return run.w.close()
	}

	return nil
}

func (run *receiveRun) waiting() bool {
	return run.received > run.cutAt || run.retry
}

// tick starts a commit when one is due, reports, and watches the server.
func (run *receiveRun) tick(ctx context.Context, now time.Time) error {
	r := run.r

	if run.inflight == nil && run.due(now.Sub(run.lastCommit)) {
		c, err := run.cut()
		if err != nil {
			return err
		}

		if err := r.at("cut"); err != nil {
			return err
		}

		run.lastCommit = now

		ch := make(chan commitDone, 1)
		go func() {
			result, err := run.commit(ctx, c)
			ch <- commitDone{cut: c, result: result, err: err}
		}()

		run.inflight = ch
	}

	if now.Sub(run.lastWatch) >= monitorInterval {
		run.watch(ctx)
		run.lastWatch = now
	}

	if now.Sub(run.lastReport) >= reportInterval {
		run.report()
		run.lastReport = now
	}

	return nil
}

// due reports whether a commit starts now, since after the last one
// started: every CommitEvery while new binlog waits, after the floor once
// CommitBytes of it waits, and backing off after failed commits.
func (run *receiveRun) due(since time.Duration) bool {
	if !run.waiting() {
		return false
	}

	r := run.r

	wait := r.CommitEvery
	if run.received-run.flushed >= r.CommitBytes {
		wait = r.floor
	}

	if run.failures > 0 {
		wait = max(wait, min(r.floor<<min(run.failures-1, 16), maxCommitBackoff))
	}

	return since >= max(wait, r.floor)
}

func (run *receiveRun) cut() (*cut, error) {
	c, err := run.w.cut()
	if err != nil {
		return nil, err
	}

	run.cutAt, run.sinceCut = run.received, time.Time{}

	return c, nil
}

func (run *receiveRun) commit(ctx context.Context, c *cut) (*backup.WalkResult, error) {
	r := run.r

	sc := &spoolCommit{
		Walker:  r.Walker(),
		Spool:   r.Spool,
		DumpSet: r.DumpSet,
		Window:  r.Window,
		Now:     time.Now,
		Chain:   run.chain,
		Server:  run.srv,
		ID:      run.id,
		Files:   c.files,
		Open:    c.open,
	}

	return sc.Run(ctx)
}

// collect takes in the commit in flight once it is done; with wait, it
// waits for it.
func (run *receiveRun) collect(ctx context.Context, wait bool) error {
	if run.inflight == nil {
		return nil
	}

	var d commitDone

	if wait {
		select {
		case d = <-run.inflight:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		select {
		case d = <-run.inflight:
		default:
			return nil
		}
	}

	run.inflight = nil

	return run.done(d)
}

// done takes in a finished commit: the binlogs that became whole meanwhile
// move into the spool.
func (run *receiveRun) done(d commitDone) error {
	if err := run.w.release(); err != nil {
		return err
	}

	if d.err != nil {
		run.failures++
		run.retry = true

		if errors.Is(d.err, ErrGap) || errors.Is(d.err, ErrForeignBinlog) || errors.Is(d.err, context.Canceled) {
			return d.err
		}

		run.r.logger().Warn("the commit failed; the binlogs wait in the spool", "error", d.err, "failures", run.failures)

		return nil
	}

	run.failures = 0
	run.retry = false

	if err := run.r.at("committed"); err != nil {
		return err
	}

	run.flushed = run.cutAt
	run.oldest = run.sinceCut

	if d.cut.end != "" {
		run.committed, run.commitPos = d.cut.end, d.cut.pos
	}

	if d.result != nil && run.r.Committed != nil {
		run.r.Committed(d.result)
	}

	run.report()

	return nil
}

// final makes the last commit when anything waits for one.
func (run *receiveRun) final(ctx context.Context) error {
	if err := run.collect(ctx, true); err != nil {
		return err
	}

	if err := run.flush(ctx); err != nil {
		return fmt.Errorf("the last commit: %w", err)
	}

	run.report()

	return nil
}

// watch reads how far the server's binlogs run past what is committed.
func (run *receiveRun) watch(ctx context.Context) {
	logs, err := BinaryLogs(ctx, run.server)
	if err != nil {
		run.r.logger().Warn("listing the server's binlogs failed", "error", err)
		return
	}

	run.lagBytes = lagBytes(logs, run.committed, run.commitPos)
}

// lagBytes is how much of logs lies past file at pos.
func lagBytes(logs []Binlog, file string, pos int64) int64 {
	var lag int64

	for _, log := range logs {
		switch c := compareBinlogs(log.Name, file); {
		case file == "" || c > 0:
			lag += log.Size
		case c == 0:
			lag += max(log.Size-pos, 0)
		}
	}

	return lag
}

func (run *receiveRun) report() {
	r := run.r
	if r.Report == nil {
		return
	}

	now := r.now()

	st := ReceiverStatus{
		Chain:      run.chain,
		Connected:  run.stream != nil,
		LagBytes:   run.lagBytes,
		SpoolBytes: run.spoolBytes(),
		ServerID:   run.serverID,
		Healthy:    true,
	}

	if name, pos := run.w.position(); name != "" {
		st.Received = fmt.Sprintf("%s:%d", name, pos)
	}

	if run.committed != "" {
		st.Committed = fmt.Sprintf("%s:%d", run.committed, run.commitPos)
	}

	if !run.oldest.IsZero() {
		age := now.Sub(run.oldest)
		st.LagSeconds = age.Seconds()
		st.Healthy = age <= 3*r.CommitEvery
	}

	if run.stream == nil && now.Sub(run.down) > 3*r.CommitEvery {
		st.Healthy = false
	}

	r.Report(st)
}

func (run *receiveRun) spoolBytes() int64 {
	var total int64

	for _, dir := range []string{run.r.Spool.Dir, run.r.Spool.receiving()} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}

		for _, e := range entries {
			if info, err := e.Info(); err == nil && info.Mode().IsRegular() && e.Name()[0] != '.' {
				total += info.Size()
			}
		}
	}

	return total
}

func parseInt(s string) int64 {
	var n int64
	_, _ = fmt.Sscan(s, &n)

	return n
}

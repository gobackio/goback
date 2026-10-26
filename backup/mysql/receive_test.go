package mysql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/cache"

	"github.com/stretchr/testify/require"
)

// t0 is when a receiver test's clock starts: the binlogs it receives are written
// after the dumps it takes.
var t0 = time.Now().UTC().Truncate(time.Second).Add(24 * time.Hour)

// eventBytes is a binlog event of kind with size bytes, ending at end.
func eventBytes(kind byte, serverID, ts, end uint32, size int, flags uint16) []byte {
	b := make([]byte, size)
	binary.LittleEndian.PutUint32(b, ts)
	b[4] = kind
	binary.LittleEndian.PutUint32(b[5:], serverID)
	binary.LittleEndian.PutUint32(b[9:], uint32(size))
	binary.LittleEndian.PutUint32(b[13:], end)
	binary.LittleEndian.PutUint16(b[17:], flags)

	return b
}

func streamed(b []byte) *streamEvent {
	h, _ := parseEventHeader(b)
	return &streamEvent{header: h, data: b}
}

// binlogScript writes the events of fake binlogs as a server sends them.
type binlogScript struct {
	serverID uint32
	events   []any
	name     string
	end      uint32
}

// open starts binlog name: the server names it, then its format
// description.
func (s *binlogScript) open(name string, ts uint32) *binlogScript {
	s.name, s.end = name, binlogHeaderBytes
	s.events = append(s.events, &streamEvent{header: eventHeader{kind: rotateEvent, flags: artificialEvent, serverID: s.serverID}, next: name, pos: binlogHeaderBytes})

	return s.event(formatDescriptionEvent, ts, 100)
}

func (s *binlogScript) event(kind byte, ts uint32, size int) *binlogScript {
	s.end += uint32(size)
	s.events = append(s.events, streamed(eventBytes(kind, s.serverID, ts, s.end, size, 0)))

	return s
}

// rotate ends the binlog with a Rotate event.
func (s *binlogScript) rotate(ts uint32) *binlogScript {
	return s.event(rotateEvent, ts, 40)
}

func (s *binlogScript) fail(err error) *binlogScript {
	s.events = append(s.events, err)
	return s
}

// scriptedDump plays scripts to a receiver on a clock of its own: each wait
// for an event with none queued moves the clock to its deadline.
type scriptedDump struct {
	mu      sync.Mutex
	now     time.Time
	scripts [][]any
	script  []any
	dials   []string
	// tick runs on every wait, at the clock's time
	tick func(now time.Time)
}

func (f *scriptedDump) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.now
}

func (f *scriptedDump) advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	now := f.now
	f.mu.Unlock()

	if f.tick != nil {
		f.tick(now)
	}
}

func (f *scriptedDump) dial(_ uint32, name string, pos int64) (binlogStream, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.dials = append(f.dials, fmt.Sprintf("%s:%d", name, pos))

	if len(f.scripts) == 0 {
		return nil, errors.New("no more connections")
	}

	f.script, f.scripts = f.scripts[0], f.scripts[1:]

	return f, nil
}

func (f *scriptedDump) next(_ context.Context, deadline time.Time) (*streamEvent, error) {
	f.mu.Lock()

	if len(f.script) > 0 {
		e := f.script[0]
		f.script = f.script[1:]
		f.mu.Unlock()

		if err, ok := e.(error); ok {
			return nil, err
		}

		return e.(*streamEvent), nil
	}

	f.mu.Unlock()
	f.advance(deadline.Sub(f.clock()))

	return nil, nil
}

func (f *scriptedDump) close() {}

type receiverFixture struct {
	*fixture
	dump    *scriptedDump
	spool   Spool
	stop    chan struct{}
	stopped sync.Once
	reports []ReceiverStatus
}

func newReceiverFixture(t *testing.T) *receiverFixture {
	f := newFixture(t)

	rf := &receiverFixture{fixture: f, dump: &scriptedDump{now: t0}, spool: Spool{Dir: t.TempDir()}, stop: make(chan struct{})}

	return rf
}

func (rf *receiverFixture) script(s *binlogScript) {
	rf.dump.scripts = append(rf.dump.scripts, s.events)
}

// stopAfter stops the receiver once d passed on its clock.
func (rf *receiverFixture) stopAfter(d time.Duration) {
	rf.dump.tick = func(now time.Time) {
		if now.Sub(t0) >= d {
			rf.stopped.Do(func() { close(rf.stop) })
		}
	}
}

func (rf *receiverFixture) receiver() *Receiver {
	return &Receiver{
		Spool: rf.spool,
		Walker: func() *backup.Walker {
			w := rf.walker("db-binlog")
			w.Root = rf.spool.Dir

			return w
		},
		DumpSet:     "db-dump",
		Window:      14 * 24 * time.Hour,
		CommitEvery: time.Minute,
		Report:      func(st ReceiverStatus) { rf.reports = append(rf.reports, st) },
		now:         rf.dump.clock,
		server:      rf.srv,
		dial:        rf.dump.dial,
		wait:        rf.dump.advance,
	}
}

func (rf *receiverFixture) run() error {
	return rf.receiver().Run(context.Background(), rf.stop)
}

func (rf *receiverFixture) node(chain int, name string) *proto.FileInfo {
	rf.t.Helper()

	ref, err := rf.index.LatestCommit(rf.ctx, "db-binlog")
	require.NoError(rf.t, err)

	held, err := readBinlogCommit(rf.ctx, rf.index, nil, ref)
	require.NoError(rf.t, err)

	for _, node := range held.chains[chain] {
		if string(node.Stat.Name) == name {
			return node.Stat
		}
	}

	rf.t.Fatalf("chain %d holds no %s", chain, name)

	return nil
}

func ts(d time.Duration) uint32 { return uint32(t0.Add(d).Unix()) }

func TestTheReceiverCommitsTheOpenBinlogAsPartialAndThenWhole(t *testing.T) {
	rf := newReceiverFixture(t)
	rf.mustDump()

	s := &binlogScript{serverID: 1}
	s.open("binlog.000001", ts(0)).event(2, ts(time.Second), 50).event(2, ts(2*time.Second), 50)
	rf.script(s)
	rf.stopAfter(90 * time.Second)

	require.NoError(t, rf.run())
	require.Equal(t, []string{"binlog.000001:4"}, rf.dump.dials, "receiving starts at the dump's binlog")
	require.Equal(t, map[int][]string{1: {"binlog.000001.partial"}}, rf.chains())

	commits, err := rf.index.CommitInfo(rf.ctx, "db-binlog", t0.Add(time.Hour), 1)
	require.NoError(t, err)
	require.Equal(t, "db-dump", commits[0].GetMetadata()[MetaDumpSet])
	require.Equal(t, "336h0m0s", commits[0].GetMetadata()[MetaWindow])
	require.Equal(t, backup.KindMySQLBinlog, commits[0].GetMetadata()[backup.MetaKind])

	partial := rf.node(1, "binlog.000001.partial")
	require.EqualValues(t, 4+100+50+50, partial.GetSize())
	require.Equal(t, t0.Add(2*time.Second), time.Unix(0, partial.GetMtimeNs()).UTC(), "the partial binlog's mtime is its last event's time")

	s = &binlogScript{serverID: 1, name: "binlog.000001", end: 204}
	s.event(2, ts(100*time.Second), 50).rotate(ts(101 * time.Second))
	s.open("binlog.000002", ts(101*time.Second)).event(2, ts(102*time.Second), 30)
	s.events = append([]any{&streamEvent{header: eventHeader{kind: rotateEvent, flags: artificialEvent}, next: "binlog.000001", pos: 204},
		streamed(eventBytes(formatDescriptionEvent, 1, ts(0), 0, 100, 0))}, s.events...)

	rf.dump.now = t0.Add(100 * time.Second)
	rf.dump.scripts = [][]any{s.events}
	rf.stop = make(chan struct{})
	rf.stopped = sync.Once{}
	rf.stopAfter(110 * time.Second)

	require.NoError(t, rf.run(), "the last commit is made on stop")
	require.Equal(t, "binlog.000001:204", rf.dump.dials[1], "receiving resumes where the spool's binlog ends")
	require.Equal(t, map[int][]string{1: {"binlog.000001", "binlog.000002.partial"}}, rf.chains())
	require.Equal(t, t0.Add(101*time.Second), time.Unix(0, rf.node(1, "binlog.000001").GetMtimeNs()).UTC(), "a whole binlog's mtime is its Rotate event's time")

	files, err := rf.spool.Files()
	require.NoError(t, err)
	require.Empty(t, files, "committed binlogs leave the spool")
}

func TestTheReceiverCommitsNothingWhileIdle(t *testing.T) {
	rf := newReceiverFixture(t)
	rf.mustDump()

	s := &binlogScript{serverID: 1}
	s.open("binlog.000001", ts(0))
	rf.script(s)

	commits := 0
	r := rf.receiver()
	r.Committed = func(*backup.WalkResult) { commits++ }

	rf.stopAfter(5 * time.Minute)
	require.NoError(t, r.Run(context.Background(), rf.stop))
	require.Equal(t, 1, commits, "the format description is committed once, and nothing after")
}

// covered lists what the binlog set's commits say their binlogs cover,
// newest first.
func (rf *receiverFixture) covered() [][2]time.Time {
	rf.t.Helper()

	commits, err := rf.index.CommitInfo(rf.ctx, "db-binlog", t0.Add(time.Hour), 100)
	require.NoError(rf.t, err)

	var covered [][2]time.Time
	for _, c := range commits {
		covered = append(covered, [2]time.Time{parseTime(c.GetMetadata()[MetaCoveredFrom]), parseTime(c.GetMetadata()[MetaCoveredTo])})
	}

	return covered
}

func TestABinlogCommitCoversItsBinlogsFromTheirFirstEventToTheirLast(t *testing.T) {
	rf := newReceiverFixture(t)
	rf.mustDump()

	s := &binlogScript{serverID: 1}
	s.open("binlog.000001", ts(0)).event(2, ts(time.Second), 50).rotate(ts(2 * time.Second))
	s.open("binlog.000002", ts(2*time.Second)).event(2, ts(3*time.Second), 50)
	rf.script(s)
	rf.stopAfter(90 * time.Second)

	require.NoError(t, rf.run())
	require.Equal(t, [2]time.Time{t0, t0.Add(3 * time.Second)}, rf.covered()[0])

	end := s.end
	s = &binlogScript{serverID: 1, name: "binlog.000002", end: end}
	s.event(2, ts(100*time.Second), 50)
	s.events = append([]any{&streamEvent{header: eventHeader{kind: rotateEvent, flags: artificialEvent}, next: "binlog.000002", pos: uint64(end)},
		streamed(eventBytes(formatDescriptionEvent, 1, ts(2*time.Second), 0, 100, 0))}, s.events...)

	rf.dump.now = t0.Add(100 * time.Second)
	rf.dump.scripts = [][]any{s.events}
	rf.stop = make(chan struct{})
	rf.stopped = sync.Once{}
	rf.stopAfter(110 * time.Second)

	require.NoError(t, rf.run())
	require.Equal(t, map[int][]string{1: {"binlog.000001", "binlog.000002.partial"}}, rf.chains())
	require.Equal(t, [2]time.Time{t0, t0.Add(100 * time.Second)}, rf.covered()[0], "the stretch still starts at the first binlog, which the spool no longer holds")
}

func TestHeartbeatsMoveAQuietBinlogSetsCoverageOn(t *testing.T) {
	rf := newReceiverFixture(t)
	rf.mustDump()

	s := &binlogScript{serverID: 1}
	s.open("binlog.000001", ts(0)).event(2, ts(time.Second), 50)
	rf.script(s)

	heartbeat := streamed(eventBytes(heartbeatEvent, 1, 0, s.end, eventHeaderLen+len("binlog.000001"), 0))
	stale := streamed(eventBytes(heartbeatEvent, 1, 0, s.end-50, eventHeaderLen+len("binlog.000001"), 0))

	// each beat waits for the commits before it, so each lands in its own,
	// without holding the clock: the receiver starts the next commit only as
	// the clock moves on
	beats := []struct {
		at      time.Duration
		stored  int
		message *streamEvent
	}{{2 * time.Minute, 1, heartbeat}, {3 * time.Minute, 2, stale}, {4 * time.Minute, 2, heartbeat}}

	var sent []time.Time

	rf.dump.tick = func(now time.Time) {
		if len(beats) > 0 && now.Sub(t0) >= beats[0].at && len(rf.covered()) >= beats[0].stored {
			rf.dump.mu.Lock()
			rf.dump.script = append(rf.dump.script, beats[0].message)
			rf.dump.mu.Unlock()

			sent = append(sent, now)
			beats = beats[1:]
		}

		if len(beats) == 0 && len(rf.covered()) >= 3 {
			rf.stopped.Do(func() { close(rf.stop) })
		}
	}

	require.NoError(t, rf.run())
	require.Equal(t, [][2]time.Time{
		{t0, sent[2]},
		{t0, sent[0]},
		{t0, t0.Add(time.Second)},
	}, rf.covered(), "a heartbeat at another position than the received binlog's end confirms nothing")
}

func TestTheCoveredStretchStartsAfterTheLastMissingBinlog(t *testing.T) {
	for _, tc := range []struct {
		names []string
		start string
	}{
		{[]string{"b.000001", "b.000002", "b.000003.partial"}, "b.000001"},
		{[]string{"b.000001", "b.000003", "b.000004"}, "b.000003"},
		{[]string{"b.000001", "b.000002.partial", "b.000005", "b.000006.partial"}, "b.000005"},
		{[]string{"b.000007.partial"}, "b.000007.partial"},
	} {
		require.Equal(t, tc.start, stretchStart(tc.names), tc.names)
	}
}

func TestATornEventIsReceivedAgain(t *testing.T) {
	spool := Spool{Dir: t.TempDir()}
	require.NoError(t, os.MkdirAll(spool.receiving(), 0o700))

	content := append([]byte{}, binlogMagic...)
	content = append(content, eventBytes(formatDescriptionEvent, 1, 10, 104, 100, 0)...)
	content = append(content, eventBytes(2, 1, 11, 154, 50, 0)...)
	content = append(content, eventBytes(2, 1, 12, 204, 50, 0)[:30]...)
	require.NoError(t, os.WriteFile(filepath.Join(spool.receiving(), "binlog.000003"), content, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(spool.receiving(), "binlog.000002"), content[:154], 0o600))

	w, err := openBinlogWriter(spool)
	require.NoError(t, err)
	defer w.stop()

	name, pos := w.position()
	require.Equal(t, "binlog.000003", name)
	require.EqualValues(t, 154, pos)
	require.EqualValues(t, 11, w.stamp)
	require.EqualValues(t, 10, w.created)

	files, err := spool.Files()
	require.NoError(t, err)
	require.Equal(t, []string{"binlog.000002"}, files, "a binlog before the open one is whole")
}

func TestARunIsCaughtUpOnceItReceivedWhereTheServerEnded(t *testing.T) {
	w, err := openBinlogWriter(Spool{Dir: t.TempDir()})
	require.NoError(t, err)
	defer w.stop()

	run := &receiveRun{w: w, from: "binlog.000002", fromPos: 300}
	require.False(t, run.caughtUp(), "without UntilCaughtUp")

	run.until = &Binlog{"binlog.000002", 300}
	require.True(t, run.caughtUp(), "nothing to receive past where receiving resumes")

	run.until = &Binlog{"binlog.000003", 194}
	require.False(t, run.caughtUp())

	require.NoError(t, w.begin("binlog.000003", 4))

	for _, event := range [][]byte{eventBytes(formatDescriptionEvent, 1, 10, 104, 100, 0), eventBytes(2, 1, 11, 154, 50, 0)} {
		h, _ := parseEventHeader(event)
		require.NoError(t, w.write(h, event))
	}

	require.False(t, run.caughtUp())

	rotate := eventBytes(rotateEvent, 1, 12, 194, 40, 0)
	h, _ := parseEventHeader(rotate)
	require.NoError(t, w.write(h, rotate))
	require.NoError(t, w.close())

	require.True(t, run.caughtUp(), "a binlog the server rotated away from is whole")
}

func TestAnEventThatDoesNotEndWhereTheBinlogDoesFails(t *testing.T) {
	w, err := openBinlogWriter(Spool{Dir: t.TempDir()})
	require.NoError(t, err)
	defer w.stop()

	require.NoError(t, w.begin("binlog.000001", 4))

	event := eventBytes(formatDescriptionEvent, 1, 10, 200, 100, 0)
	h, _ := parseEventHeader(event)
	require.Error(t, w.write(h, event))

	require.Error(t, w.begin("binlog.000001", 50), "the server must go on where the binlog ends")
	require.Error(t, w.begin("binlog.000002", 50), "a binlog is received from its t0")
}

func TestTheServerGoingBackStartsANewChain(t *testing.T) {
	rf := newReceiverFixture(t)
	rf.mustDump()

	s := &binlogScript{serverID: 1}
	s.open("binlog.000001", ts(0)).event(2, ts(time.Second), 50).rotate(ts(2 * time.Second))
	s.open("binlog.000002", ts(2*time.Second)).event(2, ts(3*time.Second), 50)
	s.open("binlog.000001", ts(4*time.Second))
	rf.script(s)

	s = &binlogScript{serverID: 1}
	s.open("binlog.000001", ts(4*time.Second)).event(2, ts(5*time.Second), 50)
	rf.script(s)

	rf.stopAfter(90 * time.Second)
	require.NoError(t, rf.run())
	require.Equal(t, []string{"binlog.000001:4", "binlog.000001:4"}, rf.dump.dials)

	chains := rf.chains()
	require.Equal(t, []string{"binlog.000001.partial"}, chains[2])

	n, ok, err := rf.spool.chain()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 2, n)
}

func TestConnectionsEndingRightAfterTheyStartMeanAnotherReplicaHasTheServerID(t *testing.T) {
	rf := newReceiverFixture(t)
	rf.mustDump()

	for range 3 {
		s := &binlogScript{serverID: 1}
		s.open("binlog.000001", ts(0)).fail(errors.New("EOF"))
		rf.script(s)
	}

	require.ErrorIs(t, rf.run(), ErrServerIDInUse)
}

func TestTheReceiverReconnectsWhereItStands(t *testing.T) {
	rf := newReceiverFixture(t)
	rf.mustDump()

	s := &binlogScript{serverID: 1}
	s.open("binlog.000001", ts(0)).event(2, ts(time.Second), 50).rotate(ts(2 * time.Second))
	s.fail(errors.New("connection reset"))
	rf.script(s)
	rf.script(&binlogScript{})

	rf.stopAfter(time.Minute)
	require.NoError(t, rf.run())
	require.Equal(t, []string{"binlog.000001:4", "binlog.000002:4"}, rf.dump.dials, "after a whole binlog, receiving goes on with the next")
}

func TestAServerIDInUseElsewhereIsRefused(t *testing.T) {
	rf := newReceiverFixture(t)
	rf.mustDump()

	r := rf.receiver()
	r.ServerID = 99
	rf.srv.replicas = [][]string{{"99", "elsewhere", "3306", "1"}}

	require.ErrorIs(t, r.Run(context.Background(), rf.stop), ErrServerIDInUse)

	r.ServerID = rf.srv.serverID
	require.ErrorIs(t, r.Run(context.Background(), rf.stop), ErrServerIDInUse, "the server's own id")
}

func TestReceivingNeedsADumpToStartFrom(t *testing.T) {
	rf := newReceiverFixture(t)

	require.ErrorContains(t, rf.run(), "take a dump first")
}

func TestResumePoints(t *testing.T) {
	for _, tc := range []struct {
		name    string
		open    string
		end     int64
		names   []string
		dump    string
		want    string
		wantPos int64
	}{
		{"open binlog", "b.000003", 500, []string{"b.000002"}, "b.000001", "b.000003", 500},
		{"after the newest whole binlog", "", 0, []string{"b.000001", "b.000002"}, "b.000001", "b.000003", 4},
		{"a partial binlog again from its t0", "", 0, []string{"b.000001", "b.000002.partial"}, "", "b.000002", 4},
		{"the whole binlog wins over its partial", "", 0, []string{"b.000002.partial", "b.000002"}, "", "b.000003", 4},
		{"the dump's binlog", "", 0, nil, "b.000007", "b.000007", 4},
		{"past six digits", "", 0, []string{"b.999999"}, "", "b.1000000", 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name, pos, ok := resumePoint(tc.open, tc.end, tc.names, tc.dump)
			require.True(t, ok)
			require.Equal(t, tc.want, name)
			require.Equal(t, tc.wantPos, pos)
		})
	}

	_, _, ok := resumePoint("", 0, nil, "")
	require.False(t, ok)
}

func TestErrorNoBinlogFileIsClassified(t *testing.T) {
	logs := []Binlog{{"b.000005", 1000}, {"b.000006", 300}}

	require.Equal(t, lostPurged, classifyLost(logs, "b.000003", 200))
	require.Equal(t, lostReset, classifyLost([]Binlog{{"b.000001", 300}}, "b.000003", 200))
	require.Equal(t, lostReset, classifyLost([]Binlog{{"other.000001", 300}}, "b.000003", 200))
	require.Equal(t, lostTail, classifyLost(logs, "b.000005", 2000))
	require.Equal(t, lostUnknown, classifyLost(logs, "b.000006", 200))
}

func TestLagIsWhatTheServerHoldsPastTheCommit(t *testing.T) {
	logs := []Binlog{{"b.000005", 1000}, {"b.000006", 300}, {"b.000007", 200}}

	require.EqualValues(t, 400+300+200, lagBytes(logs, "b.000005", 600))
	require.EqualValues(t, 1500, lagBytes(logs, "", 0))
}

func TestAPartialBinlogMustEndTheChain(t *testing.T) {
	require.NoError(t, checkContiguous([]string{"b.000001", "b.000002.partial"}, ""))
	require.ErrorIs(t, checkContiguous([]string{"b.000001.partial", "b.000002"}, ""), ErrGap)
	require.NoError(t, checkContiguous([]string{"b.000001.partial", "b.000004"}, "b.000004"), "a purged binlog's partial before the newest dump")
	require.NoError(t, checkContiguous([]string{"b.000003", "b.000004.partial"}, "b.000004"), "the dump's binlog may be partial")
}

func TestReplicaServerIDsAreHighAndStable(t *testing.T) {
	id := ReplicaServerID("db-binlog")
	require.GreaterOrEqual(t, id, uint32(1<<31))
	require.Equal(t, id, ReplicaServerID("db-binlog"))
	require.NotEqual(t, id, ReplicaServerID("other-binlog"))
}

func TestRestoreReplaysAPartialBinlogLastUnderItsBinlogsName(t *testing.T) {
	rf := newReceiverFixture(t)
	dump := rf.mustDump()

	s := &binlogScript{serverID: 1}
	s.open("binlog.000001", ts(0)).event(2, ts(time.Second), 50).rotate(ts(2 * time.Second))
	s.open("binlog.000002", ts(2*time.Second)).event(2, ts(3*time.Second), 50)
	rf.script(s)
	rf.stopAfter(90 * time.Second)
	require.NoError(t, rf.run())

	result, err := rf.restore("db-binlog", time.Time{})
	require.NoError(t, err)
	require.Equal(t, []string{"binlog.000001", "binlog.000002"}, result.Binlogs)
	require.Equal(t, "--start-position="+dump.Commit.Metadata[MetaBinlogPos], rf.srv.replayed[0][0])

	result, err = rf.restore("db-binlog", t0.Add(1500*time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, []string{"binlog.000001"}, result.Binlogs, "a moment before the rotation needs no partial binlog")
}

func TestRestoreRefusesToReplayPastAPartialBinlog(t *testing.T) {
	r := &Restore{}
	dump := &proto.Commit{Metadata: map[string]string{MetaBinlogFile: "b.000001", MetaBinlogPos: "4", MetaServerID: "1"}}
	binlogs := &binlogCommit{chain: 1, chains: map[int][]*proto.TreeNode{1: {
		{Stat: &proto.FileInfo{Name: []byte("b.000001.partial")}},
		{Stat: &proto.FileInfo{Name: []byte("b.000002")}},
	}}}

	_, reason := r.unusable(dump, binlogs)
	require.NotEmpty(t, reason)
}

// metadataReads counts the commits, trees and files read from the store.
type metadataReads struct {
	*sql.Index
	reads atomic.Int64
}

func (m *metadataReads) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := m.Index.Get(ctx, ref)
	if err == nil && obj.Type().Metadata() {
		m.reads.Add(1)
	}

	return obj, err
}

func TestAReceiversSteadyCommitsReadNoMetadataWhateverCacheItsCallerSetUp(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached=%v", cached), func(t *testing.T) {
			rf := newReceiverFixture(t)
			rf.mustDump()

			s := &binlogScript{serverID: 1}
			s.open("binlog.000001", ts(0))
			opened := len(s.events)

			for i := 1; i <= 5; i++ {
				s.event(2, ts(time.Duration(i)*70*time.Second), 50)
			}

			rf.dump.scripts = [][]any{s.events[:opened]}

			var commits atomic.Int64

			released := 0
			rf.dump.tick = func(time.Time) {
				rf.dump.mu.Lock()
				defer rf.dump.mu.Unlock()

				if int64(released) < commits.Load() && released < 5 {
					rf.dump.script = append(rf.dump.script, s.events[opened+released])
					released++
				}

				if commits.Load() > 5 {
					rf.stopped.Do(func() { close(rf.stop) })
				}
			}

			counted := &metadataReads{Index: rf.index}
			objects := backup.ObjectStore(counted)
			if cached {
				objects = cache.New(cache.NewMemory(), counted)
			}

			r := rf.receiver()
			r.Walker = func() *backup.Walker {
				w := rf.walker("db-binlog")
				w.Root, w.Objects = rf.spool.Dir, objects

				return w
			}

			var reads []int64
			r.Committed = func(*backup.WalkResult) {
				reads = append(reads, counted.reads.Swap(0))
				commits.Add(1)
			}

			require.NoError(t, r.Run(context.Background(), rf.stop))
			require.Len(t, reads, 6)

			for i, n := range reads[1:] {
				require.Zero(t, n, "commit %d read what the last one wrote", i+1)
			}

			require.EqualValues(t, 4+100+5*50, rf.node(1, "binlog.000001.partial").GetSize())
		})
	}
}

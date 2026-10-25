package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// fakeStream plays a script of messages to a session on a clock of its
// own: each wait for a message with none queued moves the clock to its
// deadline.
type fakeStream struct {
	mu     sync.Mutex
	now    time.Time
	script []any
	// tick runs on every wait, at the clock's time.
	tick   func(now time.Time)
	events []string
	sent   []sentStatus
	// busy counts commits under way that end without the clock moving;
	// waits hold the clock until they did.
	busy atomic.Int32
}

// settle makes the clock wait for each of the session's commits.
func (f *fakeStream) settle(s *session) {
	s.r.hook = func(point string) error {
		if point == "cut" {
			f.busy.Add(1)
		}

		return nil
	}

	commit := s.commit
	s.commit = func(ctx context.Context, c *cut) (*backup.WalkResult, error) {
		defer f.busy.Add(-1)
		return commit(ctx, c)
	}
}

type sentStatus struct {
	at           time.Time
	write, flush uint64
}

func (f *fakeStream) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.now
}

func (f *fakeStream) log(event string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.events = append(f.events, event)
}

func (f *fakeStream) next(_ context.Context, deadline time.Time) (any, error) {
	for f.busy.Load() > 0 {
		time.Sleep(time.Millisecond)
	}

	f.mu.Lock()

	if len(f.script) > 0 {
		m := f.script[0]
		f.script = f.script[1:]
		f.mu.Unlock()

		return m, nil
	}

	f.now = deadline
	now := f.now
	f.mu.Unlock()

	if f.tick != nil {
		f.tick(now)
	}

	return nil, nil
}

func (f *fakeStream) status(write, flush uint64, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sent = append(f.sent, sentStatus{at: f.now, write: write, flush: flush})
	f.events = append(f.events, fmt.Sprintf("status %d", flush))

	return nil
}

func (f *fakeStream) follow(context.Context, uint32, uint64) error { return nil }

func newSession(t *testing.T, f *fakeStream, commit func(context.Context, *cut) (*backup.WalkResult, error)) *session {
	t.Helper()

	f.now = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

	r := &Receiver{Spool: Spool{Dir: t.TempDir()}, CommitEvery: time.Minute, now: f.clock}
	require.NoError(t, r.setDefaults())

	w, err := newSegmentWriter(r.Spool, segSize, 1, 0)
	require.NoError(t, err)
	t.Cleanup(w.stop)

	return &session{r: r, stream: f, w: w, commit: commit}
}

func TestTheServerHearsAFlushOnlyOnceTheCommitIsStored(t *testing.T) {
	f := &fakeStream{script: []any{xlogData{start: 0, data: wal(0, 100)}}}

	release := make(chan struct{})
	stop := make(chan struct{})

	var once, stopOnce sync.Once

	f.tick = func(now time.Time) {
		elapsed := now.Sub(time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC))
		if elapsed >= 3*time.Minute {
			once.Do(func() { close(release) })
		}

		if elapsed >= 5*time.Minute {
			stopOnce.Do(func() { close(stop) })
		}
	}

	var cuts []*cut

	s := newSession(t, f, func(_ context.Context, c *cut) (*backup.WalkResult, error) {
		cuts = append(cuts, c)
		f.log("commit started")
		<-release
		f.log("commit stored")

		return &backup.WalkResult{Ref: &proto.Ref{Hash: []byte{1}}}, nil
	})

	require.NoError(t, s.run(context.Background(), stop))

	require.Len(t, cuts, 1, "one commit: nothing new arrived after it")
	require.Equal(t, uint64(100), cuts[0].end)

	stored, flushed := false, false
	for _, e := range f.events {
		switch e {
		case "commit stored":
			stored = true
		case "status 100":
			require.True(t, stored, "a flush the store doesn't hold yet")
			flushed = true
		case "status 0":
			require.False(t, flushed, "the flush went back")
		}
	}

	require.Equal(t, "status 100", f.events[len(f.events)-1])

	for i, st := range f.sent[1:] {
		require.Equal(t, uint64(100), st.write, "the write position is what the spool holds")
		require.LessOrEqual(t, st.at.Sub(f.sent[i].at), statusInterval+receiveWait, "the server hears from the receiver while the store is slow")
	}
}

func TestAKeepaliveAskingForAReplyGetsOne(t *testing.T) {
	f := &fakeStream{}
	stop := make(chan struct{})

	var asked time.Time

	f.tick = func(now time.Time) {
		elapsed := now.Sub(time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC))

		switch elapsed {
		case 3 * time.Second:
			asked = now
			f.mu.Lock()
			f.script = append(f.script, keepalive{reply: true})
			f.mu.Unlock()
		case 20 * time.Second:
			close(stop)
		}
	}

	s := newSession(t, f, func(context.Context, *cut) (*backup.WalkResult, error) {
		return nil, errors.New("nothing to commit")
	})

	require.NoError(t, s.run(context.Background(), stop))
	require.True(t, slicesContainsFunc(f.sent, func(st sentStatus) bool { return st.at.Equal(asked) }), "a reply when the keepalive came")
}

func slicesContainsFunc(sent []sentStatus, f func(sentStatus) bool) bool {
	for _, st := range sent {
		if f(st) {
			return true
		}
	}

	return false
}

func TestTheServerConfirmsWhatACaughtUpSessionHolds(t *testing.T) {
	f := &fakeStream{}
	start := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	stop := make(chan struct{})

	f.script = []any{
		xlogData{start: 0, data: wal(0, 100), serverEnd: 200, sent: start},
		xlogData{start: 100, data: wal(100, 100), serverEnd: 200, sent: start.Add(time.Second)},
	}

	later := map[time.Duration]any{
		30 * time.Second:  keepalive{serverEnd: 200, sent: start.Add(30 * time.Second)},
		90 * time.Second:  xlogData{start: 200, data: wal(200, 100), serverEnd: 400, sent: start.Add(90 * time.Second)},
		100 * time.Second: keepalive{serverEnd: 300, sent: start.Add(100 * time.Second)},
		160 * time.Second: xlogData{start: 300, data: wal(300, 100), serverEnd: 400, sent: start.Add(160 * time.Second)},
		250 * time.Second: keepalive{serverEnd: 400, sent: start.Add(250 * time.Second)},
	}

	f.tick = func(now time.Time) {
		if m, ok := later[now.Sub(start)]; ok {
			f.mu.Lock()
			f.script = append(f.script, m)
			f.mu.Unlock()
		}

		if now.Sub(start) == 400*time.Second {
			close(stop)
		}
	}

	var confirmed []time.Duration

	s := newSession(t, f, func(_ context.Context, c *cut) (*backup.WalkResult, error) {
		confirmed = append(confirmed, c.confirmed.Sub(start))
		return &backup.WalkResult{Ref: &proto.Ref{Hash: []byte{1}}}, nil
	})
	f.settle(s)

	require.NoError(t, s.run(context.Background(), stop))
	require.Equal(t, []time.Duration{30 * time.Second, 30 * time.Second, 160 * time.Second, 250 * time.Second}, confirmed,
		"a keepalive confirms nothing while WAL is still coming, and alone it makes a commit")
}

func TestAFailedCommitKeepsTheFlushAndIsRetried(t *testing.T) {
	f := &fakeStream{script: []any{xlogData{start: 0, data: wal(0, 100)}}}
	stop := make(chan struct{})

	f.tick = func(now time.Time) {
		if now.Sub(time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)) == 10*time.Minute {
			close(stop)
		}
	}

	var attempts int

	s := newSession(t, f, func(context.Context, *cut) (*backup.WalkResult, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("the store is down")
		}

		return &backup.WalkResult{Ref: &proto.Ref{Hash: []byte{1}}}, nil
	})
	f.settle(s)

	require.NoError(t, s.run(context.Background(), stop))
	require.Equal(t, 3, attempts)
	require.Equal(t, uint64(100), s.flushed)
}

func TestAFailedCommitWhileTheServerShutsDownEndsTheSession(t *testing.T) {
	f := &fakeStream{script: []any{xlogData{start: 0, data: wal(0, 100)}}}
	stop := make(chan struct{})
	start := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

	f.tick = func(now time.Time) {
		if now.Sub(start) == time.Hour {
			close(stop)
		}
	}

	var attempts int

	s := newSession(t, f, func(context.Context, *cut) (*backup.WalkResult, error) {
		attempts++
		return nil, errors.New("the store is down")
	})
	f.settle(s)

	down := make(chan struct{})
	close(down)
	s.down = down

	err := s.run(context.Background(), stop)
	require.ErrorContains(t, err, "the store is down")
	require.Equal(t, 1, attempts)
	require.Less(t, f.clock().Sub(start), time.Minute, "the shutdown waited for the store")
	require.True(t, s.retry, "the WAL waits in the spool")
}

func TestACommitThatHangsWhileTheServerShutsDownIsAbandoned(t *testing.T) {
	f := &fakeStream{script: []any{xlogData{start: 0, data: wal(0, 100)}}}
	stop := make(chan struct{})
	start := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

	f.tick = func(now time.Time) {
		if now.Sub(start) == time.Hour {
			close(stop)
		}
	}

	s := newSession(t, f, func(ctx context.Context, _ *cut) (*backup.WalkResult, error) {
		for f.clock().Sub(start) < time.Hour {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}

		return nil, errors.New("the store is slow")
	})

	down := make(chan struct{})
	close(down)
	s.down = down

	err := s.run(context.Background(), stop)
	require.Error(t, err)
	require.Less(t, f.clock().Sub(start), time.Minute, "the shutdown waited for the store")
}

func TestTheLastCommitHappensOnStop(t *testing.T) {
	stop := make(chan struct{})
	f := &fakeStream{script: []any{xlogData{start: 0, data: wal(0, 100)}}}
	f.tick = func(time.Time) {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}

	var commits int

	s := newSession(t, f, func(context.Context, *cut) (*backup.WalkResult, error) {
		commits++
		return &backup.WalkResult{Ref: &proto.Ref{Hash: []byte{1}}}, nil
	})

	require.NoError(t, s.run(context.Background(), stop))
	require.Equal(t, 1, commits, "before a minute passed")
	require.Equal(t, sentStatus{at: f.now, write: 100, flush: 100}, f.sent[len(f.sent)-1])
}

func TestASessionUntilCaughtUpCommitsWhatItReceivedAndEnds(t *testing.T) {
	f := &fakeStream{script: []any{xlogData{start: 0, data: wal(0, 100)}, xlogData{start: 100, data: wal(100, 100)}}}

	var ends []uint64

	s := newSession(t, f, func(_ context.Context, c *cut) (*backup.WalkResult, error) {
		ends = append(ends, c.end)
		return &backup.WalkResult{Ref: &proto.Ref{Hash: []byte{1}}}, nil
	})
	s.until = 150

	require.NoError(t, s.run(context.Background(), make(chan struct{})))
	require.Equal(t, []uint64{200}, ends)
	require.Equal(t, uint64(200), f.sent[len(f.sent)-1].flush)
}

func TestASessionUntilCaughtUpWithNothingNewCommitsNothing(t *testing.T) {
	f := &fakeStream{script: []any{xlogData{start: 0, data: wal(0, 100)}}}

	s := newSession(t, f, func(context.Context, *cut) (*backup.WalkResult, error) {
		t.Fatal("a commit of WAL the set holds")
		return nil, nil
	})
	s.flushed, s.until = 100, 100

	require.NoError(t, s.run(context.Background(), make(chan struct{})))
	require.Equal(t, uint64(100), f.sent[len(f.sent)-1].flush)
}

func TestAFlushBehindTheSlotIsNotReported(t *testing.T) {
	s := &session{flushed: 100, floor: 200}
	require.Zero(t, s.flush(), "it would move the slot back onto removed WAL")

	s.flushed = 300
	require.Equal(t, uint64(300), s.flush())
}

func TestCommitsRunOnTheTimerSoonerForBytesNeverWithinTheFloor(t *testing.T) {
	w := &segmentWriter{}
	s := &session{r: &Receiver{CommitEvery: time.Minute, CommitBytes: 1000, floor: 10 * time.Second}, w: w}

	require.False(t, s.due(time.Hour, false, 0), "nothing new")

	w.written = 10
	require.False(t, s.due(59*time.Second, true, 0))
	require.True(t, s.due(time.Minute, true, 0))

	w.written = 1000
	require.False(t, s.due(9*time.Second, true, 0), "the floor")
	require.True(t, s.due(10*time.Second, true, 0), "enough bytes")

	s.flushed = 500
	require.False(t, s.due(10*time.Second, true, 0), "bytes count from the last stored commit")

	w.written = 10
	s.flushed = 0
	require.False(t, s.due(time.Minute, true, 7), "backing off after failures")
	require.True(t, s.due(maxCommitBackoff, true, 30))
}

func TestACommitIntervalBelowTheFloorIsRefused(t *testing.T) {
	r := &Receiver{CommitEvery: 5 * time.Second}
	require.Error(t, r.setDefaults())
}

func TestTheSlotIsCreatedRecreatedOrRefused(t *testing.T) {
	action, err := slotAction("s", slotState{}, false)
	require.NoError(t, err)
	require.Equal(t, slotCreate, action)

	action, err = slotAction("s", slotState{physical: true, walStatus: "reserved"}, true)
	require.NoError(t, err)
	require.Equal(t, slotKeep, action)

	action, err = slotAction("s", slotState{physical: true, walStatus: "lost"}, true)
	require.NoError(t, err)
	require.Equal(t, slotRecreate, action)

	_, err = slotAction("s", slotState{physical: true, active: true}, true)
	require.ErrorIs(t, err, ErrSlotActive)

	_, err = slotAction("s", slotState{}, true)
	require.Error(t, err, "a logical slot")
}

func TestTheSlotWarnsBeforeItsCap(t *testing.T) {
	const cap = 1 << 30

	require.Empty(t, slotAlert(slotState{walStatus: "reserved", safeWALSize: cap / 2}, cap))
	require.Equal(t, "low", slotAlert(slotState{walStatus: "reserved", safeWALSize: cap/4 - 1}, cap))
	require.Equal(t, "extended", slotAlert(slotState{walStatus: "extended", safeWALSize: cap / 2}, cap))
	require.Equal(t, "unreserved", slotAlert(slotState{walStatus: "unreserved"}, cap))
	require.Empty(t, slotAlert(slotState{walStatus: "reserved", safeWALSize: -1}, -1))
}

func TestSlotNamesComeFromTheSet(t *testing.T) {
	require.Equal(t, "goback_db_wal", SlotName("DB-wal"))
	require.Len(t, SlotName(string(make([]byte, 100))), 63)
	require.NoError(t, checkSlotName(SlotName("prod.db/wal")))
	require.Error(t, checkSlotName("Bad-Name"))
}

func TestStreamingResumesAfterTheNewestWholeSegment(t *testing.T) {
	resume := func(names ...string) (position, bool) {
		from, _, _, ok := history(nil).reach(names, segSize)
		return from, ok
	}

	_, ok := resume("00000001.history")
	require.False(t, ok)

	from, ok := resume(walName(1, 3), walName(1, 4), walName(1, 2))
	require.True(t, ok)
	require.Equal(t, position{timeline: 1, lsn: 5 * segSize}, from)

	from, _ = resume(walName(1, 4), walName(1, 5)+partialSuffix)
	require.Equal(t, position{timeline: 1, lsn: 5 * segSize}, from, "the open segment is received again from its start")

	from, _ = resume(walName(1, 4), walName(1, 5)+partialSuffix, "00000002.history", walName(2, 5)+partialSuffix)
	require.Equal(t, position{timeline: 2, lsn: 5 * segSize}, from, "the newer timeline")
}

// fork is the history of timeline 2 after the cluster was restored to
// 0/3800000, inside segment 3.
func fork(t *testing.T) history {
	t.Helper()

	h, err := parseHistory(2, []byte("1\t0/3800000\tbefore 2026-10-12 10:00:00+00\n"))
	require.NoError(t, err)

	return h
}

func TestAHistoryFileLaysOutWhichTimelineHoldsEachLSN(t *testing.T) {
	h := fork(t)
	require.Equal(t, uint32(1), h.at(0x37FFFFF))
	require.Equal(t, uint32(2), h.at(0x3800000))

	require.True(t, h.holds(walName(1, 3), segSize), "up to the fork")
	require.False(t, h.holds(walName(1, 4), segSize), "past it")
	require.True(t, h.holds(walName(2, 3)+partialSuffix, segSize))
	require.False(t, h.holds(walName(3, 3), segSize), "a timeline it doesn't name")

	// timeline 3 forked off timeline 1 before timeline 2 did
	again, err := parseHistory(3, []byte("1\t0/5800000\tbefore\n\n2\t0/3800000\tbefore\n"))
	require.NoError(t, err)

	require.Equal(t, uint32(1), again.at(0x37FFFFF))
	require.Equal(t, uint32(3), again.at(0x4000000))
	require.False(t, again.holds(walName(2, 5), segSize), "timeline 2 holds nothing of it")
	require.False(t, again.holds(walName(1, 4), segSize))

	_, err = parseHistory(2, []byte("2\t0/3800000\tbefore\n"))
	require.Error(t, err, "a history naming its own timeline")
}

func TestTheReceiverResumesAtTheForkAfterARestoreToAnEarlierMoment(t *testing.T) {
	one := timelineHistory(1)
	set := []string{walName(1, 1), walName(1, 2), walName(1, 3), walName(1, 4), walName(1, 5), walName(1, 6) + partialSuffix}

	res, err := resumeAt(set, one, one, segSize)
	require.NoError(t, err)
	require.Equal(t, &resumption{position: position{timeline: 1, lsn: 6 * segSize}}, res, "the same history")

	res, err = resumeAt(set, one, fork(t), segSize)
	require.NoError(t, err)
	require.Equal(t, &resumption{position: position{timeline: 2, lsn: 3 * segSize}, fork: true}, res, "at the start of the segment it forked in, on the new timeline")

	promoted, err := parseHistory(2, []byte("1\t0/7000100\tno recovery target specified\n"))
	require.NoError(t, err)

	res, err = resumeAt(set, one, promoted, segSize)
	require.NoError(t, err)
	require.Equal(t, &resumption{position: position{timeline: 1, lsn: 6 * segSize}}, res, "a switch past the set's end is followed as it streams")

	forked := append(slices.Clone(set), "00000002.history", walName(2, 3), walName(2, 4)+partialSuffix)
	res, err = resumeAt(forked, fork(t), fork(t), segSize)
	require.NoError(t, err)
	require.Equal(t, &resumption{position: position{timeline: 2, lsn: 4 * segSize}}, res, "on the new timeline once the set follows it")

	res, err = resumeAt(append(slices.Clone(set), "00000002.history"), fork(t), fork(t), segSize)
	require.NoError(t, err)
	require.Equal(t, &resumption{position: position{timeline: 2, lsn: 3 * segSize}}, res, "the new timeline's history alone")

	_, err = resumeAt(forked, fork(t), one, segSize)
	require.ErrorIs(t, err, ErrGap, "the server went on on a timeline the set left")

	_, err = resumeAt([]string{walName(1, 8), walName(1, 9)}, one, fork(t), segSize)

	var gap *gapError
	require.ErrorAs(t, err, &gap, "a fork before the oldest WAL the set holds")
	require.Equal(t, uint64(0x3800000), gap.from)

	again, err := parseHistory(3, []byte("1\t0/3800000\tbefore\n2\t0/2000000\tbefore\n"))
	require.NoError(t, err)

	twice := []string{walName(1, 1), walName(1, 2), walName(1, 3), "00000002.history", walName(2, 3), walName(2, 4), walName(2, 5) + partialSuffix}
	res, err = resumeAt(twice, fork(t), again, segSize)
	require.NoError(t, err)
	require.Equal(t, &resumption{position: position{timeline: 3, lsn: 2 * segSize}, fork: true}, res, "restored to before the first fork")
}

func TestAGapIsBridgedOnlyByALaterBaseBackupOnTheServersHistory(t *testing.T) {
	starts := []string{walName(1, 2), walName(1, 9), walName(1, 7), ""}

	timeline, lsn, ok := gapStart(starts, 5*segSize, timelineHistory(1), segSize)
	require.True(t, ok)
	require.Equal(t, uint32(1), timeline)
	require.Equal(t, uint64(7*segSize), lsn, "the oldest base after the gap")

	_, _, ok = gapStart(starts, 9*segSize, timelineHistory(1), segSize)
	require.False(t, ok)

	timeline, lsn, ok = gapStart(append(starts, walName(2, 8)), 3*segSize, fork(t), segSize)
	require.True(t, ok)
	require.Equal(t, uint32(2), timeline, "not one on the timeline the server left")
	require.Equal(t, uint64(8*segSize), lsn)
}

func TestTheStatusUpdateCarriesFlushAsApply(t *testing.T) {
	at := time.Date(2000, 1, 1, 0, 0, 1, 0, time.UTC)
	data := encodeStatus(300, 0, at, true)

	require.Len(t, data, 34)
	require.Equal(t, byte('r'), data[0])
	require.Equal(t, uint64(300), binary.BigEndian.Uint64(data[1:]))
	require.Zero(t, binary.BigEndian.Uint64(data[9:]), "a zero flush stays zero")
	require.Zero(t, binary.BigEndian.Uint64(data[17:]), "apply is flush")
	require.Equal(t, uint64(1000000), binary.BigEndian.Uint64(data[25:]))
	require.Equal(t, byte(1), data[33])
}

package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
	_, _, ok := resumePoint([]string{"00000001.history"}, segSize)
	require.False(t, ok)

	timeline, lsn, ok := resumePoint([]string{walName(1, 3), walName(1, 4), walName(1, 2)}, segSize)
	require.True(t, ok)
	require.Equal(t, uint32(1), timeline)
	require.Equal(t, uint64(5*segSize), lsn)

	timeline, lsn, _ = resumePoint([]string{walName(1, 4), walName(1, 5) + partialSuffix}, segSize)
	require.Equal(t, uint32(1), timeline)
	require.Equal(t, uint64(5*segSize), lsn, "the open segment is received again from its start")

	timeline, lsn, _ = resumePoint([]string{walName(1, 4), walName(1, 5) + partialSuffix, "00000002.history", walName(2, 5) + partialSuffix}, segSize)
	require.Equal(t, uint32(2), timeline, "the newer timeline")
	require.Equal(t, uint64(5*segSize), lsn)
}

func TestAGapIsBridgedOnlyByALaterBaseBackup(t *testing.T) {
	starts := []string{walName(1, 2), walName(1, 9), walName(1, 7), ""}

	timeline, lsn, ok := gapStart(starts, 5*segSize, segSize)
	require.True(t, ok)
	require.Equal(t, uint32(1), timeline)
	require.Equal(t, uint64(7*segSize), lsn, "the oldest base after the gap")

	_, _, ok = gapStart(starts, 9*segSize, segSize)
	require.False(t, ok)
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

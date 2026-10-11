package postgres

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// wal is WAL of timeline 1 from lsn on, n bytes long, starting with a
// segment header where lsn starts a segment.
func wal(lsn uint64, n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(lsn + uint64(i))
	}

	if lsn%segSize == 0 && n >= longPageHeaderLen {
		copy(data, header(1, lsn, 42))
	}

	return data
}

func newWriter(t *testing.T, from uint64) (*segmentWriter, Spool) {
	t.Helper()

	spool := Spool{Dir: t.TempDir()}
	w, err := newSegmentWriter(spool, segSize, 1, from)
	require.NoError(t, err)
	t.Cleanup(w.stop)

	return w, spool
}

func TestStreamingStartsOnlyAtASegmentStart(t *testing.T) {
	_, err := newSegmentWriter(Spool{Dir: t.TempDir()}, segSize, 1, segSize+8)
	require.Error(t, err)
}

func TestTheOpenSegmentIsWrittenInPlaceAndCutAtTheLastMessage(t *testing.T) {
	w, spool := newWriter(t, segSize)

	require.NoError(t, w.write(segSize, wal(segSize, 100)))
	require.NoError(t, w.write(segSize+100, wal(segSize+100, 50)))
	require.Error(t, w.write(segSize+200, wal(segSize+200, 1)), "WAL that skips bytes")

	c, err := w.cut()
	require.NoError(t, err)
	require.Empty(t, c.files, "the open segment is not in the spool")
	require.NotNil(t, c.files)
	require.Equal(t, uint64(segSize+150), c.end)
	require.Equal(t, &OpenSegment{Name: walName(1, 1), Path: filepath.Join(spool.Dir, receivingDir, walName(1, 1)), Size: 150}, c.open)

	content, err := os.ReadFile(c.open.Path)
	require.NoError(t, err)
	require.Equal(t, append(wal(segSize, 100), wal(segSize+100, 50)...), content)

	files, err := spool.Files()
	require.NoError(t, err)
	require.Empty(t, files, "Spool.Files skips the receiving directory")
}

func TestASegmentBarelyBegunIsLeftOutOfTheCut(t *testing.T) {
	w, _ := newWriter(t, 0)

	require.NoError(t, w.write(0, wal(0, longPageHeaderLen-1)))

	c, err := w.cut()
	require.NoError(t, err)
	require.Nil(t, c.open)
	require.Zero(t, c.end)
}

func TestAFullSegmentMovesIntoTheSpool(t *testing.T) {
	w, spool := newWriter(t, 0)

	require.NoError(t, w.write(0, wal(0, segSize+100)))

	files, err := spool.Files()
	require.NoError(t, err)
	require.Equal(t, []string{walName(1, 0)}, files)

	c, err := w.cut()
	require.NoError(t, err)
	require.Equal(t, []string{walName(1, 0)}, c.files)
	require.Equal(t, walName(1, 1), c.open.Name)
	require.Equal(t, int64(100), c.open.Size)
}

func TestSegmentsThatFillWhileACommitReadsWaitForIt(t *testing.T) {
	w, spool := newWriter(t, 0)

	require.NoError(t, w.write(0, wal(0, 100)))

	c, err := w.cut()
	require.NoError(t, err)
	require.Equal(t, walName(1, 0), c.open.Name)

	require.NoError(t, w.write(100, wal(100, 2*segSize)))

	files, err := spool.Files()
	require.NoError(t, err)
	require.Empty(t, files, "the commit still reads the first segment where it was")

	_, err = os.Stat(c.open.Path)
	require.NoError(t, err)

	require.NoError(t, w.release())

	files, err = spool.Files()
	require.NoError(t, err)
	require.Equal(t, []string{walName(1, 0), walName(1, 1)}, files, "in order, once the commit is done")
}

func TestASegmentReceivedAgainMustMatchTheSpool(t *testing.T) {
	w, spool := newWriter(t, 0)
	require.NoError(t, w.write(0, wal(0, segSize)))
	w.stop()

	again, err := newSegmentWriter(spool, segSize, 1, 0)
	require.NoError(t, err)
	require.NoError(t, again.write(0, wal(0, segSize)), "the same bytes")

	different := wal(0, segSize)
	different[segSize-1]++

	again, err = newSegmentWriter(spool, segSize, 1, 0)
	require.NoError(t, err)
	require.ErrorIs(t, again.write(0, different), ErrConflict)
}

func TestAReceivedSegmentIsRewrittenFromItsStart(t *testing.T) {
	w, spool := newWriter(t, 0)
	require.NoError(t, w.write(0, wal(0, 5000)))
	w.stop()

	again, err := newSegmentWriter(spool, segSize, 1, 0)
	require.NoError(t, err)
	t.Cleanup(again.stop)
	require.NoError(t, again.write(0, wal(0, 100)))

	c, err := again.cut()
	require.NoError(t, err)

	info, err := os.Stat(c.open.Path)
	require.NoError(t, err)
	require.Equal(t, int64(100), info.Size(), "no stale tail of the earlier run")
}

func TestATimelineSwitchClosesTheOpenSegmentAsPartial(t *testing.T) {
	w, spool := newWriter(t, 0)
	require.NoError(t, w.write(0, wal(0, 1000)))

	require.NoError(t, w.switchTimeline(2, 900))
	require.Equal(t, uint64(0), w.written, "the new timeline streams from its first segment's start")

	files, err := spool.Files()
	require.NoError(t, err)
	require.Equal(t, []string{walName(1, 0) + partialSuffix}, files)

	require.NoError(t, w.write(0, wal(0, 2000)))

	c, err := w.cut()
	require.NoError(t, err)
	require.Equal(t, walName(2, 0), c.open.Name)
}

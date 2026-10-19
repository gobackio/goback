package backup

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAWalkReportsWhatItHasCoveredWhileItRuns(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("hello"))
	f.write("sub/b.bin", f.random(150<<10))
	f.write("sub/deep/c.txt", []byte("deep"))

	var (
		mtx     sync.Mutex
		reports []WalkResult
	)

	f.walker.ProgressInterval = time.Millisecond
	f.walker.Progress = func(r WalkResult) {
		mtx.Lock()
		reports = append(reports, r)
		mtx.Unlock()
	}

	result := f.run()

	mtx.Lock()
	defer mtx.Unlock()

	require.NotEmpty(t, reports, "a walk that ends still reports once")

	last := reports[len(reports)-1]
	require.Equal(t, result.Files, last.Files, "the last report is what the run ended with")
	require.Equal(t, result.Bytes, last.Bytes)
	require.Equal(t, result.Uploaded, last.Uploaded)

	require.Nil(t, last.Ref, "a report carries no refs, which only a finished run has")
}

func TestAWalkCountsTheBytesItCoversAndTheOnesItSends(t *testing.T) {
	f := newWalkerFixture(t)

	big := f.random(150 << 10)
	f.write("a.txt", []byte("hello"))
	f.write("sub/b.bin", big)

	first := f.run()
	require.EqualValues(t, 2, first.Files)
	require.EqualValues(t, len("hello")+len(big), first.Bytes, "what the files hold on disk")
	require.EqualValues(t, first.Bytes+descriptorBytes(t, filepath.Join(f.root, "a.txt"), filepath.Join(f.root, "sub")), first.Uploaded, "a first run sends all of it, and the descriptors")

	// a run that changes nothing covers the same bytes and sends none
	second := f.run()
	require.EqualValues(t, first.Files, second.Files)
	require.EqualValues(t, first.Bytes, second.Bytes, "the walk still accounts for every file")
	require.Zero(t, second.Uploaded, "but uploads nothing, which is why bytes covered cannot drive a share")

	// one new file is all the third run sends
	f.write("c.txt", []byte("added"))

	third := f.run()
	require.EqualValues(t, 3, third.Files)
	require.EqualValues(t, first.Bytes+int64(len("added")), third.Bytes)
	require.EqualValues(t, len("added"), third.Uploaded)
}

func TestAWalkWithoutAProgressHookRunsAsBefore(t *testing.T) {
	f := newWalkerFixture(t)
	f.write("a.txt", []byte("hello"))

	f.walker.ProgressInterval = time.Millisecond

	result := f.run()
	require.EqualValues(t, 1, result.Files)
}

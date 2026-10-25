package backup

import (
	"os"
	"sync"
	"time"
)

// networkSkew is how far the clock of a network filesystem's server is
// taken to be from ours.
const networkSkew = 5 * time.Minute

// timestamps is how fine a mount's mtimes are and how far its clock may be
// from ours.
type timestamps struct {
	resolution time.Duration
	skew       time.Duration
}

// mountTimes finds, once per mount a walk of the local disk reaches, the
// timestamps of its filesystem.
type mountTimes struct {
	mu    sync.Mutex
	known map[uint64]timestamps
}

func newMountTimes() *mountTimes {
	return &mountTimes{known: map[uint64]timestamps{}}
}

// of returns the timestamps of the mount holding the file at path, whose
// stat is info.
func (m *mountTimes) of(path string, info os.FileInfo) timestamps {
	device := deviceOf(info)

	m.mu.Lock()
	times, ok := m.known[device]
	m.mu.Unlock()

	if ok {
		return times
	}

	times, ok = timestampsAt(path)
	if !ok {
		return timestamps{}
	}

	m.mu.Lock()
	m.known[device] = times
	m.mu.Unlock()

	return times
}

// racyWindow is how long before the base run's scan a file's mtime must
// lie for an unchanged mtime to show that the file was not rewritten.
func (w *Walker) racyWindow(path string, info os.FileInfo) time.Duration {
	window := w.traits.Resolution + w.traits.Skew

	if w.mounts != nil {
		times := w.mounts.of(path, info)
		window += times.resolution + times.skew
	}

	return window
}

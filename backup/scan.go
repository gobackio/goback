package backup

import (
	"context"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/gobackio/goback/proto"
)

// DefaultScanWorkers is how many directories a walk lists ahead of itself
// when the caller names no number. A metadata-bound walk of 216,000 files
// takes 31s listing on the walk, 19s with one lister and 9s with four;
// eight is no better and sixteen is worse.
const DefaultScanWorkers = 4

// DefaultScanWindow is how many entries a walk holds for directories it has
// not reached yet when the caller names no window. An entry costs about 230
// bytes, so this is at most a quarter of a gigabyte whatever the root holds.
const DefaultScanWindow = 1_000_000

// scanEntry is one directory entry with the stat data taken for it. An
// entry the walk excludes carries neither, because nothing stats it.
type scanEntry struct {
	name string
	info os.FileInfo
	err  error
	// excluded is Include's answer, which the walk takes rather than asking
	// again; excludedSize is the listed size of a regular file it excludes.
	excluded     bool
	excludedSize int64
}

// scanned is one directory's entries, or the error listing it produced.
type scanned struct {
	entries []scanEntry
	err     error
}

// scanJob names a directory by the path to list and the path to filter on.
type scanJob struct {
	dir string
	rel string
}

// scanner lists directories ahead of the walk, so discovery is not a queue
// of one, and holds what it found until the walk takes it. It reads no
// further ahead than window entries, and the walk lists a directory itself
// whenever the read-ahead has not got to it. Counting, it lists the whole
// tree however far ahead, holding what fits in the window and counting all
// of it.
type scanner struct {
	ctx     context.Context
	cancel  context.CancelFunc
	workers sync.WaitGroup
	walker  *Walker
	fs      FS
	window  int

	mtx     sync.Mutex
	cond    sync.Cond
	pending []scanJob
	ready   map[string]*scanned
	flight  map[string]bool
	held    int
	// wanted counts the walks waiting for each directory, which a worker
	// publishes even when the window is full
	wanted map[string]int
	closed bool
	// counted holds the directories counted so far, while counting
	counting bool
	counted  map[string]bool
}

func newScanner(ctx context.Context, w *Walker, workers, window int) *scanner {
	if window <= 0 {
		window = DefaultScanWindow
	}

	ctx, cancel := context.WithCancel(ctx)

	s := &scanner{
		ctx:     ctx,
		cancel:  cancel,
		walker:  w,
		fs:      w.fs(),
		window:  window,
		ready:   map[string]*scanned{},
		flight:  map[string]bool{},
		wanted:  map[string]int{},
		counted: map[string]bool{},
	}
	s.cond.L = &s.mtx

	for range workers {
		s.workers.Go(func() { s.run(ctx) })
	}

	return s
}

// close drops what the read-ahead holds and stops its workers, returning
// once none of them is listing any more.
func (s *scanner) close() {
	s.mtx.Lock()
	s.closed = true
	s.pending = nil
	s.ready = nil
	s.held = 0
	s.cond.Broadcast()
	s.mtx.Unlock()

	s.cancel()
	s.workers.Wait()
}

func (s *scanner) run(ctx context.Context) {
	for {
		job, ok := s.next(ctx)
		if !ok {
			return
		}

		found := s.list(job)

		s.mtx.Lock()
		s.publish(job, found)
		s.mtx.Unlock()
	}
}

// next claims the directory nearest to where the walk is, blocking until
// one is offered, and reports false once the scanner is done.
func (s *scanner) next(ctx context.Context) (scanJob, bool) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for {
		if s.closed || ctx.Err() != nil {
			return scanJob{}, false
		}

		for len(s.pending) > 0 {
			job := s.pending[len(s.pending)-1]
			s.pending = s.pending[:len(s.pending)-1]

			if s.ready[job.dir] != nil || s.flight[job.dir] || s.counted[job.dir] {
				continue
			}

			s.flight[job.dir] = true

			return job, true
		}

		// the jobs it passed over may have been the last
		s.settle()
		s.cond.Wait()
	}
}

// publish hands a worker's listing to the walk once there is room for it;
// counting, it counts the listing and drops it when there is none. The
// caller holds the lock.
func (s *scanner) publish(job scanJob, found *scanned) {
	for !s.closed && !s.counting && !s.fits(job, found) {
		s.cond.Wait()
	}

	delete(s.flight, job.dir)

	if !s.closed {
		s.count(job, found)

		if s.fits(job, found) {
			s.ready[job.dir] = found
			s.held += len(found.entries)
		}

		s.offer(job, found)
		s.settle()
	}

	s.cond.Broadcast()
}

// fits reports whether the window has room for a listing, or the walk is
// waiting for it. The caller holds the lock.
func (s *scanner) fits(job scanJob, found *scanned) bool {
	return s.held == 0 || s.held+len(found.entries) <= s.window || s.wanted[job.dir] > 0
}

// countAll has the scanner list the whole tree and count it.
func (s *scanner) countAll() {
	s.mtx.Lock()
	s.counting = true
	s.mtx.Unlock()
}

// count adds a listing's files to the walk's Found once, while counting,
// and reports whether it did. The caller holds the lock.
func (s *scanner) count(job scanJob, found *scanned) bool {
	if !s.counting || s.counted[job.dir] {
		return false
	}

	s.counted[job.dir] = true

	var files, bytes int64

	for _, entry := range found.entries {
		if entry.excluded || entry.info == nil || !entry.info.Mode().IsRegular() {
			continue
		}

		stat := &proto.FileInfo{Type: proto.NodeType_NODE_FILE}
		recordPlatform(stat, entry.info)

		if cloudOnly(stat) && !s.walker.DownloadPlaceholders {
			continue
		}

		files++
		bytes += entry.info.Size()
	}

	atomic.AddInt64(&s.walker.result.Found, files)
	atomic.AddInt64(&s.walker.result.FoundBytes, bytes)

	return true
}

// settle marks the count whole once nothing is left to list. The caller
// holds the lock.
func (s *scanner) settle() {
	if s.counting && len(s.pending) == 0 && len(s.flight) == 0 {
		s.walker.foundAll.Store(true)
	}
}

// offer queues the directories a listing found, nearest first so a worker
// claims them in the order the walk will ask for them. Beyond the window
// the furthest are dropped, and the walk lists those itself. The caller
// holds the lock.
func (s *scanner) offer(job scanJob, found *scanned) {
	for i := len(found.entries) - 1; i >= 0; i-- {
		entry := found.entries[i]
		if entry.err != nil || entry.info == nil || !entry.info.IsDir() {
			continue
		}

		rel := path.Join(job.rel, entry.name)
		if s.walker.unchanged(rel) {
			continue
		}

		s.pending = append(s.pending, scanJob{
			dir: s.fs.Join(job.dir, entry.name),
			rel: rel,
		})
	}

	if over := len(s.pending) - s.window; over > 0 && !s.counting {
		s.pending = append(s.pending[:0], s.pending[over:]...)
	}
}

// take gives the walk the listing of one directory, waiting for a worker
// already on it and listing the directory itself otherwise.
func (s *scanner) take(job scanJob) *scanned {
	s.mtx.Lock()

	waiting := false
	stopWaiting := func() {
		if waiting {
			if s.wanted[job.dir]--; s.wanted[job.dir] <= 0 {
				delete(s.wanted, job.dir)
			}
		}
	}

	for {
		if found, ok := s.ready[job.dir]; ok {
			delete(s.ready, job.dir)
			s.held -= len(found.entries)
			stopWaiting()

			s.cond.Broadcast()
			s.mtx.Unlock()

			return found
		}

		if !s.flight[job.dir] {
			break
		}

		if !waiting {
			waiting = true
			s.wanted[job.dir]++
		}

		s.cond.Broadcast()
		s.cond.Wait()
	}

	s.flight[job.dir] = true
	stopWaiting()
	s.mtx.Unlock()

	found := s.list(job)

	s.mtx.Lock()
	delete(s.flight, job.dir)
	// a directory counted already offered what it holds
	if !s.closed && (s.count(job, found) || !s.counting) {
		s.offer(job, found)
	}

	if !s.closed {
		s.settle()
	}
	s.cond.Broadcast()
	s.mtx.Unlock()

	return found
}

// list reads one directory and stats every entry the walk will look at. It
// never stops short, because a truncated listing reads as a directory that
// lost the rest of its entries.
func (s *scanner) list(job scanJob) *scanned {
	entries, err := s.fs.ReadDir(s.ctx, job.dir)
	if err != nil {
		return &scanned{err: err}
	}

	found := &scanned{entries: make([]scanEntry, len(entries))}

	for i, entry := range entries {
		name := entry.Name()
		found.entries[i].name = name

		if !s.walker.included(path.Join(job.rel, name)) {
			found.entries[i].excluded = true

			if !entry.IsDir() {
				if info, err := entry.Info(); err == nil && info.Mode().IsRegular() {
					found.entries[i].excludedSize = info.Size()
				}
			}
			continue
		}

		found.entries[i].info, found.entries[i].err = entry.Info()
	}

	return found
}

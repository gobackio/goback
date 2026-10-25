package pack

import (
	"bufio"
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type refKey [32]byte

// liveRef is a reached object with the uncompressed size of the file
// content it carries and how soon the roots it was reached from die.
type liveRef struct {
	key   refKey
	size  uint64
	death deathClass
}

// liveRefSize is what one liveRef takes up in a run.
const liveRefSize = len(refKey{}) + 8 + 1

func keyOf(hash []byte) refKey {
	var k refKey
	copy(k[:], hash)

	return k
}

// liveRuns collects refs and spills them as sorted runs on local disk.
// Runs are named after the batch of roots they belong to, so a resumed
// mark can pick up the runs of completed batches. Each run carries what
// its roots belonged to, which is what attributes an object.
type liveRuns struct {
	dir    string
	limit  int
	prefix string
	owner  Attribution

	mtx    sync.Mutex
	buf    []liveRef
	files  []string
	owners []Attribution
	added  uint64
	// spilled are the runs of the batch begun last, which its checkpoint lists
	spilled []runFile
}

// runFile is what a checkpoint records of one run, so a resumed mark takes
// on only a run that reads back as it was written.
type runFile struct {
	name string
	size int64
	sum  string
}

// adopt takes on a run an interrupted mark left behind.
func (l *liveRuns) adopt(path string, owner Attribution) {
	l.files = append(l.files, path)
	l.owners = append(l.owners, owner)
}

func newLiveRuns(dir string, limit int) *liveRuns {
	return &liveRuns{dir: dir, limit: limit}
}

func (l *liveRuns) add(refs []liveRef) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	l.buf = append(l.buf, refs...)
	l.added += uint64(len(refs))

	if len(l.buf) >= l.limit {
		return l.spillLocked()
	}

	return nil
}

func sortRefs(refs []liveRef) {
	sort.Slice(refs, func(i, j int) bool { return bytes.Compare(refs[i].key[:], refs[j].key[:]) < 0 })
}

// sortRoots groups the roots by group and then by set, so a batch of
// them holds one set and every group's batches are contiguous. A group's
// whole roots come before its shallow ones, so a file a shallow root
// visited first never keeps a whole root from its parts, and a set's
// roots that die later come before those that die sooner, so the first
// root to reach an object is one of those that keep it longest.
func sortRoots(roots []gcRoot) {
	sort.Slice(roots, func(i, j int) bool {
		if roots[i].owner.Group != roots[j].owner.Group {
			return roots[i].owner.before(roots[j].owner)
		}

		if roots[i].shallow != roots[j].shallow {
			return !roots[i].shallow
		}

		if roots[i].owner != roots[j].owner {
			return roots[i].owner.before(roots[j].owner)
		}

		if roots[i].death != roots[j].death {
			return roots[i].death.later(roots[j].death)
		}

		return bytes.Compare(roots[i].key[:], roots[j].key[:]) < 0
	})
}

func keysOf(roots []gcRoot) []refKey {
	keys := make([]refKey, len(roots))
	for i, root := range roots {
		keys[i] = root.key
	}

	return keys
}

// begin names the batch the next runs belong to and what owns it.
func (l *liveRuns) begin(prefix string, owner Attribution) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	l.prefix, l.owner, l.spilled = prefix, owner, nil
}

func (l *liveRuns) spillLocked() error {
	sortRefs(l.buf)

	file, err := os.CreateTemp(l.dir, l.prefix+"*.run")
	if err != nil {
		return err
	}

	sum := sha256.New()
	w := bufio.NewWriterSize(io.MultiWriter(file, sum), 1<<20)
	var entry [liveRefSize]byte
	for _, ref := range l.buf {
		copy(entry[:], ref.key[:])
		binary.BigEndian.PutUint64(entry[len(ref.key):], ref.size)
		entry[len(ref.key)+8] = byte(ref.death)

		if _, err := w.Write(entry[:]); err != nil {
			_ = file.Close()
			return err
		}
	}

	if err := w.Flush(); err != nil {
		_ = file.Close()
		return err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	l.files = append(l.files, file.Name())
	l.owners = append(l.owners, l.owner)
	l.spilled = append(l.spilled, runFile{name: filepath.Base(file.Name()), size: int64(len(l.buf) * liveRefSize), sum: fmt.Sprintf("%x", sum.Sum(nil))})
	l.buf = l.buf[:0]

	return nil
}

// checkpoint spills what is buffered and marks the batch complete, listing
// the runs it spilled.
func (l *liveRuns) checkpoint(batch int) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if len(l.buf) > 0 {
		if err := l.spillLocked(); err != nil {
			return err
		}
	}

	var marker bytes.Buffer
	fmt.Fprintf(&marker, "%d\n", len(l.spilled))
	for _, run := range l.spilled {
		fmt.Fprintf(&marker, "%s %d %s\n", run.name, run.size, run.sum)
	}

	file, err := os.Create(filepath.Join(l.dir, doneName(batch)))
	if err != nil {
		return err
	}

	if _, err := file.Write(marker.Bytes()); err != nil {
		_ = file.Close()
		return err
	}

	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}

// checkpointed returns the runs the done marker of a batch lists, false
// when one is missing or does not read back as it was written.
func checkpointed(dir string, batch int) ([]string, bool) {
	data, err := os.ReadFile(filepath.Join(dir, doneName(batch)))
	if err != nil {
		return nil, false
	}

	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	count, err := strconv.Atoi(lines[0])
	if err != nil || count != len(lines)-1 {
		return nil, false
	}

	paths := make([]string, 0, count)
	for _, line := range lines[1:] {
		var run runFile
		if _, err := fmt.Sscanf(line, "%s %d %s", &run.name, &run.size, &run.sum); err != nil {
			return nil, false
		}

		path := filepath.Join(dir, filepath.Base(run.name))
		if !strings.HasPrefix(run.name, batchPrefix(batch)) || !runReadsAs(path, run) {
			return nil, false
		}

		paths = append(paths, path)
	}

	return paths, true
}

func runReadsAs(path string, run runFile) bool {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return false
	}
	defer file.Close()

	sum := sha256.New()
	n, err := io.Copy(sum, file)

	return err == nil && n == run.size && fmt.Sprintf("%x", sum.Sum(nil)) == run.sum
}

func batchPrefix(batch int) string { return fmt.Sprintf("batch-%d-", batch) }
func doneName(batch int) string    { return fmt.Sprintf("batch-%d.done", batch) }

func (l *liveRuns) close() {
	for _, name := range l.files {
		_ = os.Remove(name)
	}

	l.files = nil
	l.owners = nil
	l.buf = nil
}

// iterator sorts what is buffered and merges it with the spilled runs into
// one deduplicated ascending stream.
func (l *liveRuns) iterator() (*liveIter, error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	sortRefs(l.buf)

	it := &liveIter{}
	it.heads = append(it.heads, &runHead{refs: l.buf, owner: l.owner})

	for i, name := range l.files {
		file, err := os.Open(filepath.Clean(name))
		if err != nil {
			it.close()
			return nil, err
		}

		it.heads = append(it.heads, &runHead{file: file, reader: bufio.NewReaderSize(file, 1<<20), owner: l.owners[i]})
	}

	for _, h := range it.heads {
		if h.advance() {
			it.heap = append(it.heap, h)
		}
	}

	heap.Init(&it.heap)

	return it, nil
}

type runHead struct {
	refs   []liveRef
	file   *os.File
	reader *bufio.Reader
	cur    liveRef
	owner  Attribution
	// err is why the run ended other than at an entry's end
	err error
}

func (h *runHead) advance() bool {
	if h.reader == nil {
		if len(h.refs) == 0 {
			return false
		}

		h.cur, h.refs = h.refs[0], h.refs[1:]

		return true
	}

	var entry [liveRefSize]byte
	if _, err := io.ReadFull(h.reader, entry[:]); err != nil {
		if !errors.Is(err, io.EOF) {
			h.err = fmt.Errorf("reading live run %s: %w", h.file.Name(), err)
		}

		return false
	}

	copy(h.cur.key[:], entry[:])
	h.cur.size = binary.BigEndian.Uint64(entry[len(h.cur.key):])
	h.cur.death = deathClass(entry[len(h.cur.key)+8])

	return true
}

type runHeap []*runHead

func (h runHeap) Len() int { return len(h) }

// Less orders by ref, then by group and set, so an object several sets
// reach goes to the one whose roots the mark walked first.
func (h runHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].cur.key[:], h[j].cur.key[:]); c != 0 {
		return c < 0
	}

	return h[i].owner.before(h[j].owner)
}
func (h runHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x interface{}) { *h = append(*h, x.(*runHead)) }
func (h *runHeap) Pop() interface{} {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]

	return x
}

type liveIter struct {
	heads []*runHead
	heap  runHeap

	key   refKey
	found reach
	held  bool
	drawn bool
}

// reach is what the runs hold at one key: everything that reached it, in
// group then set order and without repeats, the size of the file content
// it carries, and how soon the last of the roots it was reached from dies.
type reach struct {
	owners []Attribution
	size   uint64
	death  deathClass
}

// at returns what the runs hold at key, or false when they hold nothing
// there. The answer stands until the caller asks after a later key, so
// the same object in two archives is answered twice.
func (it *liveIter) at(key refKey) (reach, bool) {
	for {
		if !it.drawn {
			it.draw()
		}

		if !it.held || bytes.Compare(it.key[:], key[:]) > 0 {
			return reach{}, false
		}

		if it.key == key {
			return it.found, true
		}

		it.drawn = false
	}
}

// draw collects the runs' entries for the next key they hold.
func (it *liveIter) draw() {
	it.drawn, it.held = true, false
	it.found = reach{owners: it.found.owners[:0]}

	if it.heap.Len() == 0 {
		return
	}

	it.key, it.held = it.heap[0].cur.key, true
	it.found.death = it.heap[0].cur.death

	for it.heap.Len() > 0 && it.heap[0].cur.key == it.key {
		top := it.heap[0]
		it.found.size = max(it.found.size, top.cur.size)
		if top.cur.death.later(it.found.death) {
			it.found.death = top.cur.death
		}

		if n := len(it.found.owners); n == 0 || it.found.owners[n-1] != top.owner {
			it.found.owners = append(it.found.owners, top.owner)
		}

		if top.advance() {
			heap.Fix(&it.heap, 0)
		} else {
			heap.Pop(&it.heap)
		}
	}
}

// err returns why a run ended early, nil when every run read to its end.
func (it *liveIter) err() error {
	for _, h := range it.heads {
		if h.err != nil {
			return h.err
		}
	}

	return nil
}

func (it *liveIter) close() {
	for _, h := range it.heads {
		if h.file != nil {
			_ = h.file.Close()
		}
	}
}

package badger

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/dgraph-io/badger/v4"
)

// Option configures a Store.
type Option func(*Store)

// WithCapacity bounds the bytes of objects a Store keeps: past capacity, the
// objects least recently read or written are deleted. Only a cache should
// use it, since an evicted object is gone. 0 leaves the store unbounded.
// The order of last use survives a reopen, to within ten minutes after a
// crash.
func WithCapacity(capacity int64) Option {
	return func(s *Store) {
		if capacity > 0 {
			s.lru = &lru{
				capacity: capacity,
				now:      time.Now,
				head:     none,
				tail:     none,
				free:     none,
				slots:    make([]int32, 16),
				seed:     maphash.MakeSeed(),
			}
		}
	}
}

// stampEvery is how stale the persisted last use of an object may get
// before a read writes it again.
const stampEvery = 10 * time.Minute

const usedKeyPrefix = "used|"

// sizedKey marks that every object has a used key holding its size, so a
// load reads the used keys alone instead of every object's key.
const sizedKey = "lru|sized"

func usedKey(hash []byte) []byte {
	return append([]byte(usedKeyPrefix), hash...)
}

// never is the last use of an object with none recorded, earlier than any
// recorded one; it is persisted as 0.
const never = math.MinInt64

func encodeUse(used, size int64) []byte {
	if used == never {
		used = 0
	}

	val := binary.BigEndian.AppendUint64(nil, uint64(used))

	return binary.BigEndian.AppendUint64(val, uint64(size))
}

// decodeUse reads a used value as Unix nanoseconds; size is -1 for one an
// older store wrote with the time alone.
func decodeUse(val []byte) (used, size int64) {
	if len(val) != 8 && len(val) != 16 {
		return never, -1
	}

	used = int64(binary.BigEndian.Uint64(val))
	if len(val) == 8 {
		return used, -1
	}

	return used, int64(binary.BigEndian.Uint64(val[8:]))
}

// none ends a list of entries.
const none = -1

var errTooMany = errors.New("the cache holds too many objects to track")

// lru keeps every object's size and last use, most recent first. All
// writes to the database go through it under mtx, so entries and the
// database agree on what is held.
//
// Entries 0 to allocated live in blocks, which never move. Held entries
// form a doubly linked list from head to tail through prev and next, and
// released ones a chain from free through next. slots indexes the held
// entries by hash with linear probing; each holds an entry's index plus
// one, 0 when empty.
type lru struct {
	mtx      sync.Mutex
	capacity int64
	size     int64
	count    int
	now      func() time.Time

	blocks           []*entryBlock
	allocated        int32
	head, tail, free int32
	slots            []int32
	seed             maphash.Seed
}

const blockBits = 10

type entryBlock [1 << blockBits]lruEntry

type lruEntry struct {
	hash [proto.HashSize]byte
	size int64
	// used and stamped are Unix nanoseconds, or never
	used       int64
	stamped    int64
	prev, next int32
}

func (l *lru) at(i int32) *lruEntry {
	return &l.blocks[i>>blockBits][i&(1<<blockBits-1)]
}

func (l *lru) home(hash []byte) int {
	return int(maphash.Bytes(l.seed, hash) & uint64(len(l.slots)-1))
}

// slot returns the slot holding hash, or the empty one it would go in.
func (l *lru) slot(hash []byte) (s int, ok bool) {
	mask := len(l.slots) - 1
	for s = l.home(hash); ; s = (s + 1) & mask {
		i := l.slots[s]
		if i == 0 || bytes.Equal(l.at(i - 1).hash[:], hash) {
			return s, i != 0
		}
	}
}

func (l *lru) find(hash []byte) (int32, bool) {
	s, ok := l.slot(hash)
	return l.slots[s] - 1, ok
}

// unslot empties slot s, moving later entries of its probe run back so
// none is cut off from its home.
func (l *lru) unslot(s int) {
	mask := len(l.slots) - 1
	for t := (s + 1) & mask; l.slots[t] != 0; t = (t + 1) & mask {
		home := l.home(l.at(l.slots[t] - 1).hash[:])
		if (t-home)&mask >= (t-s)&mask {
			l.slots[s] = l.slots[t]
			s = t
		}
	}
	l.slots[s] = 0
}

func (l *lru) full() bool {
	return l.free == none && l.allocated == math.MaxInt32-1
}

// add holds a new entry for hash, which must be proto.HashSize bytes and
// not held, without linking it; the caller checks full first.
func (l *lru) add(hash []byte, size, used int64) int32 {
	if (l.count+1)*4 > len(l.slots)*3 {
		old := l.slots
		l.slots = make([]int32, 2*len(old))
		for _, i := range old {
			if i != 0 {
				s, _ := l.slot(l.at(i - 1).hash[:])
				l.slots[s] = i
			}
		}
	}

	i := l.free
	if i == none {
		i = l.allocated
		l.allocated++
		if int(i>>blockBits) == len(l.blocks) {
			l.blocks = append(l.blocks, new(entryBlock))
		}
	} else {
		l.free = l.at(i).next
	}

	e := l.at(i)
	*e = lruEntry{size: size, used: used, stamped: used, prev: none, next: none}
	copy(e.hash[:], hash)

	s, _ := l.slot(hash)
	l.slots[s] = i + 1
	l.count++
	l.size += size

	return i
}

func (l *lru) pushFront(i int32) {
	e := l.at(i)
	e.prev, e.next = none, l.head
	if l.head != none {
		l.at(l.head).prev = i
	} else {
		l.tail = i
	}
	l.head = i
}

func (l *lru) pushBack(i int32) {
	e := l.at(i)
	e.prev, e.next = l.tail, none
	if l.tail != none {
		l.at(l.tail).next = i
	} else {
		l.head = i
	}
	l.tail = i
}

func (l *lru) unlink(i int32) {
	e := l.at(i)
	if e.prev != none {
		l.at(e.prev).next = e.next
	} else {
		l.head = e.next
	}
	if e.next != none {
		l.at(e.next).prev = e.prev
	} else {
		l.tail = e.prev
	}
}

func (l *lru) load(db *badger.DB) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	var sized bool

	err := db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(sizedKey))
		sized = err == nil
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}

		return err
	})
	if err != nil {
		return err
	}

	var unsized []int32
	var orphans [][]byte

	if sized {
		unsized, orphans, err = l.loadUses(db)
	} else {
		unsized, orphans, err = l.scanObjects(db)
	}
	if err != nil {
		return err
	}

	order := make([]int32, l.allocated)
	for i := range order {
		order[i] = int32(i)
	}
	slices.SortStableFunc(order, func(a, b int32) int {
		return cmp.Compare(l.at(b).used, l.at(a).used)
	})
	for _, i := range order {
		l.pushBack(i)
	}

	batch := db.NewWriteBatch()
	defer batch.Cancel()

	for _, key := range orphans {
		if err := batch.Delete(key); err != nil {
			return err
		}
	}

	if err := l.evict(batch); err != nil {
		return err
	}

	for _, i := range unsized {
		e := l.at(i)
		if _, ok := l.find(e.hash[:]); !ok {
			continue
		}

		if err := batch.Set(usedKey(e.hash[:]), encodeUse(e.used, e.size)); err != nil {
			return err
		}
	}

	if err := batch.Flush(); err != nil {
		return err
	}

	if sized {
		return nil
	}

	return db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(sizedKey), nil)
	})
}

// loadUses adds the entries from the used keys alone, looking up the size
// of those an older store wrote without it.
func (l *lru) loadUses(db *badger.DB) (unsized []int32, orphans [][]byte, err error) {
	var used, size int64
	decode := func(val []byte) error {
		used, size = decodeUse(val)
		return nil
	}

	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(usedKeyPrefix)

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			hash := item.Key()[len(usedKeyPrefix):]

			if len(hash) != proto.HashSize {
				orphans = append(orphans, item.KeyCopy(nil), objectKey(hash))
				continue
			}

			if err := item.Value(decode); err != nil {
				return err
			}

			if size >= 0 {
				if l.full() {
					return errTooMany
				}
				l.add(hash, size, used)

				continue
			}

			obj, err := txn.Get(objectKey(hash))
			if errors.Is(err, badger.ErrKeyNotFound) {
				orphans = append(orphans, item.KeyCopy(nil))
				continue
			}
			if err != nil {
				return err
			}

			if l.full() {
				return errTooMany
			}
			unsized = append(unsized, l.add(hash, int64(len(obj.Key()))+obj.ValueSize(), used))
		}

		return nil
	})

	return unsized, orphans, err
}

// scanObjects adds the entries from every object's key and the used keys
// beside them; all of them are unsized.
func (l *lru) scanObjects(db *badger.DB) (unsized []int32, orphans [][]byte, err error) {
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte(objectKeyPrefix)

		it := txn.NewIterator(opts)
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			hash := item.Key()[len(objectKeyPrefix):]

			if len(hash) != proto.HashSize {
				orphans = append(orphans, item.KeyCopy(nil))
				continue
			}

			if l.full() {
				it.Close()
				return errTooMany
			}
			unsized = append(unsized, l.add(hash, int64(len(item.Key()))+item.ValueSize(), never))
		}
		it.Close()

		opts.PrefetchValues = true
		opts.Prefix = []byte(usedKeyPrefix)

		it = txn.NewIterator(opts)
		defer it.Close()

		var used int64
		decode := func(val []byte) error {
			used, _ = decodeUse(val)
			return nil
		}

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()

			i, ok := l.find(item.Key()[len(usedKeyPrefix):])
			if !ok {
				orphans = append(orphans, item.KeyCopy(nil))
				continue
			}

			if err := item.Value(decode); err != nil {
				return err
			}

			l.at(i).used = used
			l.at(i).stamped = used
		}

		return nil
	})

	return unsized, orphans, err
}

func (l *lru) put(db *badger.DB, entry *badger.Entry) error {
	hash := entry.Key[len(objectKeyPrefix):]

	l.mtx.Lock()
	defer l.mtx.Unlock()

	if i, ok := l.find(hash); ok {
		l.touch(db, i)
		return nil
	}

	if len(hash) != proto.HashSize {
		return fmt.Errorf("a ref of %d bytes, not %d", len(hash), proto.HashSize)
	}

	if l.full() {
		return errTooMany
	}

	now := l.now().UnixNano()
	size := int64(len(entry.Key) + len(entry.Value))

	err := db.Update(func(txn *badger.Txn) error {
		if err := txn.SetEntry(entry); err != nil {
			return err
		}

		return txn.Set(usedKey(hash), encodeUse(now, size))
	})
	if err != nil {
		return err
	}

	l.pushFront(l.add(hash, size, now))

	if l.size <= l.capacity {
		return nil
	}

	batch := db.NewWriteBatch()
	defer batch.Cancel()

	if err := l.evict(batch); err != nil {
		return err
	}

	return batch.Flush()
}

func (l *lru) use(db *badger.DB, hash []byte) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if i, ok := l.find(hash); ok {
		l.touch(db, i)
	}
}

// touch makes entry i the most recently used and persists that only once
// its stamp is stampEvery old; a failed stamp only costs order after a
// reopen.
func (l *lru) touch(db *badger.DB, i int32) {
	e := l.at(i)
	e.used = l.now().UnixNano()
	if l.head != i {
		l.unlink(i)
		l.pushFront(i)
	}

	if e.stamped != never && e.used-e.stamped < int64(stampEvery) {
		return
	}

	err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(usedKey(e.hash[:]), encodeUse(e.used, e.size))
	})
	if err == nil {
		e.stamped = e.used
	}
}

func (l *lru) delete(db *badger.DB, hash []byte) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(objectKey(hash)); err != nil {
			return err
		}

		return txn.Delete(usedKey(hash))
	})
	if err != nil {
		return err
	}

	if s, ok := l.slot(hash); ok {
		l.forget(s)
	}

	return nil
}

// forget drops the entry in slot s, which must be linked.
func (l *lru) forget(s int) {
	i := l.slots[s] - 1
	l.unslot(s)
	l.unlink(i)

	e := l.at(i)
	l.size -= e.size
	l.count--
	e.next = l.free
	l.free = i
}

func (l *lru) evict(batch *badger.WriteBatch) error {
	for l.size > l.capacity && l.tail != none {
		hash := l.at(l.tail).hash[:]

		if err := batch.Delete(objectKey(hash)); err != nil {
			return err
		}

		if err := batch.Delete(usedKey(hash)); err != nil {
			return err
		}

		s, _ := l.slot(hash)
		l.forget(s)
	}

	return nil
}

// flush persists the last uses not yet stamped.
func (l *lru) flush(db *badger.DB) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	batch := db.NewWriteBatch()
	defer batch.Cancel()

	for i := l.head; i != none; i = l.at(i).next {
		e := l.at(i)
		if e.used > e.stamped {
			if batch.Set(usedKey(e.hash[:]), encodeUse(e.used, e.size)) != nil {
				return
			}
			e.stamped = e.used
		}
	}

	_ = batch.Flush()
}

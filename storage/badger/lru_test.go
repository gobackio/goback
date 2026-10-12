package badger

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func objects(n int) []*proto.Object {
	var objs []*proto.Object
	for i := range n {
		objs = append(objs, proto.NewObject(&proto.Blob{Data: []byte(fmt.Sprintf("object %03d", i))}))
	}

	return objs
}

// openBounded opens a store at dir holding at most n of the objects
// objects returns.
func openBounded(t *testing.T, dir string, n int, c *clock) *Store {
	t.Helper()

	probe, err := New(t.TempDir(), WithCapacity(1<<30))
	require.NoError(t, err)
	require.NoError(t, probe.Put(context.Background(), objects(1)[0]))
	size := probe.lru.size
	require.NoError(t, probe.Close())

	s, err := New(dir, WithCapacity(int64(n)*size))
	require.NoError(t, err)
	s.lru.now = c.now

	return s
}

func held(t *testing.T, s *Store, objs []*proto.Object) []bool {
	t.Helper()

	var out []bool
	for _, obj := range objs {
		has, err := s.Has(context.Background(), obj.Ref())
		require.NoError(t, err)
		out = append(out, has)
	}

	return out
}

func TestABoundedStoreEvictsTheLeastRecentlyUsed(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	s := openBounded(t, t.TempDir(), 3, c)
	defer s.Close()

	objs := objects(5)
	for _, obj := range objs[:3] {
		c.advance(time.Second)
		require.NoError(t, s.Put(ctx, obj))
	}

	c.advance(time.Second)
	_, err := s.Get(ctx, objs[0].Ref())
	require.NoError(t, err)

	c.advance(time.Second)
	require.NoError(t, s.Put(ctx, objs[1]))

	c.advance(time.Second)
	require.NoError(t, s.Put(ctx, objs[3]))
	require.Equal(t, []bool{true, true, false, true, false}, held(t, s, objs))

	c.advance(time.Second)
	require.NoError(t, s.Put(ctx, objs[4]))
	require.Equal(t, []bool{false, true, false, true, true}, held(t, s, objs))

	_, err = s.Get(ctx, objs[0].Ref())
	require.ErrorIs(t, err, backup.ErrNotFound)
	require.LessOrEqual(t, s.lru.size, s.lru.capacity)
}

func TestABoundedStoreKeepsItsOrderAndCapacityAcrossAReopen(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := &clock{t: time.Unix(1_000_000, 0)}
	s := openBounded(t, dir, 4, c)

	objs := objects(4)
	for _, obj := range objs {
		c.advance(time.Second)
		require.NoError(t, s.Put(ctx, obj))
	}

	c.advance(time.Second)
	_, err := s.Get(ctx, objs[0].Ref())
	require.NoError(t, err)
	require.NoError(t, s.Close())

	s = openBounded(t, dir, 2, c)
	require.Equal(t, []bool{true, false, false, true}, held(t, s, objs))
	require.LessOrEqual(t, s.lru.size, s.lru.capacity)
	require.NoError(t, s.Close())

	s = openBounded(t, dir, 2, c)
	defer s.Close()
	require.Equal(t, []bool{true, false, false, true}, held(t, s, objs))
}

func TestReadingABoundedStoreWritesTheLastUseOnlyOnceItIsStale(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	s := openBounded(t, t.TempDir(), 4, c)
	defer s.Close()

	obj := objects(1)[0]
	require.NoError(t, s.Put(ctx, obj))

	before := s.db.MaxVersion()
	for range 100 {
		c.advance(time.Second)
		_, err := s.Get(ctx, obj.Ref())
		require.NoError(t, err)
		require.NoError(t, s.Put(ctx, obj))
	}
	require.Equal(t, before, s.db.MaxVersion(), "reads within the stamp interval write nothing")

	c.advance(stampEvery)
	_, err := s.Get(ctx, obj.Ref())
	require.NoError(t, err)
	require.Greater(t, s.db.MaxVersion(), before, "a stale stamp is written")
}

func TestABoundedStoreStaysWithinCapacityUnderConcurrentUse(t *testing.T) {
	ctx := context.Background()
	c := &clock{t: time.Unix(1_000_000, 0)}
	s := openBounded(t, t.TempDir(), 8, c)
	defer s.Close()

	objs := objects(64)

	var grp errgroup.Group
	for w := range 8 {
		grp.Go(func() error {
			for i := range 200 {
				obj := objs[(w*7+i)%len(objs)]
				switch i % 3 {
				case 0:
					if err := s.Put(ctx, obj); err != nil {
						return err
					}
				case 1:
					if _, err := s.Get(ctx, obj.Ref()); err != nil && !errors.Is(err, backup.ErrNotFound) {
						return err
					}
				default:
					if err := s.Delete(ctx, obj.Ref()); err != nil {
						return err
					}
				}
			}

			return nil
		})
	}
	require.NoError(t, grp.Wait())
	require.LessOrEqual(t, s.lru.size, s.lru.capacity)

	var stored int
	require.NoError(t, s.Walk(ctx, false, proto.ObjectType_BLOB, func(*proto.Object) error {
		stored++
		return nil
	}))
	require.Equal(t, s.lru.order.Len(), stored)
}

func TestASecondOpenOfAStoreFailsAsLocked(t *testing.T) {
	dir := t.TempDir()

	first, err := New(dir)
	require.NoError(t, err)
	defer first.Close()

	_, err = New(dir)
	require.ErrorIs(t, err, ErrLocked)
}

// rewriteUses rewrites every used key of the store at dir as a store
// before sizes were recorded wrote it, the time alone, and drops the
// sized mark unless keepMark.
func rewriteUses(t *testing.T, dir string, keepMark bool) {
	t.Helper()

	s, err := New(dir, WithCapacity(1<<30))
	require.NoError(t, err)

	require.NoError(t, s.db.Update(func(txn *badger.Txn) error {
		for el := s.lru.order.Front(); el != nil; el = el.Next() {
			e := el.Value.(*lruEntry)
			if err := txn.Set(usedKey([]byte(e.hash)), encodeUse(e.used, e.size)[:8]); err != nil {
				return err
			}
		}

		if keepMark {
			return nil
		}

		return txn.Delete([]byte(sizedKey))
	}))
	require.NoError(t, s.db.Close())
}

func TestABoundedStoreLoadsWhatAnOlderStoreWrote(t *testing.T) {
	for _, keepMark := range []bool{false, true} {
		t.Run(fmt.Sprintf("mark=%v", keepMark), func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			c := &clock{t: time.Unix(1_000_000, 0)}
			s := openBounded(t, dir, 4, c)

			objs := objects(4)
			for _, obj := range objs {
				c.advance(time.Second)
				require.NoError(t, s.Put(ctx, obj))
			}

			c.advance(time.Second)
			_, err := s.Get(ctx, objs[0].Ref())
			require.NoError(t, err)

			size := s.lru.size
			require.NoError(t, s.Close())

			rewriteUses(t, dir, keepMark)

			s = openBounded(t, dir, 4, c)
			require.Equal(t, size, s.lru.size)
			require.NoError(t, s.Close())

			s = openBounded(t, dir, 2, c)
			defer s.Close()
			require.Equal(t, []bool{true, false, false, true}, held(t, s, objs))
		})
	}
}

func TestABoundedStoreCountsWhatItHeldUnbounded(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	c := &clock{t: time.Unix(1_000_000, 0)}
	objs := objects(4)

	s := openBounded(t, dir, 4, c)
	require.NoError(t, s.Put(ctx, objs[0]))
	one := s.lru.size
	require.NoError(t, s.Close())

	unbounded, err := New(dir)
	require.NoError(t, err)
	for _, obj := range objs[1:] {
		require.NoError(t, unbounded.Put(ctx, obj))
	}
	require.NoError(t, unbounded.Close())

	s = openBounded(t, dir, 4, c)
	defer s.Close()
	require.Equal(t, 4*one, s.lru.size)
}

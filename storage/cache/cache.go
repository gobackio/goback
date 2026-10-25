package cache

import (
	"context"
	"sync"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/wrapped"
)

var _ backup.ObjectStore = (*Store)(nil)
var _ backup.TreeFetcher = (*Store)(nil)
var _ backup.Keeper = (*Store)(nil)
var _ wrapped.Wrapper = (*Store)(nil)

// Copies holds a Store's copies of objects by ref. *blobcache.Cache is one
// on disk, Memory one for a single process, and InStore adapts an object
// store.
type Copies interface {
	// Get returns the copy of ref, if there is one.
	Get(ref *proto.Ref) (*proto.Object, bool)
	// Put keeps obj as the copy of ref.
	Put(ref *proto.Ref, obj *proto.Object) error
	// Drop forgets the copy of ref.
	Drop(ref *proto.Ref)
}

// New layers copies over wrapped for its commits, trees and files.
func New(copies Copies, wrapped backup.ObjectStore) *Store {
	return &Store{
		cache:   copies,
		wrapped: wrapped,
	}
}

// Store serves metadata objects from its copies before wrapped, filling them
// on reads and writes; failures to keep a copy are ignored. Objects are
// immutable by ref, so a copy is never stale; what is newest is still the
// wrapped store's to say.
type Store struct {
	cache   Copies
	wrapped backup.ObjectStore
}

// Unwrap implements wrapped.Wrapper.
func (s *Store) Unwrap() backup.ObjectStore { return s.wrapped }

func (s *Store) keep(obj *proto.Object) {
	if obj.Type().Metadata() {
		_ = s.cache.Put(obj.Ref(), obj)
	}
}

// Put implements backup.ObjectStore.
func (s *Store) Put(ctx context.Context, object *proto.Object) error {
	err := s.wrapped.Put(ctx, object)
	if err == nil {
		s.keep(object)
	}

	return err
}

var _ backup.CommitBatcher = (*Store)(nil)

// PutCommits implements backup.CommitBatcher through the wrapped store.
func (s *Store) PutCommits(ctx context.Context, commits []*proto.Object) error {
	err := backup.PutCommits(ctx, s.wrapped, commits)
	if err == nil {
		for _, commit := range commits {
			s.keep(commit)
		}
	}

	return err
}

var _ backup.Batcher = (*Store)(nil)

// Missing implements backup.Batcher through the wrapped store.
func (s *Store) Missing(ctx context.Context, refs []*proto.Ref) ([]*proto.Ref, error) {
	return backup.Missing(ctx, s.wrapped, refs)
}

// PutObjects implements backup.Batcher through the wrapped store.
func (s *Store) PutObjects(ctx context.Context, objects []*proto.Object) error {
	err := backup.PutObjects(ctx, s.wrapped, objects)
	if err == nil {
		for _, obj := range objects {
			s.keep(obj)
		}
	}

	return err
}

// Keep implements backup.Keeper.
func (s *Store) Keep(ctx context.Context, obj *proto.Object) {
	s.keep(obj)

	if keeper, ok := s.wrapped.(backup.Keeper); ok {
		keeper.Keep(ctx, obj)
	}
}

// Kept implements backup.Keeper: a copy of ref is kept here, or by the
// wrapped store where it is a Keeper.
func (s *Store) Kept(ref *proto.Ref) bool {
	if _, ok := s.cache.Get(ref); ok {
		return true
	}

	keeper, ok := s.wrapped.(backup.Keeper)

	return ok && keeper.Kept(ref)
}

// GetTree serves the tree at ref and its splits from the copies when it
// holds them all, and otherwise forwards to the wrapped store's prefetch,
// if it has one, keeping what it fetched. Trees below are left to later
// calls.
func (s *Store) GetTree(ctx context.Context, ref *proto.Ref, maxDepth uint32) ([]*proto.Object, error) {
	if objects, ok := s.cachedTree(ref); ok {
		return objects, nil
	}

	fetcher, ok := s.wrapped.(backup.TreeFetcher)
	if !ok {
		return nil, backup.ErrNotImplemented
	}

	objects, err := fetcher.GetTree(ctx, ref, maxDepth)
	if err != nil {
		return nil, err
	}

	for _, obj := range objects {
		s.keep(obj)
	}

	return objects, nil
}

func (s *Store) cachedTree(ref *proto.Ref) ([]*proto.Object, bool) {
	root, ok := s.cache.Get(ref)
	if !ok {
		return nil, false
	}

	objects := []*proto.Object{root}

	for _, split := range root.GetTree().GetSplits() {
		obj, ok := s.cache.Get(split)
		if !ok {
			return nil, false
		}

		objects = append(objects, obj)
	}

	return objects, true
}

// Get implements backup.ObjectStore.
func (s *Store) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	if obj, ok := s.cache.Get(ref); ok {
		return obj, nil
	}

	obj, err := s.wrapped.Get(ctx, ref)
	if err == nil {
		s.keep(obj)
	}

	return obj, err
}

// Delete implements backup.ObjectStore.
func (s *Store) Delete(ctx context.Context, ref *proto.Ref) error {
	err := s.wrapped.Delete(ctx, ref)
	if err == nil {
		s.cache.Drop(ref)
	}

	return err
}

// Walk implements backup.ObjectStore.
func (s *Store) Walk(ctx context.Context, b bool, objectType proto.ObjectType, receiver backup.ObjectReceiver) error {
	return s.wrapped.Walk(ctx, b, objectType, receiver)
}

// Has implements backup.ObjectStore.
func (s *Store) Has(ctx context.Context, ref *proto.Ref) (bool, error) {
	if _, ok := s.cache.Get(ref); ok {
		return true, nil
	}

	return s.wrapped.Has(ctx, ref)
}

// Memory is Copies held in memory for as long as it is referenced.
type Memory struct {
	mtx     sync.Mutex
	objects map[string]*proto.Object
}

// NewMemory returns empty Copies in memory.
func NewMemory() *Memory {
	return &Memory{objects: map[string]*proto.Object{}}
}

// Get implements Copies.
func (m *Memory) Get(ref *proto.Ref) (*proto.Object, bool) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	obj, ok := m.objects[string(ref.GetHash())]

	return obj, ok
}

// Put implements Copies.
func (m *Memory) Put(ref *proto.Ref, obj *proto.Object) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.objects[string(ref.GetHash())] = obj

	return nil
}

// Drop implements Copies.
func (m *Memory) Drop(ref *proto.Ref) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	delete(m.objects, string(ref.GetHash()))
}

// Recent is Copies in memory that hold what was kept or read since the
// generation before the current one: Rotate starts a generation and drops
// every copy the one before it neither kept nor read. Rotated once per
// commit, it holds what the last commit wrote and what it read of the one
// before, and nothing older.
type Recent struct {
	mtx      sync.Mutex
	current  map[string]*proto.Object
	previous map[string]*proto.Object
}

// NewRecent returns empty Recent copies.
func NewRecent() *Recent {
	return &Recent{current: map[string]*proto.Object{}, previous: map[string]*proto.Object{}}
}

// Rotate starts a generation.
func (r *Recent) Rotate() {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.previous, r.current = r.current, map[string]*proto.Object{}
}

// Get implements Copies; a copy of the previous generation joins the
// current one.
func (r *Recent) Get(ref *proto.Ref) (*proto.Object, bool) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	key := string(ref.GetHash())
	if obj, ok := r.current[key]; ok {
		return obj, true
	}

	obj, ok := r.previous[key]
	if ok {
		r.current[key] = obj
	}

	return obj, ok
}

// Put implements Copies.
func (r *Recent) Put(ref *proto.Ref, obj *proto.Object) error {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.current[string(ref.GetHash())] = obj

	return nil
}

// Drop implements Copies.
func (r *Recent) Drop(ref *proto.Ref) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	delete(r.current, string(ref.GetHash()))
	delete(r.previous, string(ref.GetHash()))
}

// InStore keeps Copies in an object store such as a local badger store.
func InStore(store backup.ObjectStore) Copies {
	return storeCopies{store}
}

type storeCopies struct {
	store backup.ObjectStore
}

func (c storeCopies) Get(ref *proto.Ref) (*proto.Object, bool) {
	obj, err := c.store.Get(context.Background(), ref)

	return obj, err == nil && obj != nil
}

func (c storeCopies) Put(_ *proto.Ref, obj *proto.Object) error {
	return c.store.Put(context.Background(), obj)
}

func (c storeCopies) Drop(ref *proto.Ref) {
	_ = c.store.Delete(context.Background(), ref)
}

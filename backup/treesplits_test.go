package backup

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// keepingStore keeps copies of the trees and files put through it, read
// from it or handed to Keep, and serves them before the store, as a cache
// in front of a store server does; it records the trees it was sent.
type keepingStore struct {
	*presenceStore

	mtx    sync.Mutex
	copies map[string]*proto.Object
	sent   map[string]bool
}

func keepingSplits(f *walkerFixture) *keepingStore {
	k := &keepingStore{presenceStore: &presenceStore{countingStore: f.objects}, copies: map[string]*proto.Object{}, sent: map[string]bool{}}
	f.walker.Objects = k

	return k
}

func (k *keepingStore) keep(obj *proto.Object) {
	if !obj.Type().Metadata() {
		return
	}

	k.mtx.Lock()
	defer k.mtx.Unlock()

	k.copies[string(obj.Ref().Hash)] = obj
}

func (k *keepingStore) copyOf(ref *proto.Ref) (*proto.Object, bool) {
	k.mtx.Lock()
	defer k.mtx.Unlock()

	obj, ok := k.copies[string(ref.GetHash())]

	return obj, ok
}

func (k *keepingStore) record(obj *proto.Object) {
	if obj.Type() != proto.ObjectType_TREE {
		return
	}

	k.mtx.Lock()
	defer k.mtx.Unlock()

	k.sent[string(obj.Ref().Hash)] = true
}

// sentTrees returns the trees sent since the last call.
func (k *keepingStore) sentTrees() map[string]bool {
	k.mtx.Lock()
	defer k.mtx.Unlock()

	sent := k.sent
	k.sent = map[string]bool{}

	return sent
}

func (k *keepingStore) Put(ctx context.Context, obj *proto.Object) error {
	k.record(obj)

	err := k.presenceStore.Put(ctx, obj)
	if err == nil {
		k.keep(obj)
	}

	return err
}

func (k *keepingStore) PutAssuming(ctx context.Context, obj *proto.Object, assumed []*proto.Ref) ([]*proto.Ref, error) {
	k.record(obj)

	return k.presenceStore.PutAssuming(ctx, obj, assumed)
}

func (k *keepingStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	if obj, ok := k.copyOf(ref); ok {
		return obj, nil
	}

	obj, err := k.presenceStore.Get(ctx, ref)
	if err == nil {
		k.keep(obj)
	}

	return obj, err
}

func (k *keepingStore) GetTree(ctx context.Context, ref *proto.Ref, maxDepth uint32) ([]*proto.Object, error) {
	if root, ok := k.copyOf(ref); ok {
		objects := []*proto.Object{root}

		for _, split := range root.GetTree().GetSplits() {
			if obj, ok := k.copyOf(split); ok {
				objects = append(objects, obj)
			}
		}

		if len(objects) == len(root.GetTree().GetSplits())+1 {
			return objects, nil
		}
	}

	objects, err := k.presenceStore.GetTree(ctx, ref, maxDepth)
	for _, obj := range objects {
		k.keep(obj)
	}

	return objects, err
}

func (k *keepingStore) Keep(_ context.Context, obj *proto.Object) { k.keep(obj) }

func (k *keepingStore) Kept(ref *proto.Ref) bool {
	_, ok := k.copyOf(ref)
	return ok
}

// splits returns the splits of the directory at name in the commit's root.
func (f *walkerFixture) splits(result *WalkResult, name string) []*proto.Ref {
	f.t.Helper()

	obj, err := f.store.Get(context.Background(), f.tree(result.Commit.Tree)[name].Ref)
	require.NoError(f.t, err)

	splits := obj.GetTree().GetSplits()
	require.Greater(f.t, len(splits), 2, "%s is split", name)

	return splits
}

func containsRef(refs []*proto.Ref, ref *proto.Ref) bool {
	for _, r := range refs {
		if r.Equal(ref) {
			return true
		}
	}

	return false
}

func writeMany(f *walkerFixture, count int) {
	for i := 0; i < count; i++ {
		f.write(fmt.Sprintf("many/file-%04d.txt", i), []byte(fmt.Sprintf("content %d", i)))
	}
}

func TestASecondCommitOfALargeTreeSendsOnlyTheSplitsThatChanged(t *testing.T) {
	f := newWalkerFixture(t)
	store := keepingSplits(f)
	writeMany(f, 1000)

	first := f.run()
	before := f.splits(first, "many")
	require.Len(t, store.sentTrees(), len(before)+2, "the first commit sends every split")

	f.write("many/file-0500.txt", []byte("changed"))

	second := f.run()
	after := f.splits(second, "many")
	sent := store.sentTrees()

	resent := 0
	for _, split := range after {
		if containsRef(before, split) {
			require.False(t, sent[string(split.Hash)], "split %x is unchanged", split.Hash)
		} else {
			require.True(t, sent[string(split.Hash)], "split %x changed", split.Hash)
			resent++
		}
	}

	require.Equal(t, 1, resent)
	require.Len(t, sent, resent+2, "the changed split, its directory and the root")
	require.Len(t, f.tree(f.tree(second.Commit.Tree)["many"].Ref), 1000)
}

func TestASplitTheStoreLostSinceIsSentAgain(t *testing.T) {
	f := newWalkerFixture(t)
	store := keepingSplits(f)
	writeMany(f, 1000)

	first := f.run()
	splits := f.splits(first, "many")
	lost := splits[len(splits)-1]

	head, err := f.store.Get(context.Background(), splits[0])
	require.NoError(t, err)

	require.NoError(t, f.store.Delete(context.Background(), lost))
	store.sentTrees()

	f.write("many/"+string(head.GetTree().Nodes[0].Stat.Name), []byte("changed"))

	second := f.run()
	require.True(t, store.sentTrees()[string(lost.Hash)], "the store said it lacks the split")
	require.Equal(t, 1, store.missing)

	after := f.splits(second, "many")
	require.True(t, containsRef(after, lost))
	require.Len(t, f.tree(f.tree(second.Commit.Tree)["many"].Ref), 1000, "the store holds every split")
}

package sql

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// recordStore reads records together and counts the trees and files asked
// for one at a time.
type recordStore struct {
	*memStore
	gets atomic.Int64
}

func (r *recordStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	obj, err := r.memStore.Get(ctx, ref)
	if obj.Type() == proto.ObjectType_TREE || obj.Type() == proto.ObjectType_FILE {
		r.gets.Add(1)
	}

	return obj, err
}

func (r *recordStore) ReadRecords(ctx context.Context, refs []*proto.Ref) ([]*proto.Object, error) {
	out := make([]*proto.Object, len(refs))
	for i, ref := range refs {
		out[i], _ = r.memStore.Get(ctx, ref)
	}

	return out, nil
}

func TestIndexingACommitAndBuildingItsFilterReadTreesAndFilesTogether(t *testing.T) {
	f := newFixture(t)
	store := &recordStore{memStore: f.store}
	f.x = openIndex(t, store)
	f.x.Now = func() time.Time { return f.clock }

	var dirs []*proto.TreeNode
	for d := range 4 {
		var files []*proto.TreeNode
		for n := range 5 {
			files = append(files, f.file("f"+strconv.Itoa(n), "content "+strconv.Itoa(d)+"/"+strconv.Itoa(n)))
		}

		dirs = append(dirs, f.dir("d"+strconv.Itoa(d), f.dir("inner", files...)))
	}

	f.commit("world", f.tree(dirs...), false)

	require.LessOrEqual(t, store.gets.Load(), int64(1), "only the commit's root tree is read on its own")
}

package backup

import (
	"context"
	"sync"

	"github.com/gobackio/goback/proto"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type concurrentTreeNode struct {
	prefix string
	object *proto.Object
}

// SkipTree, returned by a TraverserHandler for a directory, leaves that
// subtree unvisited.
var SkipTree = errors.New("skip tree")

// TraverserHandler receives every visited node with its slash-joined path.
type TraverserHandler func(string, *proto.TreeNode) error

// TraverseTree visits every node under tree, calling handler from workers
// goroutines at once; an error other than SkipTree stops the traversal.
func TraverseTree(ctx context.Context, store ObjectStore, tree *proto.Object, workers int, handler TraverserHandler) error {
	tr := &concurrentTreeTraverser{
		store:      store,
		queue:      make(chan *concurrentTreeNode),
		traverseFn: handler,
	}

	return tr.run(ctx, tree, workers)
}

type concurrentTreeTraverser struct {
	store      ObjectStore
	queue      chan *concurrentTreeNode
	wg         sync.WaitGroup
	traverseFn TraverserHandler
}

func (c *concurrentTreeTraverser) run(ctx context.Context, root *proto.Object, workers int) error {
	grp, ctx := errgroup.WithContext(ctx)

	if workers < 1 {
		return errors.New("Need at least one worker")
	}

	for i := 0; i < workers; i++ {
		grp.Go(c.traverser(ctx))
	}

	c.wg.Add(1)
	c.queue <- &concurrentTreeNode{
		object: root,
	}

	c.wg.Wait()

	close(c.queue)

	return grp.Wait()
}

func (c *concurrentTreeTraverser) traverseTree(ctx context.Context, t *concurrentTreeNode) error {
	// directories go first so sub-trees reach idle workers early; files
	// follow in a second pass
	for _, node := range t.object.GetTree().GetNodes() {
		info := node.Stat
		if !info.IsDir() {
			continue
		}

		err := c.traverseFn(proto.JoinPath(t.prefix, info.Name), node)
		if err != nil {
			if errors.Is(err, SkipTree) {
				continue
			}

			return err
		}

		if info.Lost {
			continue
		}

		subTree, err := LoadTree(ctx, c.store, node.Ref)
		if err != nil {
			return errors.Wrapf(err, "Sub tree %x could not be retrieved", node.Ref.Hash)
		}

		subTreeNode := &concurrentTreeNode{
			prefix: proto.JoinPath(t.prefix, info.Name),
			object: proto.NewObject(subTree),
		}

		c.wg.Add(1)
		// an idle worker takes the sub-tree; without one it is done here
		select {
		case c.queue <- subTreeNode:

		default:
			c.wg.Done()
			err = c.traverseTree(ctx, subTreeNode)
			if err != nil {
				return err
			}
		}
	}

	for _, node := range t.object.GetTree().GetNodes() {
		info := node.Stat
		if info.IsDir() {
			continue
		}

		err := c.traverseFn(proto.JoinPath(t.prefix, info.Name), node)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *concurrentTreeTraverser) traverser(ctx context.Context) func() error {
	return func() error {
		for {
			done := ctx.Done()

			select {
			case n := <-c.queue:
				if n == nil {
					return nil
				}

				err := c.traverseTree(ctx, n)
				c.wg.Done()

				if err != nil {
					return err
				}

			case <-done:
				return ctx.Err()
			}
		}
	}
}

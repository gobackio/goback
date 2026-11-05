package backup

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/gobackio/goback/backup/presence"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"golang.org/x/sync/errgroup"
)

// PresenceScope says whose commits feed the filters an agent receives.
type PresenceScope int

const (
	// PresenceOff hands out no filters.
	PresenceOff PresenceScope = iota
	// PresenceSet covers the agent's own set.
	PresenceSet
	// PresenceStore covers every set of the store.
	PresenceStore
)

// ParsePresenceScope reads off, set or store.
func ParsePresenceScope(s string) (PresenceScope, error) {
	switch strings.ToLower(s) {
	case "off":
		return PresenceOff, nil
	case "set":
		return PresenceSet, nil
	case "store":
		return PresenceStore, nil
	}

	return PresenceOff, fmt.Errorf("unknown presence scope %q", s)
}

// String implements fmt.Stringer.
func (s PresenceScope) String() string {
	switch s {
	case PresenceSet:
		return "set"
	case PresenceStore:
		return "store"
	}

	return "off"
}

// PresenceSource hands an agent the filters of its scope.
type PresenceSource interface {
	// Presence returns the filters for a run on set.
	Presence(ctx context.Context, set string) (presence.Set, error)
}

// Confirmer stores a file or tree object once the store holds every part
// or split the caller assumed it holds and did not send; otherwise it
// stores nothing and names the missing refs.
type Confirmer interface {
	// PutAssuming stores obj when every assumed ref is held; otherwise it
	// returns the refs that are not.
	PutAssuming(ctx context.Context, obj *proto.Object, assumed []*proto.Ref) (missing []*proto.Ref, err error)
}

// Batcher is a store that answers about many refs, and takes many objects,
// in one call.
type Batcher interface {
	// Missing returns the refs the store does not hold, in the order given.
	Missing(ctx context.Context, refs []*proto.Ref) ([]*proto.Ref, error)
	// PutObjects stores objects, none of them a commit or pin, each after
	// the ones it references; a refusal can leave some of them stored.
	PutObjects(ctx context.Context, objects []*proto.Object) error
}

// PresenceIndex serves the filters of a scope.
type PresenceIndex interface {
	// Presence returns the filters of scope; set matters for PresenceSet.
	Presence(ctx context.Context, scope PresenceScope, set string) ([]*proto.PresenceFilter, error)
}

// PolicySource knows the store's policy; nil means the store has none set.
type PolicySource interface {
	// StorePolicy returns the store's policy, or nil.
	StorePolicy(ctx context.Context) (*storekey.Policy, error)
}

// HasAller is implemented by stores that answer Has for many refs at once.
type HasAller interface {
	// HasAll reports Has for each of refs, in their order.
	HasAll(ctx context.Context, refs []*proto.Ref) ([]bool, error)
}

// HasAll reports Has for each of refs, in their order, through the store's
// HasAller when it is one.
func HasAll(ctx context.Context, store ObjectStore, refs []*proto.Ref) ([]bool, error) {
	if haser, ok := store.(HasAller); ok {
		return haser.HasAll(ctx, refs)
	}

	present := make([]bool, len(refs))
	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(16)

	for i, ref := range refs {
		grp.Go(func() error {
			has, err := store.Has(ctx, ref)
			present[i] = has

			return err
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	return present, nil
}

// Missing returns the refs the store does not hold, in the order given.
func Missing(ctx context.Context, store ObjectStore, refs []*proto.Ref) ([]*proto.Ref, error) {
	present, err := HasAll(ctx, store, refs)
	if err != nil {
		return nil, err
	}

	var missing []*proto.Ref
	for i, ref := range refs {
		if !present[i] {
			missing = append(missing, ref)
		}
	}

	return missing, nil
}

// CollectPresence walks a tree and returns a filter over every blob ref it
// reaches, and how many tree and file objects it read, reading each level of
// the tree and the files on it together.
func CollectPresence(ctx context.Context, store ObjectStore, tree *proto.Ref) (*presence.Filter, int, error) {
	refs := map[string]struct{}{}
	add := func(parts []*proto.FilePart) {
		for _, part := range parts {
			if part.GetRef() != nil {
				refs[string(part.Ref.Hash)] = struct{}{}
			}
		}
	}

	read := 0
	level := []*proto.Ref{tree}
	for len(level) > 0 {
		var next, files []*proto.Ref

		err := readChunks(ctx, store, level, func(ref *proto.Ref, obj *proto.Object) error {
			t := obj.GetTree()
			if t == nil {
				return fmt.Errorf("object %x is not a tree", ref.Hash)
			}

			next = append(next, t.Splits...)

			for _, node := range t.GetNodes() {
				switch {
				case node.Ref == nil:
				case node.GetStat().IsDir():
					next = append(next, node.Ref)
				case node.GetStat().GetType() == proto.NodeType_NODE_FILE:
					files = append(files, node.Ref)
				}
			}

			return nil
		})
		if err != nil {
			return nil, read, err
		}

		read += len(level)

		if err := readFileParts(ctx, store, files, add); err != nil {
			return nil, read, err
		}

		read += len(files)
		level = next
	}

	filter := presence.New(uint64(len(refs)) * 5 / 4)
	for ref := range refs {
		filter.Add([]byte(ref))
	}

	return filter, read, nil
}

// UpdatePresence returns a copy of old, the filter of a commit whose tree
// is oldTree, that also holds every blob ref tree reaches, and how many
// tree and file objects it read. It skips every entry of tree whose ref
// equals the one at the same path in oldTree, so its reads grow with what
// changed, not with the tree.
func UpdatePresence(ctx context.Context, store ObjectStore, old *presence.Filter, oldTree, tree *proto.Ref) (*presence.Filter, int, error) {
	filter := old.Clone()
	add := func(parts []*proto.FilePart) {
		for _, part := range parts {
			if part.GetRef() != nil {
				filter.Add(part.Ref.Hash)
			}
		}
	}

	read := 0
	dirs := []*presenceDir{{old: []*proto.Ref{oldTree}, new: []*proto.Ref{tree}}}

	for len(dirs) > 0 {
		var refs, files []*proto.Ref
		for _, d := range dirs {
			d.dropCommon()
			refs = append(append(refs, d.old...), d.new...)
		}

		objects, err := readObjects(ctx, store, refs)
		if err != nil {
			return nil, read, err
		}

		read += len(refs)

		var next []*presenceDir
		at := 0
		for _, d := range dirs {
			n := len(d.old) + len(d.new)
			if err := d.expand(refs[at:at+n], objects[at:at+n]); err != nil {
				return nil, read, err
			}

			at += n

			if len(d.old)+len(d.new) > 0 {
				next = append(next, d)
				continue
			}

			next, files = d.changed(next, files)
		}

		if err := readFileParts(ctx, store, files, add); err != nil {
			return nil, read, err
		}

		read += len(files)
		dirs = next
	}

	return filter, read, nil
}

// presenceDir is one directory UpdatePresence compares: the tree objects
// of each side still to read, and the nodes read so far.
type presenceDir struct {
	old, new           []*proto.Ref
	oldNodes, newNodes []*proto.TreeNode
}

// dropCommon drops the tree objects both sides still have to read: names
// are unique within a directory, so the nodes they hold are the same on
// both sides and pair with nothing else.
func (d *presenceDir) dropCommon() {
	common := map[string]bool{}
	for _, ref := range d.old {
		common[string(ref.GetHash())] = false
	}

	for _, ref := range d.new {
		if _, ok := common[string(ref.GetHash())]; ok {
			common[string(ref.GetHash())] = true
		}
	}

	keep := func(refs []*proto.Ref) []*proto.Ref {
		return slices.DeleteFunc(refs, func(ref *proto.Ref) bool { return common[string(ref.GetHash())] })
	}

	d.old, d.new = keep(d.old), keep(d.new)
}

// expand collects the nodes of the objects read for d's pending refs, old
// side first, and makes their splits the refs still pending.
func (d *presenceDir) expand(refs []*proto.Ref, objects []*proto.Object) error {
	var oldSplits, newSplits []*proto.Ref
	for i, obj := range objects {
		t := obj.GetTree()
		if t == nil {
			return fmt.Errorf("object %x is not a tree", refs[i].Hash)
		}

		if i < len(d.old) {
			oldSplits = append(oldSplits, t.Splits...)
			d.oldNodes = append(d.oldNodes, t.GetNodes()...)
		} else {
			newSplits = append(newSplits, t.Splits...)
			d.newNodes = append(d.newNodes, t.GetNodes()...)
		}
	}

	d.old, d.new = oldSplits, newSplits

	return nil
}

// changed queues the subdirectories of d whose refs differ from the old
// side's at the same name, and lists its files whose refs differ.
func (d *presenceDir) changed(dirs []*presenceDir, files []*proto.Ref) ([]*presenceDir, []*proto.Ref) {
	old := make(map[string]*proto.TreeNode, len(d.oldNodes))
	for _, node := range d.oldNodes {
		old[string(node.GetStat().GetName())] = node
	}

	for _, node := range d.newNodes {
		if node.Ref == nil {
			continue
		}

		was := old[string(node.GetStat().GetName())]
		if was.GetRef() != nil && bytes.Equal(was.Ref.Hash, node.Ref.Hash) {
			continue
		}

		switch {
		case node.GetStat().IsDir():
			sub := &presenceDir{new: []*proto.Ref{node.Ref}}
			if was.GetRef() != nil && was.GetStat().IsDir() {
				sub.old = []*proto.Ref{was.Ref}
			}

			dirs = append(dirs, sub)
		case node.GetStat().GetType() == proto.NodeType_NODE_FILE:
			files = append(files, node.Ref)
		}
	}

	return dirs, files
}

// readFileParts reads the file objects of refs and hands the parts of each
// to add.
func readFileParts(ctx context.Context, store ObjectStore, refs []*proto.Ref, add func([]*proto.FilePart)) error {
	return readChunks(ctx, store, refs, func(ref *proto.Ref, obj *proto.Object) error {
		file := obj.GetFile()
		if file == nil {
			return fmt.Errorf("object %x is not a file", ref.Hash)
		}

		parts, err := FileParts(ctx, store, file)
		if err != nil {
			return fmt.Errorf("file %x: %w", ref.Hash, err)
		}

		add(parts)

		return nil
	})
}

// readChunks hands each of refs to fn with its object, in order, reading
// them recordChunk at a time.
func readChunks(ctx context.Context, store Getter, refs []*proto.Ref, fn func(*proto.Ref, *proto.Object) error) error {
	for start := 0; start < len(refs); start += recordChunk {
		chunk := refs[start:min(start+recordChunk, len(refs))]

		objects, err := readObjects(ctx, store, chunk)
		if err != nil {
			return err
		}

		for i, ref := range chunk {
			if err := fn(ref, objects[i]); err != nil {
				return err
			}
		}
	}

	return nil
}

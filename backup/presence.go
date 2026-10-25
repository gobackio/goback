package backup

import (
	"context"
	"fmt"
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

// Confirmer stores a file object once the store holds every part the
// caller assumed; otherwise it stores nothing and names the missing refs.
type Confirmer interface {
	// PutFile stores obj when every assumed ref is held; otherwise it
	// returns the refs that are not.
	PutFile(ctx context.Context, obj *proto.Object, assumed []*proto.Ref) (missing []*proto.Ref, err error)
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

// VerifiedHaser is implemented by stores that tell the copies they
// verified, stored in the clear with the ref recomputed from their bytes,
// from sealed copies, whose ref is the writer's word.
type VerifiedHaser interface {
	// HasVerified is HasAll counting only verified copies.
	HasVerified(ctx context.Context, refs []*proto.Ref) ([]bool, error)
}

// HasVerified is HasAll counting only the copies the store verified, where
// it is a VerifiedHaser, and every copy where it is not. A plaintext writer
// relies on it, so that a sealed copy forged under a plaintext ref never
// stands in for that content.
func HasVerified(ctx context.Context, store ObjectStore, refs []*proto.Ref) ([]bool, error) {
	if verifier, ok := store.(VerifiedHaser); ok {
		return verifier.HasVerified(ctx, refs)
	}

	return HasAll(ctx, store, refs)
}

// MissingVerified is Missing by HasVerified.
func MissingVerified(ctx context.Context, store ObjectStore, refs []*proto.Ref) ([]*proto.Ref, error) {
	present, err := HasVerified(ctx, store, refs)
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

// VerifiedBatcher is a Batcher that also answers which refs it holds no
// verified copy of, as MissingVerified does.
type VerifiedBatcher interface {
	Batcher
	MissingVerified(ctx context.Context, refs []*proto.Ref) ([]*proto.Ref, error)
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
// reaches, reading each level of the tree and the files on it together.
func CollectPresence(ctx context.Context, store ObjectStore, tree *proto.Ref) (*presence.Filter, error) {
	refs := map[string]struct{}{}
	add := func(parts []*proto.FilePart) {
		for _, part := range parts {
			refs[string(part.GetRef().GetHash())] = struct{}{}
		}
	}

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
			return nil, err
		}

		err = readChunks(ctx, store, files, func(ref *proto.Ref, obj *proto.Object) error {
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
		if err != nil {
			return nil, err
		}

		level = next
	}

	filter := presence.New(uint64(len(refs)))
	for ref := range refs {
		filter.Add([]byte(ref))
	}

	return filter, nil
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

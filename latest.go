package goback

import (
	"context"
	"errors"
	"fmt"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"
	"github.com/gobackio/goback/storage/wrapped"
)

// LatestCommit is the newest commit of set, a set name or a numeric set
// id: an id is read from the set's head, a name from the index, falling
// back to the heads the store keeps when the index has none.
func (r *Repo) LatestCommit(ctx context.Context, set string) (*proto.Ref, error) {
	if id, ok := backup.SetID(set); ok {
		return headOf(r.Store, id)
	}

	ref, err := r.Index.LatestCommit(ctx, set)
	if !errors.Is(err, backup.ErrNotFound) {
		return ref, err
	}

	ps, ok := wrapped.Innermost(r.Store).(*pack.PackStorage)
	if !ok {
		return nil, err
	}

	heads, herr := ps.Heads()
	if herr != nil {
		return nil, herr
	}

	var newest *pack.Head
	for i, head := range heads {
		if head.Set == set && (newest == nil || head.ReceivedAtNs > newest.ReceivedAtNs) {
			newest = &heads[i]
		}
	}

	if newest == nil {
		return nil, err
	}

	return &proto.Ref{Hash: newest.Commit}, nil
}

// headOf is the newest commit of the set with id.
func headOf(store backup.ObjectStore, id uint64) (*proto.Ref, error) {
	ps, ok := wrapped.Innermost(store).(*pack.PackStorage)
	if !ok {
		return nil, fmt.Errorf("store %T keeps no set heads to find set %d in", store, id)
	}

	heads, err := ps.Heads()
	if err != nil {
		return nil, err
	}

	for _, head := range heads {
		if head.SetID == id {
			return &proto.Ref{Hash: head.Commit}, nil
		}
	}

	return nil, fmt.Errorf("%w: set %d has no head", backup.ErrNotFound, id)
}

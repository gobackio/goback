package storage

import (
	"context"
	"fmt"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

// splitLoad is how many sub-files of a split file a part walk reads at
// once.
const splitLoad = 64

// partWalk visits the parts of a file in order, holding only the sub-files
// of the splits it is inside, read splitLoad at a time, so a file whose
// splits repeat costs no memory for the parts it expands to.
type partWalk struct {
	ctx   context.Context
	store backup.Getter
	stack []*partFrame
	at    int

	// entered gathers the ref of each sub-file the walk goes into
	entered []*proto.Ref

	// with stepOver set, a sub-file whose every part a walk took before
	// is stepped over, its parts counted but not visited
	stepOver bool
	counts   map[string]int
	complete map[string]bool
}

type partFrame struct {
	file  *proto.File
	ref   *proto.Ref
	start int
	next  int

	loaded   []*proto.File
	loadedAt int

	missed bool
}

func newPartWalk(ctx context.Context, store backup.Getter, file *proto.File) *partWalk {
	return &partWalk{
		ctx:      ctx,
		store:    store,
		stack:    []*partFrame{{file: file}},
		counts:   map[string]int{},
		complete: map[string]bool{},
	}
}

// next returns the index and the part the walk is at, and a nil part once
// it has visited them all.
func (w *partWalk) next() (int, *proto.FilePart, error) {
	for len(w.stack) > 0 {
		frame := w.stack[len(w.stack)-1]
		file := frame.file

		if parts := file.GetParts(); frame.next < len(parts) {
			part := parts[frame.next]
			frame.next++
			w.at++

			return w.at - 1, part, nil
		}

		splits := file.GetSplits()
		if frame.next >= len(splits) {
			w.pop()
			continue
		}

		i, ref := frame.next, splits[frame.next]
		frame.next++

		if w.stepOver && w.complete[string(ref.GetHash())] {
			w.at += w.counts[string(ref.GetHash())]
			continue
		}

		sub, err := w.subFile(frame, i)
		if err != nil {
			return 0, nil, err
		}

		w.stack = append(w.stack, &partFrame{file: sub, ref: ref, start: w.at})
		w.entered = append(w.entered, ref)
	}

	return w.at, nil, nil
}

// miss tells the walk the part it last returned was not taken, so the
// sub-files holding it are not stepped over.
func (w *partWalk) miss() {
	if len(w.stack) > 0 {
		w.stack[len(w.stack)-1].missed = true
	}
}

func (w *partWalk) pop() {
	frame := w.stack[len(w.stack)-1]
	w.stack = w.stack[:len(w.stack)-1]

	if frame.ref == nil {
		return
	}

	key := string(frame.ref.GetHash())
	w.counts[key] = w.at - frame.start

	switch {
	case !frame.missed:
		w.complete[key] = true
	case len(w.stack) > 0:
		w.stack[len(w.stack)-1].missed = true
	}
}

// subFile is the sub-file of split i of frame's file, reading it with the
// splits after it that the walk is not to step over.
func (w *partWalk) subFile(frame *partFrame, i int) (*proto.File, error) {
	if i < frame.loadedAt || i >= frame.loadedAt+len(frame.loaded) || frame.loaded[i-frame.loadedAt] == nil {
		if err := w.load(frame, i); err != nil {
			return nil, err
		}
	}

	return frame.loaded[i-frame.loadedAt], nil
}

func (w *partWalk) load(frame *partFrame, from int) error {
	splits := frame.file.GetSplits()[from:min(from+splitLoad, len(frame.file.GetSplits()))]

	var (
		refs  []*proto.Ref
		which = map[string]int{}
	)

	for j, ref := range splits {
		key := string(ref.GetHash())
		if _, ok := which[key]; ok || (j > 0 && w.stepOver && w.complete[key]) {
			continue
		}

		which[key] = len(refs)
		refs = append(refs, ref)
	}

	objects, errs := backup.ReadObjects(w.ctx, w.store, refs)

	frame.loaded, frame.loadedAt = make([]*proto.File, len(splits)), from

	for j, ref := range splits {
		k, ok := which[string(ref.GetHash())]
		if !ok {
			continue
		}

		if errs[k] != nil {
			return fmt.Errorf("split %d (%x) of file: %w", from+j, ref.GetHash(), errs[k])
		}

		if objects[k].GetFile() == nil {
			return fmt.Errorf("split %d (%x) of file is not a file object", from+j, ref.GetHash())
		}

		frame.loaded[j] = objects[k].GetFile()
	}

	return nil
}

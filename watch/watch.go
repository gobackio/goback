// Package watch follows a directory tree for changes, so a backup of it
// can take the directories nothing changed in whole from its previous
// commit. A Source reports what changed through one backend: fanotify or
// inotify on Linux, the NTFS change journal on Windows. A Tracker follows
// a Source for any number of sets, and a Schedule decides, per set, what a
// run reads and when a change starts one.
package watch

import (
	"errors"
	"strings"
)

// Position marks a point in a Source's changes: Stream names an unbroken
// run of them, and Seq orders the changes in it. Positions of different
// streams don't compare.
type Position struct {
	Stream string `json:"stream"`
	Seq    int64  `json:"seq"`
}

// IsZero reports whether p marks nothing.
func (p Position) IsZero() bool {
	return p.Stream == ""
}

// ErrInDoubt is a Source's error when it can't tell what changed since a
// position: its queue overflowed, the position is from another stream or
// too old, or the root was moved or unmounted.
var ErrInDoubt = errors.New("the change tracking can't tell what changed")

// ErrStarting is a Source's error while it is still setting up; it has no
// position yet.
var ErrStarting = errors.New("the change tracking is still starting")

// ErrUnavailable is Open's error when no backend can follow the root.
var ErrUnavailable = errors.New("change tracking is unavailable")

// Source reports the changes under one root through one backend.
type Source interface {
	// Backend names how the source follows the root, as inotify.
	Backend() string
	// Now is the position of the present: a change after it has a Seq at or
	// past its Seq.
	Now() (Position, error)
	// Read is what changed from from on, or ErrInDoubt, and the position it
	// read to, which the next Read starts from.
	Read(from Position) (*Changes, Position, error)
	Close() error
}

// Changes is what changed under a root, as slash-separated paths below it
// ("" is the root), each with the Seq of its latest change.
type Changes struct {
	// Dirs are the directories whose entries changed, each with its
	// ancestors.
	Dirs map[string]int64 `json:"dirs,omitempty"`
	// Trees are the directories everything under which changed, as one
	// created or moved in.
	Trees map[string]int64 `json:"trees,omitempty"`
	// Unwatched are the subtrees the source can't follow, with why; they
	// count as changed until the source starts again.
	Unwatched map[string]string `json:"unwatched,omitempty"`
}

// NewChanges returns empty Changes.
func NewChanges() *Changes {
	return &Changes{Dirs: map[string]int64{}, Trees: map[string]int64{}, Unwatched: map[string]string{}}
}

// Mark records that the directory rel changed at seq, and so did each of
// its ancestors.
func (c *Changes) Mark(rel string, seq int64) {
	for {
		if prev, ok := c.Dirs[rel]; !ok || prev < seq {
			c.Dirs[rel] = seq
		}

		if rel == "" {
			return
		}

		rel = parent(rel)
	}
}

// MarkTree records that everything under the directory rel changed at seq.
func (c *Changes) MarkTree(rel string, seq int64) {
	if prev, ok := c.Trees[rel]; !ok || prev < seq {
		c.Trees[rel] = seq
	}

	c.Mark(rel, seq)
}

// Unwatch records that the source can't follow the subtree at rel.
func (c *Changes) Unwatch(rel, why string) {
	c.Unwatched[rel] = why
}

// Len is how many paths c holds.
func (c *Changes) Len() int {
	return len(c.Dirs) + len(c.Trees) + len(c.Unwatched)
}

func (c *Changes) merge(o *Changes) {
	for rel, seq := range o.Dirs {
		if prev, ok := c.Dirs[rel]; !ok || prev < seq {
			c.Dirs[rel] = seq
		}
	}

	for rel, seq := range o.Trees {
		if prev, ok := c.Trees[rel]; !ok || prev < seq {
			c.Trees[rel] = seq
		}
	}

	for rel, why := range o.Unwatched {
		c.Unwatched[rel] = why
	}
}

// since is the part of c at or after seq, the unwatched subtrees with it.
func (c *Changes) since(seq int64) *Changes {
	out := NewChanges()

	for rel, s := range c.Dirs {
		if s >= seq {
			out.Dirs[rel] = s
		}
	}

	for rel, s := range c.Trees {
		if s >= seq {
			out.Trees[rel] = s
		}
	}

	for rel, why := range c.Unwatched {
		out.Unwatched[rel] = why
	}

	return out
}

func (c *Changes) prune(floor int64) {
	for rel, s := range c.Dirs {
		if s < floor {
			delete(c.Dirs, rel)
		}
	}

	for rel, s := range c.Trees {
		if s < floor {
			delete(c.Trees, rel)
		}
	}
}

// unchanged reports whether nothing at or under rel is in c: no change in
// it or below it, no changed or unwatched tree holding it, and no unwatched
// subtree below it.
func (c *Changes) unchanged(rel string) bool {
	if _, ok := c.Dirs[rel]; ok {
		return false
	}

	for p := rel; ; p = parent(p) {
		if _, ok := c.Trees[p]; ok {
			return false
		}

		if _, ok := c.Unwatched[p]; ok {
			return false
		}

		if p == "" {
			break
		}
	}

	for u := range c.Unwatched {
		if rel == "" || strings.HasPrefix(u, rel+"/") {
			return false
		}
	}

	return true
}

func parent(rel string) string {
	if i := strings.LastIndexByte(rel, '/'); i >= 0 {
		return rel[:i]
	}

	return ""
}

func join(dir, name string) string {
	if dir == "" {
		return name
	}

	return dir + "/" + name
}

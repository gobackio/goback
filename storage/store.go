// Package storage serves a store to agents over gRPC and reaches one: Store
// holds a store's operations over its index, Server serves them, and
// Client is the backup.Index an agent uses to talk to a store server.
// BucketStore keeps a store's archives in a gocloud bucket.
package storage

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/gobackio/goback/auth"
	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/upload"

	"golang.org/x/sync/errgroup"
)

// ErrInvalidRequest is a call the store cannot act on as given: a malformed
// ref, an object that fails validation, a ref of the wrong object type.
var ErrInvalidRequest = errors.New("invalid request")

// Store is one store's operations over an index and its sessions: the work
// behind every RPC of proto.Store, with no transport in it. The caller's
// principal and session travel in the context.
type Store struct {
	Index    backup.Index
	Sessions backup.SessionStore

	// Lease is what BeginSession promises the client: the session
	// store's own when it has one.
	Lease time.Duration

	// CommitUploaded, when set, takes the commits an upload file sent to
	// SendUpload held under the upload's id, once the rest of the file is
	// stored, and answers where the upload stands; nil puts the commits at
	// once, with PutCommits.
	CommitUploaded func(ctx context.Context, upload string, commits []*proto.Object) (backup.UploadStatus, error)

	uploads uploads
}

// NewStore returns a Store over index. sessions is nil for a store without
// sessions.
func NewStore(index backup.Index, sessions backup.SessionStore) *Store {
	s := &Store{
		Index:    index,
		Sessions: sessions,
		Lease:    30 * time.Minute,
	}

	if leased, ok := sessions.(backup.Leased); ok && leased.SessionLease() > 0 {
		s.Lease = leased.SessionLease()
	}

	return s
}

// BeginCommit asks the index whether the caller may commit to set. An index
// without a gate allows every commit and assigns the set id later; a
// refusal is backup.ErrCommitDenied with the reason.
func (s *Store) BeginCommit(ctx context.Context, set string) (*backup.CommitGrant, error) {
	gate, ok := s.Index.(backup.CommitGate)
	if !ok {
		return &backup.CommitGrant{}, nil
	}

	return gate.BeginCommit(ctx, set)
}

// Upload is one Put: the object, the ref the client computed for it (nil
// for a commit or pin, which the store stamps itself) and the refs a file
// assumes are stored already.
type Upload struct {
	Object  *proto.Object
	Ref     *proto.Ref
	Assumed []*proto.Ref
}

// Receipt answers an Upload: the stored ref, the stamped object for a
// commit or pin, or the assumed refs that are missing, in which case
// nothing was stored.
type Receipt struct {
	Ref     *proto.Ref
	Object  *proto.Object
	Missing []*proto.Ref
}

// Put stores up.Object after recomputing its ref from the received bytes;
// a client ref that does not match is proto.ErrRefMismatch. Commits and
// pins are stamped with the receipt time, and a commit with its set id, so
// their ref is computed here and returned with the object.
func (s *Store) Put(ctx context.Context, up Upload) (*Receipt, error) {
	object := up.Object

	err := object.Validate()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}

	// a policy rules every set of the store once reindexed, so only the
	// store itself writes one
	if object.Type() == proto.ObjectType_POLICY {
		return nil, fmt.Errorf("%w: policies are written by the store", ErrInvalidRequest)
	}

	stamped := object.GetCommit() != nil || object.GetPin() != nil

	if !stamped && up.Ref != nil && !object.Ref().Equal(up.Ref) {
		return nil, fmt.Errorf("%w: client sent %x, server computed %x", proto.ErrRefMismatch, up.Ref.Hash, object.Ref().Hash)
	}

	if stamped {
		if err := admitStamped(ctx, object); err != nil {
			return nil, err
		}
	}

	if _, err := s.writeSession(ctx); err != nil {
		return nil, err
	}

	if len(up.Assumed) > 0 {
		if object.GetFile() == nil {
			return nil, fmt.Errorf("%w: assumed refs are only accepted for file objects", ErrInvalidRequest)
		}

		// a file written in the clear relies only on parts the store
		// verified, never on a sealed copy forged under a part's ref
		lookUp := backup.MissingVerified
		if len(object.KeyId) > 0 {
			lookUp = backup.Missing
		}

		missing, err := lookUp(ctx, s.Index, up.Assumed)
		if err != nil {
			return nil, err
		}

		if len(missing) > 0 {
			return &Receipt{Ref: object.Ref(), Missing: missing}, nil
		}
	}

	err = s.Index.Put(ctx, object)

	var missing *backup.MissingError
	if errors.As(err, &missing) {
		return &Receipt{Ref: object.Ref(), Missing: missing.Refs}, nil
	}

	if err != nil {
		return nil, err
	}

	receipt := &Receipt{Ref: object.Ref()}
	if stamped {
		receipt.Object = object
	}

	return receipt, nil
}

// admitStamped refuses a commit or pin the caller may not store and
// clears what of its stamp the store assigns.
func admitStamped(ctx context.Context, object *proto.Object) error {
	p, err := auth.Require(ctx)
	if err != nil {
		return err
	}

	if commit := object.GetCommit(); commit != nil {
		if err := p.AuthorizeCommit(commit); err != nil {
			return err
		}
	}

	// whatever the client wrote there, the server decides
	clearStamp(object)

	return nil
}

// MaxCommits is the most commits one PutCommits call takes, as many as an
// upload file holds.
const MaxCommits = upload.MaxCommits

// CommitsReceipt answers PutCommits: the commits as stored, in the order
// given, or, when any of them lacks something, what each lacks, in which
// case none was stored.
type CommitsReceipt struct {
	Commits []*proto.Object
	Missing [][]*proto.Ref
}

// PutCommits stores commits of the caller's session together, as
// backup.CommitBatcher does, each stamped as Put stamps one; at most
// MaxCommits at once. A single commit is put as Put puts it.
func (s *Store) PutCommits(ctx context.Context, commits []*proto.Object) (*CommitsReceipt, error) {
	if len(commits) == 0 || len(commits) > MaxCommits {
		return nil, fmt.Errorf("%w: %d commits in one call, 1 to %d", ErrInvalidRequest, len(commits), MaxCommits)
	}

	if len(commits) == 1 {
		if commits[0].GetCommit() == nil {
			return nil, fmt.Errorf("%w: PutCommits takes commits, not a %s", ErrInvalidRequest, commits[0].Type())
		}

		receipt, err := s.Put(ctx, Upload{Object: commits[0]})
		switch {
		case err != nil:
			return nil, err
		case len(receipt.Missing) > 0:
			return &CommitsReceipt{Missing: [][]*proto.Ref{receipt.Missing}}, nil
		}

		return &CommitsReceipt{Commits: []*proto.Object{receipt.Object}}, nil
	}

	for _, object := range commits {
		if err := object.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}

		if object.GetCommit() == nil {
			return nil, fmt.Errorf("%w: PutCommits takes commits, not a %s", ErrInvalidRequest, object.Type())
		}

		if err := admitStamped(ctx, object); err != nil {
			return nil, err
		}
	}

	if _, err := s.writeSession(ctx); err != nil {
		return nil, err
	}

	err := backup.PutCommits(ctx, s.Index, commits)

	var missing *backup.MissingError
	if errors.As(err, &missing) {
		return &CommitsReceipt{Missing: byCommit(missing, len(commits))}, nil
	}

	if err != nil {
		return nil, err
	}

	return &CommitsReceipt{Commits: commits}, nil
}

// byCommit is what each of n commits lacks; a store that did not say which
// lacks what has each lack it all.
func byCommit(missing *backup.MissingError, n int) [][]*proto.Ref {
	if len(missing.Commits) == n {
		return missing.Commits
	}

	out := make([][]*proto.Ref, n)
	for i := range out {
		out[i] = missing.Refs
	}

	return out
}

// MaxMissingRefs is the most refs one Missing call asks about.
const MaxMissingRefs = 4096

// Missing returns the refs the store does not hold, in the order given; at
// most MaxMissingRefs at once.
func (s *Store) Missing(ctx context.Context, refs []*proto.Ref) ([]*proto.Ref, error) {
	if len(refs) > MaxMissingRefs {
		return nil, fmt.Errorf("%w: %d refs in one call, at most %d", ErrInvalidRequest, len(refs), MaxMissingRefs)
	}

	for _, ref := range refs {
		if !ref.Valid() {
			return nil, fmt.Errorf("%w: malformed ref", ErrInvalidRequest)
		}
	}

	return backup.Missing(ctx, s.Index, refs)
}

var _ backup.VerifiedBatcher = (*Store)(nil)

// MissingVerified is Missing counting only the copies the store verified,
// as backup.MissingVerified does.
func (s *Store) MissingVerified(ctx context.Context, refs []*proto.Ref) ([]*proto.Ref, error) {
	if len(refs) > MaxMissingRefs {
		return nil, fmt.Errorf("%w: %d refs in one call, at most %d", ErrInvalidRequest, len(refs), MaxMissingRefs)
	}

	return backup.MissingVerified(ctx, s.Index, refs)
}

// putObjectsWorkers bounds the objects one PutObjects call stores at once.
const putObjectsWorkers = 32

// PutObjects stores objects as Put stores each without assumed refs, each
// after the ones among them it references. A commit or pin is
// ErrInvalidRequest: Put stamps those.
func (s *Store) PutObjects(ctx context.Context, objects []*proto.Object) error {
	for _, object := range objects {
		if err := object.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidRequest, err)
		}

		switch object.Type() {
		case proto.ObjectType_POLICY, proto.ObjectType_COMMIT, proto.ObjectType_PIN:
			return fmt.Errorf("%w: a %s goes through Put", ErrInvalidRequest, object.Type())
		}
	}

	if s.Sessions != nil {
		if _, ok := backup.SessionFromContext(ctx); !ok {
			return fmt.Errorf("%w: writes need a session", backup.ErrNoSession)
		}
	}

	if batcher, ok := s.Index.(backup.Batcher); ok {
		return batcher.PutObjects(ctx, objects)
	}

	for _, level := range backup.ByReference(objects) {
		grp, gctx := errgroup.WithContext(ctx)
		grp.SetLimit(putObjectsWorkers)

		for _, object := range level {
			grp.Go(func() error { return s.Index.Put(gctx, object) })
		}

		if err := grp.Wait(); err != nil {
			return err
		}
	}

	return nil
}

// clearStamp removes a client-supplied set id, and a pin's receipt time,
// so the index assigns its own. A commit's receipt time is the index's to
// keep or replace.
func clearStamp(object *proto.Object) {
	if commit := object.GetCommit(); commit != nil {
		commit.SetId = 0
	}

	if pin := object.GetPin(); pin != nil {
		pin.ReceivedAtNs = 0
	}
}

func notFound(ref *proto.Ref) error {
	return fmt.Errorf("object %x: %w", ref.GetHash(), backup.ErrNotFound)
}

// readable refuses a malformed ref, and one no set of the store
// references, before anything is read.
func (s *Store) readable(ctx context.Context, ref *proto.Ref) error {
	if !ref.Valid() {
		return fmt.Errorf("%w: malformed ref", ErrInvalidRequest)
	}

	scope, ok := s.Index.(backup.RefScope)
	if !ok {
		return nil
	}

	referenced, err := scope.References(ctx, ref)
	if err != nil {
		return err
	}

	if !referenced {
		return notFound(ref)
	}

	return nil
}

// offered passes on the object types the store serves and hides the rest.
func offered(ref *proto.Ref, object *proto.Object, err error) (*proto.Object, error) {
	if errors.Is(err, backup.ErrNotFound) {
		return nil, notFound(ref)
	}

	if err != nil {
		return nil, err
	}

	switch object.Type() {
	case proto.ObjectType_COMMIT, proto.ObjectType_TREE, proto.ObjectType_FILE:
		return object, nil
	}

	return nil, notFound(ref)
}

// Get fetches a commit, tree or file object the caller references.
// Anything else, and anything absent, is backup.ErrNotFound.
func (s *Store) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	if err := s.readable(ctx, ref); err != nil {
		return nil, err
	}

	object, err := s.Index.Get(ctx, ref)

	return offered(ref, object, err)
}

const (
	// treeBatch is how many trees Tree reads at once.
	treeBatch = 10_000
)

// Tree walks the tree at ref breadth-first, its splits, and the trees of
// directories below it down to maxDepth levels, handing fn each of them
// in walk order. A tree reached again, no shallower than before, is not
// handed over again.
func (s *Store) Tree(ctx context.Context, ref *proto.Ref, maxDepth uint32, fn func(*proto.GetTreeResponse) error) error {
	if err := s.readable(ctx, ref); err != nil {
		return err
	}

	type pending struct {
		ref   *proto.Ref
		depth uint32
	}

	queue := []*pending{{ref: ref}}
	queued := map[string]uint32{string(ref.Hash): 0}

	// a split keeps its tree's depth, so a tree first reached deeper can
	// later be reached shallower and must be walked again from there
	enqueue := func(ref *proto.Ref, depth uint32) {
		if at, ok := queued[string(ref.GetHash())]; !ok || depth < at {
			queued[string(ref.GetHash())] = depth
			queue = append(queue, &pending{ref: ref, depth: depth})
		}
	}

	for read := 0; read < len(queue); {
		batch := queue[read:min(read+treeBatch, len(queue))]

		refs := make([]*proto.Ref, len(batch))
		for i, next := range batch {
			refs[i] = next.ref
		}

		objects, err := s.readTrees(ctx, refs, read == 0)
		if err != nil {
			return err
		}

		for i, next := range batch {
			tree := objects[i].GetTree()
			if tree == nil {
				return fmt.Errorf("%w: object %x is not a tree", ErrInvalidRequest, next.ref.GetHash())
			}

			if err := fn(&proto.GetTreeResponse{Ref: next.ref, Object: objects[i]}); err != nil {
				return err
			}

			for _, split := range tree.Splits {
				enqueue(split, next.depth)
			}

			if next.depth >= maxDepth {
				continue
			}

			for _, node := range tree.Nodes {
				if node.GetStat().IsDir() && node.Ref != nil {
					enqueue(node.Ref, next.depth+1)
				}
			}
		}

		read += len(batch)
	}

	return nil
}

// readTrees reads the objects of refs, in one lookup and a range read per
// run of neighbours where the index can. With root set, refs[0] is the
// walk's root, which is answered like Get.
func (s *Store) readTrees(ctx context.Context, refs []*proto.Ref, root bool) ([]*proto.Object, error) {
	objects, errs := backup.ReadObjects(ctx, s.Index, refs)

	for i, ref := range refs {
		err := errs[i]

		switch {
		case root && i == 0:
			if _, err := offered(ref, objects[i], err); err != nil {
				return nil, err
			}
		case errors.Is(err, backup.ErrNotFound):
			return nil, fmt.Errorf("tree %x: %w", ref.GetHash(), backup.ErrNotFound)
		case err != nil:
			return nil, err
		}
	}

	return objects, nil
}

const (
	// readFileWorkers bounds the reads one ReadFile or ReadFiles call has
	// in flight.
	readFileWorkers = 16
	// readFileBatch is how many parts ReadFile locates at once.
	readFileBatch = 1024
)

// ReadFile hands fn the file's parts except the indexes in skip: runs
// of them the index can locate, and the stored objects of the rest in
// order. An inline part is never handed over.
func (s *Store) ReadFile(ctx context.Context, ref *proto.Ref, skip []uint32, fn func(*proto.ReadFileResponse) error) error {
	obj, err := s.Get(ctx, ref)
	if err != nil {
		return err
	}

	if obj.GetFile() == nil {
		return fmt.Errorf("%w: object %x is not a file", ErrInvalidRequest, ref.GetHash())
	}

	skipped := make(map[uint32]bool, len(skip))
	for _, i := range skip {
		skipped[i] = true
	}

	walk := newPartWalk(ctx, s.Index, obj.GetFile())
	walk.entered = []*proto.Ref{ref}

	next := func() ([]wantedPart, error) {
		var batch []wantedPart

		for len(batch) < readFileBatch {
			i, part, err := walk.next()
			if err != nil {
				return nil, err
			}

			if part == nil {
				break
			}

			if !skipped[uint32(i)] && part.Ref != nil {
				batch = append(batch, wantedPart{ref: part.Ref, index: i})
			}
		}

		if len(batch) == 0 {
			return nil, nil
		}

		for _, entered := range walk.entered {
			batch = append(batch, wantedPart{ref: entered, around: true})
		}

		walk.entered = nil

		return batch, nil
	}

	err = s.streamParts(ctx, next, func(p wantedPart) string {
		return fmt.Sprintf("part %d", p.index)
	}, func(batch []wantedPart, runs []*proto.LocatedRun) error {
		for _, run := range runs {
			for _, record := range run.Records {
				record.Index = uint32(batch[record.Index].index)
			}
		}

		return fn(&proto.ReadFileResponse{Runs: runs})
	}, func(p wantedPart, obj *proto.Object) error {
		return fn(&proto.ReadFileResponse{Index: uint32(p.index), Object: obj})
	})
	if err != nil {
		return fmt.Errorf("file %x: %w", ref.GetHash(), err)
	}

	return nil
}

// ReadFiles hands fn the object of each of files in order and then, unless
// objectsOnly, the parts of each except the indexes in its skip, a part
// several of them hold once: runs of those the index can locate, all
// located together, and the stored objects of the rest in order. An inline
// part is never handed over.
func (s *Store) ReadFiles(ctx context.Context, files []*proto.ReadFilesEntry, objectsOnly bool, fn func(*proto.ReadFilesResponse) error) error {
	if len(files) > backup.MaxFilesPerRead {
		return fmt.Errorf("%w: %d files in one read, at most %d", ErrInvalidRequest, len(files), backup.MaxFilesPerRead)
	}

	refs := make([]*proto.Ref, len(files))
	for i, file := range files {
		refs[i] = file.GetRef()
	}

	if err := s.readableAll(ctx, refs); err != nil {
		return err
	}

	objects := make([]*proto.Object, len(files))
	parts := make([][]wantedPart, len(files))
	entered := make([][]*proto.Ref, len(files))

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(readFileWorkers)

	read, errs := backup.ReadObjects(ctx, s.Index, refs)

	for i, ref := range refs {
		grp.Go(func() error {
			obj, err := offered(ref, read[i], errs[i])
			if err != nil {
				return err
			}

			if obj.GetFile() == nil {
				return fmt.Errorf("%w: object %x is not a file", ErrInvalidRequest, ref.GetHash())
			}

			objects[i] = obj

			if objectsOnly {
				return nil
			}

			parts[i], entered[i], err = firstParts(gctx, s.Index, obj.GetFile(), files[i].GetSkipParts())

			return err
		})
	}

	if err := grp.Wait(); err != nil {
		return err
	}

	for i, obj := range objects {
		if err := fn(&proto.ReadFilesResponse{File: uint32(i), Object: obj}); err != nil {
			return err
		}
	}

	if objectsOnly {
		return nil
	}

	var (
		wanted []wantedPart
		seen   = map[string]bool{}
	)

	for i := range files {
		for _, part := range parts[i] {
			if !seen[string(part.ref.Hash)] {
				seen[string(part.ref.Hash)] = true
				part.file = i
				wanted = append(wanted, part)
			}
		}
	}

	if len(wanted) > 0 {
		for i, ref := range refs {
			for _, around := range append([]*proto.Ref{ref}, entered[i]...) {
				wanted = append(wanted, wantedPart{ref: around, file: i, around: true})
			}
		}
	}

	return s.streamParts(ctx, func() ([]wantedPart, error) {
		batch := wanted
		wanted = nil

		return batch, nil
	}, func(p wantedPart) string {
		return fmt.Sprintf("part %d of file %x", p.index, refs[p.file].GetHash())
	}, func(batch []wantedPart, runs []*proto.LocatedRun) error {
		for _, run := range runs {
			for _, record := range run.Records {
				at := batch[record.Index]
				record.File, record.Index = uint32(at.file), uint32(at.index)
			}
		}

		return fn(&proto.ReadFilesResponse{Runs: runs})
	}, func(p wantedPart, obj *proto.Object) error {
		return fn(&proto.ReadFilesResponse{File: uint32(p.file), Part: true, Index: uint32(p.index), Object: obj})
	})
}

// firstParts lists, in order, each part of file that is not inline and
// not at an index in skip and whose ref no earlier part listed holds, and
// the sub-files it went into for them.
func firstParts(ctx context.Context, store backup.Getter, file *proto.File, skip []uint32) ([]wantedPart, []*proto.Ref, error) {
	skipped := make(map[uint32]bool, len(skip))
	for _, i := range skip {
		skipped[i] = true
	}

	walk := newPartWalk(ctx, store, file)
	walk.stepOver = true

	var (
		parts []wantedPart
		seen  = map[string]bool{}
	)

	for {
		i, part, err := walk.next()
		if err != nil {
			return nil, nil, err
		}

		if part == nil {
			return parts, walk.entered, nil
		}

		switch {
		case part.Ref == nil:
		case seen[string(part.Ref.Hash)]:
		case skipped[uint32(i)]:
			walk.miss()
		default:
			seen[string(part.Ref.Hash)] = true
			parts = append(parts, wantedPart{ref: part.Ref, index: i})
		}
	}
}

// wantedPart is a part to hand over: part index of file. An around part
// is not handed over but is the caller's to read, the file or a sub-file
// of it, so a located run may span its bytes.
type wantedPart struct {
	ref         *proto.Ref
	file, index int
	around      bool
}

// streamParts hands runs the runs of the parts the index can locate, each
// batch next returns at a time, their record indexes counting that batch,
// and object the stored objects of the rest in order, read readFileWorkers
// at a time, until next returns none. name says which part a failed read
// was.
func (s *Store) streamParts(ctx context.Context, next func() ([]wantedPart, error), name func(wantedPart) string,
	runs func([]wantedPart, []*proto.LocatedRun) error, object func(wantedPart, *proto.Object) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	locator, _ := s.Index.(backup.RecordLocator)

	type result struct {
		part  wantedPart
		obj   *proto.Object
		batch []wantedPart
		runs  []*proto.LocatedRun
		err   error
	}

	pending := make(chan chan result, readFileWorkers)

	push := func(done chan result) bool {
		select {
		case pending <- done:
			return true
		case <-ctx.Done():
			return false
		}
	}

	ready := func(res result) chan result {
		done := make(chan result, 1)
		done <- res

		return done
	}

	go func() {
		defer close(pending)

		for {
			batch, err := next()
			if err != nil {
				push(ready(result{err: err}))
				return
			}

			if len(batch) == 0 {
				return
			}

			located := make(map[int]bool)

			if locator != nil {
				refs := make([]*proto.Ref, len(batch))
				for i, part := range batch {
					refs[i] = part.ref
				}

				found, err := locator.LocateRecords(ctx, refs)
				if err != nil {
					push(ready(result{err: fmt.Errorf("locating parts: %w", err)}))
					return
				}

				for _, run := range found {
					run.Records = slices.DeleteFunc(run.Records, func(record *proto.LocatedRecord) bool {
						return int(record.Index) >= len(batch) || batch[record.Index].around
					})

					for _, record := range run.Records {
						located[int(record.Index)] = true
					}
				}

				if len(found) > 0 && !push(ready(result{batch: batch, runs: found})) {
					return
				}
			}

			for i, part := range batch {
				if located[i] || part.around {
					continue
				}

				done := make(chan result, 1)
				if !push(done) {
					return
				}

				go func() {
					obj, err := s.Index.Get(ctx, part.ref)
					if err != nil {
						err = fmt.Errorf("%s: %w", name(part), err)
					}

					done <- result{part: part, obj: obj, err: err}
				}()
			}
		}
	}()

	for done := range pending {
		res := <-done
		if res.err != nil {
			return res.err
		}

		var err error
		if res.runs != nil {
			err = runs(res.batch, res.runs)
		} else {
			err = object(res.part, res.obj)
		}

		if err != nil {
			return err
		}
	}

	return ctx.Err()
}

// readableAll is readable for many refs, asking the index once.
func (s *Store) readableAll(ctx context.Context, refs []*proto.Ref) error {
	for _, ref := range refs {
		if !ref.Valid() {
			return fmt.Errorf("%w: malformed ref", ErrInvalidRequest)
		}
	}

	scope, ok := s.Index.(backup.RefScope)
	if !ok {
		return nil
	}

	referenced, err := scope.ReferencesAll(ctx, refs)
	if err != nil {
		return err
	}

	for i, ok := range referenced {
		if !ok {
			return notFound(refs[i])
		}
	}

	return nil
}

// Retention is the index's retention state, backup.ErrNotImplemented for
// an index that keeps none.
func (s *Store) Retention() (backup.Retention, error) {
	ret, ok := s.Index.(backup.Retention)
	if !ok {
		return nil, fmt.Errorf("%w: index %T keeps no retention state", backup.ErrNotImplemented, s.Index)
	}

	return ret, nil
}

// Policies reads and sets the retention policies of a store's sets.
type Policies interface {
	// GetPolicy is the named set's retention.
	GetPolicy(ctx context.Context, set string) (index.SetRetention, error)
	// SetPolicy replaces the named set's own policy; nil leaves it to the
	// store's default.
	SetPolicy(ctx context.Context, set string, p *retention.Policy) error
}

// Policies is the index's set retention policies, backup.ErrNotImplemented
// for an index that keeps none.
func (s *Store) Policies() (Policies, error) {
	p, ok := s.Index.(Policies)
	if !ok {
		return nil, fmt.Errorf("%w: index %T keeps no retention policies", backup.ErrNotImplemented, s.Index)
	}

	return p, nil
}

// Escrow returns where the store keeps its escrowed key.
func (s *Store) Escrow() (backup.KeyEscrow, error) {
	escrow, ok := s.Index.(backup.KeyEscrow)
	if !ok {
		return nil, fmt.Errorf("%w: index %T keeps no escrowed key", backup.ErrNotImplemented, s.Index)
	}

	return escrow, nil
}

// ReadDir lists what a set held directly under dir at notAfter, sorted by
// name; backup.ErrNotImplemented for an index that cannot answer.
func (s *Store) ReadDir(ctx context.Context, set string, dir string, notAfter time.Time) ([]*proto.TreeNode, error) {
	lister, ok := s.Index.(backup.DirLister)
	if !ok {
		return nil, fmt.Errorf("%w: index %T lists no directories", backup.ErrNotImplemented, s.Index)
	}

	return lister.ReadDir(ctx, set, dir, notAfter)
}

// presenceScope is the store policy's scope; a store without a policy
// has the default policy's.
func (s *Store) presenceScope(ctx context.Context) (backup.PresenceScope, error) {
	policy := storekey.DefaultPolicy()

	if source, ok := s.Index.(backup.PolicySource); ok {
		stored, err := source.StorePolicy(ctx)
		if err != nil {
			return backup.PresenceOff, err
		}

		if stored != nil {
			policy = *stored
		}
	}

	return backup.ParsePresenceScope(policy.PresenceScope)
}

// Presence returns the head filters of the caller's scope for set: nil
// when the index keeps none or the scope is off.
func (s *Store) Presence(ctx context.Context, set string) ([]*proto.PresenceFilter, error) {
	index, ok := s.Index.(backup.PresenceIndex)
	if !ok {
		return nil, nil
	}

	scope, err := s.presenceScope(ctx)
	if err != nil {
		return nil, err
	}

	if scope == backup.PresenceOff {
		return nil, nil
	}

	return index.Presence(ctx, scope, set)
}

// SessionLookup resolves a session id presented by a caller.
type SessionLookup interface {
	LookupSession(ctx context.Context, id string) (*backup.Session, error)
}

// BeginSession opens a session on set for the caller; restore names the
// commit a restore session reads, nil for a backup. initial, when set, is
// the retention policy set starts with if the session creates it; an
// invalid one is ErrInvalidRequest.
func (s *Store) BeginSession(ctx context.Context, set string, restore *proto.Ref, initial *retention.Policy) (*backup.Session, error) {
	if s.Sessions == nil {
		return nil, fmt.Errorf("%w: this store has no sessions", backup.ErrNotImplemented)
	}

	p, err := auth.Require(ctx)
	if err != nil {
		return nil, err
	}

	if restore != nil {
		if err := s.readable(ctx, restore); err != nil {
			return nil, err
		}
	}

	if initial != nil {
		if err := initial.Validate(); err != nil {
			return nil, fmt.Errorf("%w: initial retention: %v", ErrInvalidRequest, err)
		}
	}

	session := &backup.Session{
		AgentID:          p.AgentID,
		Set:              set,
		Restore:          restore,
		InitialRetention: initial,
	}

	_, err = s.Sessions.BeginSession(ctx, session)
	if err != nil {
		return nil, err
	}

	return session, nil
}

// EndSession drops what the caller's session id has not committed.
func (s *Store) EndSession(ctx context.Context, id string) error {
	session, err := s.Session(ctx, id)
	if err != nil {
		return err
	}

	return s.Sessions.EndSession(backup.WithSession(ctx, session))
}

// TouchSession renews the lease of the caller's session id.
func (s *Store) TouchSession(ctx context.Context, id string) error {
	session, err := s.Session(ctx, id)
	if err != nil {
		return err
	}

	toucher, ok := s.Sessions.(backup.SessionToucher)
	if !ok {
		return fmt.Errorf("%w: this store's sessions have no lease", backup.ErrNotImplemented)
	}

	return toucher.TouchSession(backup.WithSession(ctx, session))
}

// Session returns the live session behind id if it belongs to the caller;
// an unknown id is backup.ErrNotFound, another caller's auth.ErrForbidden.
func (s *Store) Session(ctx context.Context, id string) (*backup.Session, error) {
	if s.Sessions == nil {
		return nil, fmt.Errorf("%w: this store has no sessions", backup.ErrNotImplemented)
	}

	lookup, ok := s.Sessions.(SessionLookup)
	if !ok {
		return nil, fmt.Errorf("%w: this store cannot resolve sessions", backup.ErrNotImplemented)
	}

	p, err := auth.Require(ctx)
	if err != nil {
		return nil, err
	}

	session, err := lookup.LookupSession(ctx, id)
	if errors.Is(err, backup.ErrNoSession) {
		return nil, fmt.Errorf("session %s: %w", id, backup.ErrNotFound)
	}

	if err != nil {
		return nil, err
	}

	if session.AgentID != p.AgentID {
		return nil, fmt.Errorf("%w: session %s belongs to another caller", auth.ErrForbidden, id)
	}

	return session, nil
}

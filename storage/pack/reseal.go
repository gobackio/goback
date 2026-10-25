package pack

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"golang.org/x/sync/errgroup"
	pb "google.golang.org/protobuf/proto"
)

const (
	// resealReads bounds the store calls a reseal makes at once.
	resealReads = 32
	// resealFanout bounds the entries of one tree or file resealed at once.
	resealFanout = 16
	// resealMemo bounds the trees and files a reseal remembers the new
	// refs of before it starts over.
	resealMemo = 1 << 20
)

// KeyOpener returns the store key whose id is keyID.
type KeyOpener func(ctx context.Context, keyID []byte) (*storekey.Key, error)

// ResealReport is what one Reseal did.
type ResealReport struct {
	// Busy is set when another process maintains the store; a reseal that
	// finds it busy does nothing else.
	Busy bool
	// Resealed pairs each commit replaced with its replacement, in the
	// order they were replaced.
	Resealed []ResealedCommit
	// Current counts the commits that held no PRF-named blob.
	Current int
	// Blobs counts the blobs sealed again, and Pins the pins moved to what
	// replaced their target.
	Blobs int
	Pins  int
	// Failed names the commits and pins left as they were, with why.
	Failed []ResealFailure
}

// ResealedCommit is a commit Reseal replaced and its replacement.
type ResealedCommit struct {
	Old, New *proto.Ref
}

// ResealFailure is a commit or pin Reseal left as it was, and why.
type ResealFailure struct {
	Ref *proto.Ref
	Err error
}

// Summary renders the report as a line for a log or an operator.
func (r *ResealReport) Summary() string {
	if r.Busy {
		return "Reseal left to another process maintaining the store"
	}

	return fmt.Sprintf("Resealed %d commits and %d blobs, moved %d pins, found %d commits current, %d failed",
		len(r.Resealed), r.Blobs, r.Pins, r.Current, len(r.Failed))
}

// Reseal rewrites every PRF-named blob under the ref of its sealed bytes,
// opened with the key keys returns for its key id, and replaces each
// commit above one with a commit of the same time, set and metadata that
// takes over its pins, trash state and head, tombstoning the old one for a
// collection to drop. A commit it cannot reseal is reported and left as it
// was; a stopped run carries on when run again. Rebuild the index from the
// store afterwards, into an empty database. It fails while a published plan
// waits for its rewrite, and while another process maintains the store it
// does nothing and sets Busy.
func (ps *PackStorage) Reseal(ctx context.Context, keys KeyOpener) (*ResealReport, error) {
	if _, ok := backup.SessionFromContext(ctx); ok {
		return nil, errors.New("a reseal runs outside a session")
	}

	ps.maintenanceMtx.Lock()
	defer ps.maintenanceMtx.Unlock()

	lease, ctx, err := ps.leaseMaintenance(ctx)
	if err != nil {
		return nil, fmt.Errorf("leasing the maintenance: %w", err)
	}

	if lease == nil {
		return &ResealReport{Busy: true}, nil
	}
	defer lease.release()

	if err := ps.refreshArchives(); err != nil {
		return nil, fmt.Errorf("refreshing the archives: %w", err)
	}

	if err := ps.noPendingPlan(); err != nil {
		return nil, err
	}

	rs := &resealer{
		ps:       ps,
		keys:     keys,
		store:    resealStore{PackStorage: ps, reads: make(chan struct{}, resealReads)},
		opened:   map[string]*storekey.Key{},
		memo:     map[string]*resealed{},
		replaced: map[string]*proto.Ref{},
		report:   &ResealReport{},
	}

	err = rs.run(ctx)
	rs.report.Blobs = int(rs.blobs.Load())

	if flushErr := ps.Flush(); err == nil {
		err = flushErr
	}

	return rs.report, err
}

// resealStore is the store as a reseal reads and writes it: a bounded
// number of calls at once, no object written twice, and a commit stored
// as given.
type resealStore struct {
	*PackStorage
	reads chan struct{}
}

// Get implements backup.ObjectStore.
func (s resealStore) Get(ctx context.Context, ref *proto.Ref) (*proto.Object, error) {
	s.reads <- struct{}{}
	defer func() { <-s.reads }()

	return s.PackStorage.Get(ctx, ref)
}

// Put implements backup.ObjectStore.
func (s resealStore) Put(ctx context.Context, object *proto.Object) error {
	_, err := s.putAbsent(ctx, object)
	return err
}

// putAbsent stores object unless the store holds it, and reports whether
// it did.
func (s resealStore) putAbsent(ctx context.Context, object *proto.Object) (bool, error) {
	s.reads <- struct{}{}
	defer func() { <-s.reads }()

	has, err := s.Has(ctx, object.Ref())
	if err != nil || has {
		return false, err
	}

	return true, s.putWriteCache(ctx, object, s.put(ctx, object))
}

// resealed is what rewriting one tree or file came to, once done closes.
type resealed struct {
	done chan struct{}
	ref  *proto.Ref
	err  error
}

type resealer struct {
	ps    *PackStorage
	keys  KeyOpener
	store resealStore

	keysMtx sync.Mutex
	opened  map[string]*storekey.Key

	memoMtx sync.Mutex
	memo    map[string]*resealed

	// all holds every commit the archives hold, live or not, live the refs
	// of the live ones and received those by receipt; replaced maps each
	// live commit handled so far to the commit that stands for it now
	all      map[string]*proto.Object
	live     map[string]bool
	received map[receipt][]string
	replaced map[string]*proto.Ref

	pins     map[string][]*proto.Object
	policies map[string][]*proto.Policy

	blobs  atomic.Int64
	report *ResealReport
}

func (rs *resealer) run(ctx context.Context) error {
	commits, err := rs.load(ctx)
	if err != nil {
		return err
	}

	for _, ref := range commits {
		if err := rs.commit(ctx, ref); err != nil {
			if ctx.Err() != nil {
				return err
			}

			rs.report.Failed = append(rs.report.Failed, ResealFailure{Ref: ref, Err: err})
		}
	}

	for target, pins := range rs.pins {
		if rs.all[target] != nil {
			continue
		}

		for _, pin := range pins {
			if err := rs.pin(ctx, pin); err != nil {
				if ctx.Err() != nil {
					return err
				}

				rs.report.Failed = append(rs.report.Failed, ResealFailure{Ref: pin.Ref(), Err: err})
			}
		}
	}

	return nil
}

// load reads the store's commits, live pins and commit policies, and
// returns the live commits in the order they were received.
func (rs *resealer) load(ctx context.Context) ([]*proto.Ref, error) {
	rs.all = map[string]*proto.Object{}

	err := rs.ps.Walk(ctx, true, proto.ObjectType_COMMIT, func(obj *proto.Object) error {
		rs.all[string(obj.Ref().Hash)] = obj
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing the commits: %w", err)
	}

	refs := make([]*proto.Ref, 0, len(rs.all))
	for hash := range rs.all {
		refs = append(refs, &proto.Ref{Hash: []byte(hash)})
	}

	held, err := rs.held(ctx, refs)
	if err != nil {
		return nil, err
	}

	rs.live = map[string]bool{}
	rs.received = map[receipt][]string{}

	var live []*proto.Ref
	for i, ref := range refs {
		if held[i] {
			rs.live[string(ref.Hash)] = true
			at := receiptOf(rs.all[string(ref.Hash)])
			rs.received[at] = append(rs.received[at], string(ref.Hash))
			live = append(live, ref)
		}
	}

	slices.SortFunc(live, func(a, b *proto.Ref) int {
		x, y := rs.all[string(a.Hash)].GetCommit(), rs.all[string(b.Hash)].GetCommit()

		return cmp.Or(cmp.Compare(x.ReceivedAtNs, y.ReceivedAtNs), cmp.Compare(x.SetId, y.SetId), bytes.Compare(a.Hash, b.Hash))
	})

	if rs.pins, err = rs.livePins(ctx); err != nil {
		return nil, err
	}

	rs.policies = map[string][]*proto.Policy{}
	seen := map[string]bool{}

	err = rs.ps.Walk(ctx, true, proto.ObjectType_POLICY, func(obj *proto.Object) error {
		scope := obj.GetPolicy().GetCommit()
		if scope == nil || seen[string(obj.Ref().Hash)] {
			return nil
		}

		seen[string(obj.Ref().Hash)] = true
		rs.policies[string(scope.GetCommit().GetHash())] = append(rs.policies[string(scope.GetCommit().GetHash())], obj.GetPolicy())

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing the policies: %w", err)
	}

	return live, nil
}

// held is HasAll in batches.
func (rs *resealer) held(ctx context.Context, refs []*proto.Ref) ([]bool, error) {
	out := make([]bool, 0, len(refs))

	for start := 0; start < len(refs); start += reviveBatch {
		has, err := rs.ps.HasAll(ctx, refs[start:min(start+reviveBatch, len(refs))])
		if err != nil {
			return nil, err
		}

		out = append(out, has...)
	}

	return out, nil
}

// livePins returns the live pins by the hash of their target.
func (rs *resealer) livePins(ctx context.Context) (map[string][]*proto.Object, error) {
	all := map[string]*proto.Object{}

	err := rs.ps.Walk(ctx, true, proto.ObjectType_PIN, func(obj *proto.Object) error {
		all[string(obj.Ref().Hash)] = obj
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("listing the pins: %w", err)
	}

	refs := make([]*proto.Ref, 0, len(all))
	for hash := range all {
		refs = append(refs, &proto.Ref{Hash: []byte(hash)})
	}

	held, err := rs.held(ctx, refs)
	if err != nil {
		return nil, err
	}

	pins := map[string][]*proto.Object{}
	for i, ref := range refs {
		if held[i] {
			pin := all[string(ref.Hash)]
			target := string(pin.GetPin().GetTarget().GetHash())
			pins[target] = append(pins[target], pin)
		}
	}

	return pins, nil
}

// commit replaces the live commit ref with one over resealed blobs, unless
// it holds none to reseal.
func (rs *resealer) commit(ctx context.Context, ref *proto.Ref) error {
	obj := rs.all[string(ref.Hash)]
	commit := obj.GetCommit()

	if commit.KeyId != nil && len(commit.KeyId) == 0 {
		rs.replaced[string(ref.Hash)] = ref
		return nil
	}

	tree, err := rs.rewrite(ctx, commit.Tree)
	if err != nil {
		return fmt.Errorf("commit %x: %w", ref.Hash, err)
	}

	parent := rs.standIn(commit.Parent)

	if tree.Equal(commit.Tree) && parent.Equal(commit.Parent) {
		rs.replaced[string(ref.Hash)] = ref
		rs.report.Current++

		return nil
	}

	next := pb.Clone(commit).(*proto.Commit)
	next.Tree, next.Parent = tree, parent

	replacement := proto.NewObject(next)
	replacement.KeyId = obj.KeyId

	if err := rs.store.Put(ctx, replacement); err != nil {
		return fmt.Errorf("commit %x: %w", ref.Hash, err)
	}

	if err := rs.replace(ctx, ref, replacement); err != nil {
		return fmt.Errorf("commit %x: %w", ref.Hash, err)
	}

	rs.replaced[string(ref.Hash)] = replacement.Ref()
	rs.report.Resealed = append(rs.report.Resealed, ResealedCommit{Old: ref, New: replacement.Ref()})

	return nil
}

// standIn is the commit that stands for parent now: its replacement, or
// for a parent an earlier run replaced, the live commit received with it
// in its set; parent itself where there is neither.
func (rs *resealer) standIn(parent *proto.Ref) *proto.Ref {
	if parent == nil {
		return nil
	}

	if ref, ok := rs.replaced[string(parent.Hash)]; ok {
		return ref
	}

	old := rs.all[string(parent.Hash)]
	if old == nil || rs.live[string(parent.Hash)] {
		return parent
	}

	for _, twin := range rs.received[receiptOf(old)] {
		if ref, ok := rs.replaced[twin]; ok {
			return ref
		}
	}

	return parent
}

// receipt is when a commit was received in its set, which no two commits
// of a set share but a replacement and the commit it replaced.
type receipt struct {
	set uint64
	at  int64
}

func receiptOf(obj *proto.Object) receipt {
	return receipt{set: obj.GetCommit().GetSetId(), at: obj.GetCommit().GetReceivedAtNs()}
}

// replace moves the pins, trash state and head of the commit old to
// replacement, which the store holds, then tombstones old and its pins.
func (rs *resealer) replace(ctx context.Context, old *proto.Ref, replacement *proto.Object) error {
	pins := rs.pins[string(old.Hash)]

	for _, pin := range pins {
		moved := pb.Clone(pin.GetPin()).(*proto.Pin)
		moved.Target = replacement.Ref()

		if err := rs.store.Put(ctx, proto.NewObject(moved)); err != nil {
			return fmt.Errorf("moving pin %x: %w", pin.Ref().Hash, err)
		}
	}

	for _, policy := range rs.policies[string(old.Hash)] {
		moved := pb.Clone(policy).(*proto.Policy)
		moved.GetCommit().Commit = replacement.Ref()

		if err := rs.store.Put(ctx, proto.NewObject(moved)); err != nil {
			return fmt.Errorf("moving policy %d: %w", policy.Sequence, err)
		}
	}

	// the tombstones go out only once what takes over is durable
	if err := rs.ps.Flush(); err != nil {
		return err
	}

	if err := rs.ps.moveHead(old, replacement); err != nil {
		return fmt.Errorf("moving the head: %w", err)
	}

	for _, pin := range pins {
		if err := rs.ps.Delete(ctx, pin.Ref()); err != nil {
			return err
		}
	}

	rs.report.Pins += len(pins)

	return rs.ps.Delete(ctx, old)
}

// pin moves a live pin of a tree or file to what that is resealed as.
func (rs *resealer) pin(ctx context.Context, pin *proto.Object) error {
	target, err := rs.rewrite(ctx, pin.GetPin().GetTarget())
	if err != nil || target.Equal(pin.GetPin().GetTarget()) {
		return err
	}

	moved := pb.Clone(pin.GetPin()).(*proto.Pin)
	moved.Target = target

	if err := rs.store.Put(ctx, proto.NewObject(moved)); err != nil {
		return err
	}

	if err := rs.ps.Flush(); err != nil {
		return err
	}

	rs.report.Pins++

	return rs.ps.Delete(ctx, pin.Ref())
}

// rewrite returns the ref the tree, file or blob ref names is resealed
// under, writing what it has to. A tree or file is rewritten once, and a
// blob once at a time.
func (rs *resealer) rewrite(ctx context.Context, ref *proto.Ref) (*proto.Ref, error) {
	key := string(ref.Hash)

	rs.memoMtx.Lock()
	if entry, ok := rs.memo[key]; ok {
		rs.memoMtx.Unlock()
		<-entry.done

		return entry.ref, entry.err
	}

	if len(rs.memo) >= resealMemo {
		rs.memo = map[string]*resealed{}
	}

	entry := &resealed{done: make(chan struct{})}
	rs.memo[key] = entry
	rs.memoMtx.Unlock()

	var blob bool
	entry.ref, blob, entry.err = rs.rewriteObject(ctx, ref)

	// blobs far outnumber trees and files, and are rarely met twice; an
	// error may be another walk's cancellation
	if blob || entry.err != nil {
		rs.memoMtx.Lock()
		if rs.memo[key] == entry {
			delete(rs.memo, key)
		}
		rs.memoMtx.Unlock()
	}

	close(entry.done)

	return entry.ref, entry.err
}

// rewriteObject is rewrite of the object ref names, which it reports
// whether is a blob.
func (rs *resealer) rewriteObject(ctx context.Context, ref *proto.Ref) (*proto.Ref, bool, error) {
	obj, err := rs.store.Get(ctx, ref)
	if err != nil {
		return nil, false, fmt.Errorf("%x: %w", ref.Hash, err)
	}

	switch {
	case obj.GetSealed() != nil:
		next, err := rs.blob(ctx, ref, obj.GetSealed())
		return next, true, err
	case obj.GetBlob() != nil:
		return ref, true, nil
	case obj.GetTree() != nil:
		next, err := rs.tree(ctx, ref, obj)
		return next, false, err
	case obj.GetFile() != nil:
		next, err := rs.file(ctx, ref, obj)
		return next, false, err
	}

	return nil, false, fmt.Errorf("%x is a %s, which no tree holds", ref.Hash, obj.Type())
}

// tree rewrites a tree over what its nodes, their metadata and its splits
// are resealed as.
func (rs *resealer) tree(ctx context.Context, ref *proto.Ref, obj *proto.Object) (*proto.Ref, error) {
	tree := pb.Clone(obj.GetTree()).(*proto.Tree)

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(resealFanout)

	var changed atomic.Bool

	remap := func(at **proto.Ref) {
		grp.Go(func() error {
			next, err := rs.rewrite(gctx, *at)
			if err == nil && !next.Equal(*at) {
				*at = next
				changed.Store(true)
			}

			return err
		})
	}

	for _, node := range tree.Nodes {
		if node.Ref != nil {
			remap(&node.Ref)
		}

		if node.Stat.GetSecurity() != nil {
			remap(&node.Stat.Security)
		}

		for _, stream := range node.Stat.GetStreams() {
			remap(&stream.Ref)
		}

		for _, xattr := range node.Stat.GetXattrs() {
			remap(&xattr.Ref)
		}
	}

	for i := range tree.Splits {
		remap(&tree.Splits[i])
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	if !changed.Load() {
		return ref, nil
	}

	next := proto.NewObject(tree)
	next.KeyId = obj.KeyId

	if err := rs.store.Put(ctx, next); err != nil {
		return nil, err
	}

	return next.Ref(), nil
}

// file rewrites a file over its parts resealed, cut as a backup cuts
// them. A file's parts are all written one way, so the first part says
// whether there is anything to do.
func (rs *resealer) file(ctx context.Context, ref *proto.Ref, obj *proto.Object) (*proto.Ref, error) {
	parts, err := backup.FileParts(ctx, rs.store, obj.GetFile())
	if err != nil {
		return nil, err
	}

	first := slices.IndexFunc(parts, func(part *proto.FilePart) bool { return part.Ref != nil })
	if first < 0 {
		return ref, nil
	}

	probe, err := rs.store.Get(ctx, parts[first].Ref)
	if err != nil {
		return nil, fmt.Errorf("part %x: %w", parts[first].Ref.Hash, err)
	}

	sealed := probe.GetSealed()
	if sealed == nil || sealed.Ref().Equal(parts[first].Ref) {
		return ref, nil
	}

	key, err := rs.key(ctx, sealed.KeyId)
	if err != nil {
		return nil, err
	}

	next := make([]*proto.FilePart, len(parts))

	grp, gctx := errgroup.WithContext(ctx)
	grp.SetLimit(resealFanout)

	for i, part := range parts {
		next[i] = pb.Clone(part).(*proto.FilePart)
		if part.Ref == nil {
			continue
		}

		grp.Go(func() error {
			resealed, err := rs.rewrite(gctx, part.Ref)
			next[i].Ref = resealed

			return err
		})
	}

	if err := grp.Wait(); err != nil {
		return nil, err
	}

	return backup.PutParts(ctx, rs.store, key, next)
}

// blob seals a PRF-named blob again under the ref of its sealed bytes,
// once it opened it bound to ref and found its plaintext names ref.
func (rs *resealer) blob(ctx context.Context, ref *proto.Ref, sealed *proto.Sealed) (*proto.Ref, error) {
	if sealed.Ref().Equal(ref) {
		return ref, nil
	}

	key, err := rs.key(ctx, sealed.KeyId)
	if err != nil {
		return nil, err
	}

	stored, err := key.OpenPRFBlob(sealed.Data, ref.Hash)
	if err != nil {
		return nil, fmt.Errorf("blob %x: %w", ref.Hash, err)
	}

	chunk, err := proto.Decode(stored, sealed.Compression)
	if err != nil {
		return nil, fmt.Errorf("blob %x: %w", ref.Hash, err)
	}

	if !proto.HashPayload(proto.ObjectType_BLOB, key.Digest(chunk)).Equal(ref) {
		return nil, fmt.Errorf("%w: blob %x does not hash to its ref", storekey.ErrWrongKey, ref.Hash)
	}

	next := proto.NewObject(backup.SealBlob(key, chunk))

	wrote, err := rs.store.putAbsent(ctx, next)
	if wrote {
		rs.blobs.Add(1)
	}

	return next.Ref(), err
}

// key opens the key of keyID once per reseal.
func (rs *resealer) key(ctx context.Context, keyID []byte) (*storekey.Key, error) {
	rs.keysMtx.Lock()
	defer rs.keysMtx.Unlock()

	if key, ok := rs.opened[string(keyID)]; ok {
		return key, nil
	}

	key, err := rs.keys(ctx, keyID)
	if err != nil {
		return nil, fmt.Errorf("opening key %x: %w", keyID, err)
	}

	if !bytes.Equal(key.ID(), keyID) {
		return nil, fmt.Errorf("opening key %x gave key %x", keyID, key.ID())
	}

	rs.opened[string(keyID)] = key

	return key, nil
}

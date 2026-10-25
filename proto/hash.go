package proto

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	pb "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// HashSize is the width in bytes of every Ref.
const HashSize = sha256.Size

var (
	// ErrRefMismatch is returned when stored bytes do not hash to the ref
	// they are filed under.
	ErrRefMismatch = errors.New("object bytes do not match ref")

	// ErrInvalidObject is wrapped by every error from canonical encoding.
	ErrInvalidObject = errors.New("invalid object")
)

func invalid(format string, args ...interface{}) error {
	return fmt.Errorf("%w: %s", ErrInvalidObject, fmt.Sprintf(format, args...))
}

// typeTag returns the domain-separation tag hashed in front of a payload.
func typeTag(t ObjectType) (string, bool) {
	switch t {
	case ObjectType_COMMIT:
		return "commit", true
	case ObjectType_TREE:
		return "tree", true
	case ObjectType_FILE:
		return "file", true
	case ObjectType_BLOB:
		return "blob", true
	case ObjectType_TOMBSTONE:
		return "tombstone", true
	case ObjectType_PIN:
		return "pin", true
	case ObjectType_POLICY:
		return "policy", true
	default:
		return "", false
	}
}

// HashPayload computes the ref of a payload of the given type as
// SHA-256(tag || " " || decimal(len(payload)) || 0x00 || payload).
func HashPayload(t ObjectType, payload []byte) *Ref {
	tag, ok := typeTag(t)
	if !ok {
		panic(fmt.Sprintf("no hash tag for object type %s", t))
	}

	return hashTagged(tag, payload)
}

// sealedTag separates the refs of sealed blobs from those of every object
// type, so no sealed ref equals a plaintext one.
const sealedTag = "sealed blob"

// Ref is the ref a sealed blob is stored under: HashPayload's form under
// its own tag over uvarint(len(key_id)) || key_id || uvarint(compression)
// || data.
func (s *Sealed) Ref() *Ref {
	prefix := protowire.AppendVarint(nil, uint64(len(s.GetKeyId())))
	prefix = append(prefix, s.GetKeyId()...)
	prefix = protowire.AppendVarint(prefix, uint64(s.GetCompression()))

	return hashTagged(sealedTag, prefix, s.GetData())
}

// hashTagged is SHA-256(tag || " " || decimal(len(payload)) || 0x00 ||
// payload) over the concatenation of parts.
func hashTagged(tag string, parts ...[]byte) *Ref {
	size := 0
	for _, part := range parts {
		size += len(part)
	}

	h := sha256.New()
	h.Write([]byte(tag))
	h.Write([]byte{' '})
	h.Write([]byte(strconv.Itoa(size)))
	h.Write([]byte{0})

	for _, part := range parts {
		h.Write(part)
	}

	return &Ref{Hash: h.Sum(nil)}
}

// TombstoneRef is the ref under which a tombstone for target is stored.
func TombstoneRef(target *Ref) *Ref {
	return HashPayload(ObjectType_TOMBSTONE, target.GetHash())
}

// Equal reports whether two refs name the same object.
func (r *Ref) Equal(other *Ref) bool {
	return hmac.Equal(r.GetHash(), other.GetHash())
}

// Valid reports whether the ref has the expected width.
func (r *Ref) Valid() bool {
	return len(r.GetHash()) == HashSize
}

// Ref computes the object's ref. It panics on an object that cannot be
// canonically encoded; use Canonical to get the error instead.
func (o *Object) Ref() *Ref {
	if sealed := o.GetSealed(); sealed != nil {
		return sealed.Ref()
	}

	payload, err := o.Canonical()
	if err != nil {
		panic(err)
	}

	return HashPayload(o.Type(), payload)
}

// Verify fails with ErrRefMismatch, naming ref, unless the object is the one
// ref names.
func (o *Object) Verify(ref *Ref) error {
	payload, err := o.Canonical()
	if err != nil {
		return fmt.Errorf("object %x: %w", ref.GetHash(), err)
	}

	got := HashPayload(o.Type(), payload)
	if sealed := o.GetSealed(); sealed != nil {
		got = sealed.Ref()
	}

	if !got.Equal(ref) {
		return fmt.Errorf("%w: object %x hashes to %x", ErrRefMismatch, ref.GetHash(), got.GetHash())
	}

	return nil
}

// StoredHash is the hash every object header carries over its stored bytes.
func StoredHash(stored []byte) []byte {
	sum := sha256.Sum256(stored)
	return sum[:]
}

// VerifyStored checks stored bytes against their header: the stored hash
// when present, and the ref of everything but a sealed blob, which its
// stored hash alone checks, so that a PRF-named blob a store has yet to
// reseal still reads back.
func VerifyStored(hdr *ObjectHeader, stored []byte) error {
	if len(hdr.StoredHash) > 0 && !hmac.Equal(StoredHash(stored), hdr.StoredHash) {
		return ErrRefMismatch
	}

	if hdr.Type == ObjectType_TOMBSTONE {
		if !TombstoneRef(hdr.TombstoneFor).Equal(hdr.Ref) {
			return ErrRefMismatch
		}

		return nil
	}

	if hdr.Encryption != Encryption_PLAINTEXT {
		if len(hdr.StoredHash) == 0 {
			return invalid("sealed object %x without a stored hash", hdr.Ref.GetHash())
		}

		return nil
	}

	_, err := VerifyPayload(stored, hdr.Compression, hdr.Type, hdr.Ref)

	return err
}

// ObjectFromStored decodes stored bytes after VerifyStored. A sealed object
// comes back as is, for the holder of the key to open.
func ObjectFromStored(hdr *ObjectHeader, stored []byte) (*Object, error) {
	err := VerifyStored(hdr, stored)
	if err != nil {
		return nil, err
	}

	if hdr.Encryption != Encryption_PLAINTEXT {
		return &Object{Object: &Object_Sealed{Sealed: &Sealed{
			Type:        hdr.Type,
			Data:        stored,
			Compression: hdr.Compression,
			Encryption:  hdr.Encryption,
			KeyId:       hdr.KeyId,
		}}, KeyId: hdr.KeyId}, nil
	}

	obj, err := NewVerifiedObject(stored, hdr.Compression, hdr.Type, hdr.Ref)
	if err != nil {
		return nil, err
	}

	obj.KeyId = hdr.KeyId

	return obj, nil
}

// HeaderFor builds the archive header of an object and returns the bytes to
// store: sealed data as is, anything else compressed.
func HeaderFor(o *Object) (*ObjectHeader, []byte, error) {
	payload, err := o.Canonical()
	if err != nil {
		return nil, nil, err
	}

	if sealed := o.GetSealed(); sealed != nil {
		return &ObjectHeader{
			Ref:         sealed.Ref(),
			Type:        sealed.Type,
			Compression: sealed.Compression,
			Encryption:  sealed.Encryption,
			KeyId:       sealed.KeyId,
			StoredHash:  StoredHash(sealed.Data),
		}, sealed.Data, nil
	}

	stored, compression := Encode(payload)

	hdr := &ObjectHeader{
		Ref:         HashPayload(o.Type(), payload),
		Type:        o.Type(),
		Compression: compression,
		KeyId:       o.KeyId,
		StoredHash:  StoredHash(stored),
	}

	// a commit or pin entered the system when the server received it, and
	// a rebuild must reproduce that
	if ns := o.ReceivedAtNs(); ns != 0 {
		hdr.Timestamp = timestamppb.New(time.Unix(0, ns))
	}

	return hdr, stored, nil
}

// ReceivedAtNs is the server receipt time of a commit or pin, 0 for every
// other object and for one the server has not stamped yet.
func (o *Object) ReceivedAtNs() int64 {
	switch t := o.GetObject().(type) {
	case *Object_Commit:
		return t.Commit.GetReceivedAtNs()
	case *Object_Pin:
		return t.Pin.GetReceivedAtNs()
	}

	return 0
}

// Stamp records the server receipt time on a commit or pin, and the set id
// on a commit. It does nothing for other objects.
func (o *Object) Stamp(setID uint64, receivedAt time.Time) {
	switch t := o.GetObject().(type) {
	case *Object_Commit:
		t.Commit.SetId = setID
		t.Commit.ReceivedAtNs = receivedAt.UnixNano()
	case *Object_Pin:
		t.Pin.ReceivedAtNs = receivedAt.UnixNano()
	}
}

// Validate reports whether the object can be canonically encoded.
func (o *Object) Validate() error {
	_, err := o.Canonical()
	return err
}

// Canonical returns the payload that is hashed and stored for the object:
// the raw chunk for a blob, and the canonical protowire encoding of the
// inner message for everything else.
func (o *Object) Canonical() ([]byte, error) {
	if err := noUnknown(o); err != nil {
		return nil, err
	}

	switch t := o.GetObject().(type) {
	case *Object_Blob:
		if err := noUnknown(t.Blob); err != nil {
			return nil, err
		}
		return t.Blob.GetData(), nil
	case *Object_Sealed:
		if err := noUnknown(t.Sealed); err != nil {
			return nil, err
		}
		if len(t.Sealed.KeyId) == 0 {
			return nil, invalid("sealed object without a key id")
		}
		if t.Sealed.Encryption == Encryption_PLAINTEXT {
			return nil, invalid("sealed object without an encryption mode")
		}
		if t.Sealed.Type != ObjectType_BLOB {
			return nil, invalid("sealed %s object: only a blob is sealed", t.Sealed.Type)
		}
		return t.Sealed.GetData(), nil
	case *Object_Commit:
		return canonicalCommit(t.Commit)
	case *Object_Tree:
		return canonicalTree(t.Tree)
	case *Object_File:
		return canonicalFile(t.File)
	case *Object_Pin:
		return canonicalPin(t.Pin)
	case *Object_Policy:
		return canonicalPolicy(t.Policy)
	default:
		return nil, invalid("empty object")
	}
}

// NewObjectFromPayload decodes a stored payload of the given type.
func NewObjectFromPayload(payload []byte, t ObjectType) (*Object, error) {
	switch t {
	case ObjectType_BLOB:
		return NewObject(&Blob{Data: payload}), nil
	case ObjectType_COMMIT:
		c := new(Commit)
		return NewObject(c), pb.Unmarshal(payload, c)
	case ObjectType_TREE:
		tr := new(Tree)
		return NewObject(tr), pb.Unmarshal(payload, tr)
	case ObjectType_FILE:
		f := new(File)
		return NewObject(f), pb.Unmarshal(payload, f)
	case ObjectType_PIN:
		p := new(Pin)
		return NewObject(p), pb.Unmarshal(payload, p)
	case ObjectType_POLICY:
		p := new(Policy)
		return NewObject(p), pb.Unmarshal(payload, p)
	default:
		return nil, invalid("cannot decode object type %s", t)
	}
}

// VerifyPayload decompresses stored bytes and fails with ErrRefMismatch
// unless they hash to ref under type t. It returns the decompressed payload.
func VerifyPayload(stored []byte, compression Compression, t ObjectType, ref *Ref) ([]byte, error) {
	payload, err := decode(stored, compression)
	if err != nil {
		return nil, err
	}

	if !HashPayload(t, payload).Equal(ref) {
		return nil, ErrRefMismatch
	}

	return payload, nil
}

// NewVerifiedObject decodes stored bytes after checking them against ref.
func NewVerifiedObject(stored []byte, compression Compression, t ObjectType, ref *Ref) (*Object, error) {
	payload, err := VerifyPayload(stored, compression, t, ref)
	if err != nil {
		return nil, err
	}

	return NewObjectFromPayload(payload, t)
}

func decode(stored []byte, compression Compression) ([]byte, error) {
	switch compression {
	case Compression_NONE:
		return stored, nil
	case Compression_ZSTD:
		return decodeZstd(stored)
	default:
		return nil, fmt.Errorf("unsupported compression %s", compression)
	}
}

func noUnknown(m pb.Message) error {
	if len(m.ProtoReflect().GetUnknown()) > 0 {
		return invalid("%s carries unknown fields", m.ProtoReflect().Descriptor().FullName())
	}

	return nil
}

func appendVarint(b []byte, num protowire.Number, v uint64) []byte {
	if v == 0 {
		return b
	}

	b = protowire.AppendTag(b, num, protowire.VarintType)
	return protowire.AppendVarint(b, v)
}

func appendBool(b []byte, num protowire.Number, v bool) []byte {
	if !v {
		return b
	}

	return appendVarint(b, num, 1)
}

func appendString(b []byte, num protowire.Number, v string) ([]byte, error) {
	if v == "" {
		return b, nil
	}

	if !utf8.ValidString(v) {
		return nil, invalid("field %d is not valid UTF-8", num)
	}

	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, v), nil
}

func appendBytes(b []byte, num protowire.Number, v []byte) []byte {
	if len(v) == 0 {
		return b
	}

	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, v)
}

func appendMessage(b []byte, num protowire.Number, body []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, body)
}

// appendMap encodes a metadata map as its entries sorted by key, each a
// message of key (1) and value (2), so equal maps hash equally.
func appendMap(b []byte, num protowire.Number, m map[string]string) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		if k == "" {
			return nil, invalid("field %d: metadata entry with an empty key", num)
		}

		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, k := range keys {
		body, err := appendString(nil, 1, k)
		if err != nil {
			return nil, err
		}

		body, err = appendString(body, 2, m[k])
		if err != nil {
			return nil, err
		}

		b = appendMessage(b, num, body)
	}

	return b, nil
}

func appendRef(b []byte, num protowire.Number, ref *Ref) ([]byte, error) {
	if ref == nil {
		return nil, invalid("field %d: missing ref", num)
	}

	if err := noUnknown(ref); err != nil {
		return nil, err
	}

	if !ref.Valid() {
		return nil, invalid("field %d: ref has %d bytes, want %d", num, len(ref.Hash), HashSize)
	}

	body := protowire.AppendTag(nil, 1, protowire.BytesType)
	body = protowire.AppendBytes(body, ref.Hash)

	return appendMessage(b, num, body), nil
}

func canonicalCommit(c *Commit) ([]byte, error) {
	if err := noUnknown(c); err != nil {
		return nil, err
	}

	b := appendVarint(nil, 1, uint64(c.Timestamp))

	b, err := appendRef(b, 2, c.Tree)
	if err != nil {
		return nil, fmt.Errorf("commit tree: %w", err)
	}

	b, err = appendString(b, 3, c.BackupSet)
	if err != nil {
		return nil, err
	}

	if c.Parent != nil {
		b, err = appendRef(b, 4, c.Parent)
		if err != nil {
			return nil, fmt.Errorf("commit parent: %w", err)
		}
	}

	b, err = appendString(b, 5, c.AgentId)
	if err != nil {
		return nil, err
	}

	b = appendVarint(b, 6, uint64(c.ScanStartNs))
	b = appendBool(b, 7, c.Partial)
	b = appendVarint(b, 9, uint64(c.PolicyVersion))
	b = appendVarint(b, 10, c.SetId)
	b = appendVarint(b, 11, uint64(c.ReceivedAtNs))
	b = appendBool(b, 12, c.Consistent)

	b, err = appendMap(b, 13, c.Metadata)
	if err != nil {
		return nil, err
	}

	// present and empty says plain, so it is written even when empty
	if c.KeyId != nil {
		b = protowire.AppendTag(b, 14, protowire.BytesType)
		b = protowire.AppendBytes(b, c.KeyId)
	}

	return appendBool(b, 15, c.Incomplete), nil
}

func canonicalPin(p *Pin) ([]byte, error) {
	if err := noUnknown(p); err != nil {
		return nil, err
	}

	if p.Target == nil {
		return nil, invalid("pin without a target")
	}

	b, err := appendRef(nil, 1, p.Target)
	if err != nil {
		return nil, fmt.Errorf("pin target: %w", err)
	}

	b = appendVarint(b, 3, uint64(p.ReceivedAtNs))

	return appendMap(b, 4, p.Metadata)
}

func canonicalPolicy(p *Policy) ([]byte, error) {
	if err := noUnknown(p); err != nil {
		return nil, err
	}

	b := appendVarint(nil, 1, p.Sequence)
	b = appendVarint(b, 2, uint64(p.WrittenAtNs))

	var (
		scope []byte
		err   error
	)

	switch s := p.Scope.(type) {
	case *Policy_Store:
		scope, err = canonicalStoreScope(s.Store)
		b = appendMessage(b, 3, scope)
	case *Policy_Set:
		scope, err = canonicalSetScope(s.Set)
		b = appendMessage(b, 4, scope)
	case *Policy_Commit:
		scope, err = canonicalCommitScope(s.Commit)
		b = appendMessage(b, 5, scope)
	default:
		return nil, invalid("policy without a scope")
	}

	return b, err
}

func canonicalStoreScope(s *StoreScope) ([]byte, error) {
	if s == nil {
		return nil, invalid("empty store scope")
	}

	if err := noUnknown(s); err != nil {
		return nil, err
	}

	b, err := appendString(nil, 1, s.WritePolicy)
	if err != nil {
		return nil, err
	}

	b = appendVarint(b, 2, uint64(s.WritePolicyVersion))
	b = appendVarint(b, 3, uint64(s.KeyAcknowledgedAtNs))

	b, err = appendString(b, 4, s.DefaultRetention)
	if err != nil {
		return nil, err
	}

	b = appendVarint(b, 5, uint64(s.HoldDays))

	return appendVarint(b, 6, uint64(s.TrashDays)), nil
}

func canonicalSetScope(s *SetScope) ([]byte, error) {
	if s == nil {
		return nil, invalid("empty set scope")
	}

	if err := noUnknown(s); err != nil {
		return nil, err
	}

	if s.SetId == 0 {
		return nil, invalid("set scope without a set id")
	}

	b := appendVarint(nil, 1, s.SetId)

	b, err := appendString(b, 2, s.Name)
	if err != nil {
		return nil, err
	}

	b, err = appendString(b, 3, s.Retention)
	if err != nil {
		return nil, err
	}

	b = appendBool(b, 4, s.RetentionPaused)
	b = appendVarint(b, 5, uint64(s.State))
	b = appendBool(b, 6, s.Erase)

	return appendVarint(b, 7, uint64(s.ClosedAtNs)), nil
}

func canonicalCommitScope(s *CommitScope) ([]byte, error) {
	if s == nil {
		return nil, invalid("empty commit scope")
	}

	if err := noUnknown(s); err != nil {
		return nil, err
	}

	if s.Commit == nil {
		return nil, invalid("commit scope without a commit")
	}

	b, err := appendRef(nil, 1, s.Commit)
	if err != nil {
		return nil, err
	}

	return appendVarint(b, 2, uint64(s.DeletedAtNs)), nil
}

func canonicalFileInfo(info *FileInfo) ([]byte, error) {
	if info == nil {
		return nil, invalid("missing file info")
	}

	if err := noUnknown(info); err != nil {
		return nil, err
	}

	if len(info.Name) == 0 {
		return nil, invalid("file info without a name")
	}

	b := appendBytes(nil, 1, info.Name)
	b = appendVarint(b, 2, uint64(info.Mode))
	b = appendBytes(b, 3, info.User)
	b = appendBytes(b, 4, info.Group)
	b = appendVarint(b, 6, uint64(info.Size))
	b = appendVarint(b, 8, uint64(info.MtimeNs))

	if _, ok := NodeType_name[int32(info.Type)]; !ok {
		return nil, invalid("unknown node type %d", info.Type)
	}

	b = appendVarint(b, 9, uint64(info.Type))

	if len(info.LinkTarget) > 0 && info.Type != NodeType_NODE_SYMLINK {
		return nil, invalid("link target on a %s node", info.Type)
	}

	b = appendBytes(b, 10, info.LinkTarget)
	b = appendVarint(b, 11, uint64(info.CtimeNs))
	b = appendVarint(b, 12, uint64(info.BirthtimeNs))
	b = appendVarint(b, 13, uint64(info.Attributes))

	var err error
	if info.Security != nil {
		if b, err = appendRef(b, 14, info.Security); err != nil {
			return nil, err
		}
	}

	if b, err = appendStreams(b, 15, "stream", info.Streams); err != nil {
		return nil, err
	}

	b = appendVarint(b, 16, uint64(info.ReparseTag))
	b = appendBool(b, 17, info.Lost)
	b = appendVarint(b, 18, uint64(info.Uid))
	b = appendVarint(b, 19, uint64(info.Gid))

	if b, err = appendStreams(b, 20, "xattr", info.Xattrs); err != nil {
		return nil, err
	}

	b = appendVarint(b, 21, uint64(info.LinuxFlags))
	b = appendVarint(b, 22, uint64(info.DarwinFlags))

	if len(info.HardLink) > 0 && info.Type != NodeType_NODE_FILE {
		return nil, invalid("hard link on a %s node", info.Type)
	}

	b = appendBytes(b, 23, info.HardLink)

	return appendBool(b, 24, info.MetadataLost), nil
}

// appendStreams encodes streams, sorted by name and each name once, as
// field num; what names one in an error.
func appendStreams(b []byte, num protowire.Number, what string, streams []*Stream) ([]byte, error) {
	var last []byte

	for i, stream := range streams {
		if stream == nil {
			return nil, invalid("%s %d is empty", what, i)
		}

		if err := noUnknown(stream); err != nil {
			return nil, err
		}

		if len(stream.Name) == 0 {
			return nil, invalid("%s %d without a name", what, i)
		}

		if i > 0 && bytes.Compare(stream.Name, last) <= 0 {
			return nil, invalid("%ss not sorted or not unique at %q", what, stream.Name)
		}
		last = stream.Name

		if stream.Size < 0 {
			return nil, invalid("%s %q of %d bytes", what, stream.Name, stream.Size)
		}

		body := appendBytes(nil, 1, stream.Name)

		body, err := appendRef(body, 2, stream.Ref)
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", what, stream.Name, err)
		}

		b = appendMessage(b, num, appendVarint(body, 3, uint64(stream.Size)))
	}

	return b, nil
}

// SortStreams puts streams in the order a FileInfo records them, by name.
func SortStreams(streams []*Stream) {
	slices.SortFunc(streams, func(a, b *Stream) int { return bytes.Compare(a.Name, b.Name) })
}

func canonicalTree(t *Tree) ([]byte, error) {
	if err := noUnknown(t); err != nil {
		return nil, err
	}

	if len(t.Nodes) > 0 && len(t.Splits) > 0 {
		return nil, invalid("tree has both nodes and splits")
	}

	var b []byte
	var last []byte

	for i, node := range t.Nodes {
		if node == nil {
			return nil, invalid("tree node %d is empty", i)
		}

		if err := noUnknown(node); err != nil {
			return nil, err
		}

		name := node.GetStat().GetName()
		if i > 0 && bytes.Compare(name, last) <= 0 {
			return nil, invalid("tree nodes not sorted or not unique at %q", name)
		}
		last = name

		body, err := canonicalFileInfo(node.Stat)
		if err != nil {
			return nil, fmt.Errorf("tree node %d: %w", i, err)
		}

		body = appendMessage(nil, 1, body)

		if node.Stat.Type == NodeType_NODE_SYMLINK {
			if len(node.Stat.LinkTarget) == 0 {
				return nil, invalid("tree node %q: symlink without a target", name)
			}

			if node.Ref != nil {
				return nil, invalid("tree node %q: symlink with a ref", name)
			}
		} else if node.Stat.Lost {
			if node.Ref != nil {
				return nil, invalid("tree node %q: lost with a ref", name)
			}
		} else {
			body, err = appendRef(body, 2, node.Ref)
			if err != nil {
				return nil, fmt.Errorf("tree node %q: %w", name, err)
			}
		}

		b = appendMessage(b, 1, body)
	}

	for i, split := range t.Splits {
		var err error

		b, err = appendRef(b, 2, split)
		if err != nil {
			return nil, fmt.Errorf("tree split %d: %w", i, err)
		}
	}

	return appendBool(b, 3, t.Incomplete), nil
}

// InlineLimit is the largest file stored inline in its File object.
const InlineLimit = 4 << 10

// Size is how many bytes of the file the part covers: the hole's length
// for a hole, the part's length otherwise.
func (p *FilePart) Size() uint64 {
	if p.GetHole() > 0 {
		return p.Hole
	}

	return p.GetLength()
}

func canonicalFile(f *File) ([]byte, error) {
	if err := noUnknown(f); err != nil {
		return nil, err
	}

	if len(f.Parts) > 0 && len(f.Splits) > 0 {
		return nil, invalid("file has both parts and splits")
	}

	if len(f.Inline) > 0 && (len(f.Parts) > 0 || len(f.Splits) > 0) {
		return nil, invalid("file has inline content and parts")
	}

	if len(f.SplitLengths) > 0 && len(f.SplitLengths) != len(f.Splits) {
		return nil, invalid("file measures %d of its %d splits", len(f.SplitLengths), len(f.Splits))
	}

	if (len(f.SplitLengths) > 0) != (f.SplitDepth > 0) {
		return nil, invalid("file has split lengths without a depth, or a depth without lengths")
	}

	if len(f.Inline) > InlineLimit {
		return nil, invalid("inline content of %d bytes exceeds %d", len(f.Inline), InlineLimit)
	}

	if _, ok := Chunker_name[int32(f.Chunker)]; !ok {
		return nil, invalid("unknown chunker %d", f.Chunker)
	}

	var b []byte

	// a split of a larger file holds a run of its parts, so the parts are
	// contiguous from wherever the first one starts
	var next uint64
	if len(f.Parts) > 0 && f.Parts[0] != nil {
		next = f.Parts[0].Offset
	}

	for i, part := range f.Parts {
		if part == nil {
			return nil, invalid("file part %d is empty", i)
		}

		if err := noUnknown(part); err != nil {
			return nil, err
		}

		if part.Offset != next {
			return nil, invalid("file part %d starts at %d, want %d", i, part.Offset, next)
		}

		if part.Size() == 0 {
			return nil, invalid("file part %d is empty", i)
		}

		next += part.Size()

		body := appendVarint(nil, 1, part.Offset)

		if part.Hole > 0 {
			if part.Ref != nil || part.Length != 0 && part.Length != part.Hole {
				return nil, invalid("file part %d is a hole and has a ref or another length", i)
			}

			b = appendMessage(b, 1, appendVarint(body, 4, part.Hole))
			continue
		}

		body = appendVarint(body, 2, part.Length)

		body, err := appendRef(body, 3, part.Ref)
		if err != nil {
			return nil, fmt.Errorf("file part %d: %w", i, err)
		}

		b = appendMessage(b, 1, body)
	}

	for i, split := range f.Splits {
		var err error

		b, err = appendRef(b, 2, split)
		if err != nil {
			return nil, fmt.Errorf("file split %d: %w", i, err)
		}
	}

	b = appendVarint(b, 3, uint64(f.Chunker))
	b = appendBytes(b, 4, f.Inline)
	b = appendVarint(b, 6, uint64(f.InlineEncryption))

	for i, length := range f.SplitLengths {
		if length == 0 {
			return nil, invalid("file split %d is empty", i)
		}

		b = appendVarint(b, 7, length)
	}

	b = appendVarint(b, 8, uint64(f.SplitDepth))

	return b, nil
}

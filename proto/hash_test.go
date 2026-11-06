package proto

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	pb "google.golang.org/protobuf/proto"
)

func refOf(seed string) *Ref {
	sum := sha256.Sum256([]byte(seed))
	return &Ref{Hash: sum[:]}
}

func TestBlobFraming(t *testing.T) {
	blob := NewObject(&Blob{Data: []byte("hello")})

	want := sha256.Sum256([]byte("blob 5\x00hello"))
	require.Equal(t, want[:], blob.Ref().Hash)
	require.Equal(t, "8aec4e4876f854f688d0ebfc8f37598f38e5fd6903cccc850ca36591175aeb60", hex.EncodeToString(blob.Ref().Hash))
}

func TestTombstoneFraming(t *testing.T) {
	target := refOf("target")

	want := sha256.Sum256(append([]byte("tombstone 32\x00"), target.Hash...))
	require.Equal(t, want[:], TombstoneRef(target).Hash)
}

// Golden vectors pin the canonical encoding; they were produced by an
// independent encoder, and a change here is a format break.
func TestCanonicalGoldenVectors(t *testing.T) {
	tree := refOf("tree")
	part := refOf("part")

	cases := []struct {
		name    string
		object  *Object
		payload string
	}{
		{
			name:    "commit",
			object:  NewObject(&Commit{Timestamp: 1700000000, Tree: tree, BackupSet: "world", AgentId: "agent-1", ScanStartNs: 1700000000000000000, Partial: true}),
			payload: "0880e2cfaa0612220a20dc9c5edb8b2d479e697b4b0b8ab874f32b325138598ce9e7b759eb82921106221a05776f726c642a076167656e742d31308080a8b1e39fe7cb173801",
		},
		{
			name:    "commit with metadata",
			object:  NewObject(&Commit{Timestamp: 1700000000, Tree: tree, BackupSet: "world", AgentId: "agent-1", ScanStartNs: 1700000000000000000, Partial: true, Metadata: map[string]string{"env": "prod", "a": "b"}}),
			payload: "0880e2cfaa0612220a20dc9c5edb8b2d479e697b4b0b8ab874f32b325138598ce9e7b759eb82921106221a05776f726c642a076167656e742d31308080a8b1e39fe7cb1738016a060a01611201626a0b0a03656e76120470726f64",
		},
		{
			name:    "consistent commit",
			object:  NewObject(&Commit{Timestamp: 1700000000, Tree: tree, BackupSet: "world", AgentId: "agent-1", ScanStartNs: 1700000000000000000, Consistent: true}),
			payload: "0880e2cfaa0612220a20dc9c5edb8b2d479e697b4b0b8ab874f32b325138598ce9e7b759eb82921106221a05776f726c642a076167656e742d31308080a8b1e39fe7cb176001",
		},
		{
			name:    "file",
			object:  NewObject(&File{Parts: []*FilePart{{Offset: 0, Length: 5, Ref: part}}}),
			payload: "0a2610051a220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed1",
		},
		{
			name:    "measured split file",
			object:  NewObject(&File{Splits: []*Ref{part, tree}, SplitLengths: []uint64{300, 7}, SplitDepth: 2}),
			payload: "12220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed112220a20dc9c5edb8b2d479e697b4b0b8ab874f32b325138598ce9e7b759eb829211062238ac0238074002",
		},
		{
			name:    "inline file",
			object:  NewObject(&File{Inline: []byte("hi")}),
			payload: "22026869",
		},
		{
			name:    "empty file",
			object:  NewObject(&File{}),
			payload: "",
		},
		{
			name: "tree",
			object: NewObject(&Tree{Nodes: []*TreeNode{
				{Stat: &FileInfo{Name: []byte("a"), Mode: 0644, Size: 5, MtimeNs: 1}, Ref: part},
				{Stat: &FileInfo{Name: []byte("b"), Mode: 0755 | 1<<31, Type: NodeType_NODE_DIRECTORY}, Ref: tree},
			}}),
			payload: "0a300a0a0a016110a4033005400112220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed10a310a0b0a016210ed83808008480112220a20dc9c5edb8b2d479e697b4b0b8ab874f32b325138598ce9e7b759eb8292110622",
		},
		{
			name: "tree with windows metadata",
			object: NewObject(&Tree{Nodes: []*TreeNode{
				{Stat: &FileInfo{
					Name: []byte("a"), Mode: 0644, Size: 5, MtimeNs: 1, CtimeNs: 2, BirthtimeNs: 3, Attributes: 0x20, Security: tree,
					Streams:    []*Stream{{Name: []byte("x"), Ref: part, Size: 4}, {Name: []byte("y"), Ref: part}},
					ReparseTag: 0x9000001a,
				}, Ref: part},
			}}),
			payload: "0ab6010a8f010a016110a4033005400158026003682072220a20dc9c5edb8b2d479e697b4b0b8ab874f32b325138598ce9e7b759eb82921106227a290a017812220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed118047a270a017912220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed180019a8080800912220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed1",
		},
		{
			name: "tree with unix owner ids",
			object: NewObject(&Tree{Nodes: []*TreeNode{
				{Stat: &FileInfo{Name: []byte("a"), Mode: 0644, Size: 5, MtimeNs: 1, User: []byte("u"), Uid: 999, Gid: 70}, Ref: part},
			}}),
			payload: "0a3a0a140a016110a4031a0175300540019001e70798014612220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed1",
		},
		{
			name: "tree with unix metadata",
			object: NewObject(&Tree{Nodes: []*TreeNode{
				{Stat: &FileInfo{
					Name: []byte("a"), Mode: 0644, Size: 5, MtimeNs: 1,
					Xattrs:     []*Stream{{Name: []byte("security.selinux"), Ref: part, Size: 4}, {Name: []byte("user.x"), Ref: part}},
					LinuxFlags: 0x10, DarwinFlags: 0x2, HardLink: []byte("b/a"),
				}, Ref: part},
			}}),
			payload: "0aa7010a80010a016110a40330054001a201380a1073656375726974792e73656c696e757812220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed11804a2012c0a06757365722e7812220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed1a80110b00102ba0103622f6112220a2037a680133bd09342f934afb8dd2c7d9e1b624da5f35e3a38adb103e37c055ed1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := tc.object.Canonical()
			require.NoError(t, err)
			require.Equal(t, tc.payload, hex.EncodeToString(payload))

			// the canonical form must be readable by the generated decoder
			decoded, err := NewObjectFromPayload(payload, tc.object.Type())
			require.NoError(t, err)
			require.True(t, pb.Equal(tc.object, decoded))

			// and re-encoding the decoded form must give the same bytes
			again, err := decoded.Canonical()
			require.NoError(t, err)
			require.Equal(t, payload, again)

			tag, _ := typeTag(tc.object.Type())
			expected := sha256.Sum256(append([]byte(tag+" "+strconv.Itoa(len(payload))+"\x00"), payload...))
			require.Equal(t, expected[:], tc.object.Ref().Hash)
		})
	}
}

func TestCanonicalRejects(t *testing.T) {
	good := refOf("good")

	cases := map[string]*Object{
		"short ref":          NewObject(&Commit{Tree: &Ref{Hash: []byte("short")}}),
		"missing ref":        NewObject(&Commit{}),
		"unsorted tree":      NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("b")}, Ref: good}, {Stat: &FileInfo{Name: []byte("a")}, Ref: good}}}),
		"duplicate name":     NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("a")}, Ref: good}, {Stat: &FileInfo{Name: []byte("a")}, Ref: good}}}),
		"unsorted streams":   NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("a"), Streams: []*Stream{{Name: []byte("y"), Ref: good}, {Name: []byte("x"), Ref: good}}}, Ref: good}}}),
		"unnamed stream":     NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("a"), Streams: []*Stream{{Ref: good}}}, Ref: good}}}),
		"stream without ref": NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("a"), Streams: []*Stream{{Name: []byte("x")}}}, Ref: good}}}),
		"short security ref": NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("a"), Security: &Ref{Hash: []byte("short")}}, Ref: good}}}),
		"nameless node":      NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{}, Ref: good}}}),
		"unsorted xattrs":    NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("a"), Xattrs: []*Stream{{Name: []byte("y"), Ref: good}, {Name: []byte("x"), Ref: good}}}, Ref: good}}}),
		"hard link on dir":   NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("a"), Type: NodeType_NODE_DIRECTORY, HardLink: []byte("x")}, Ref: good}}}),
		"link target on dir": NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("a"), Type: NodeType_NODE_DIRECTORY, LinkTarget: []byte("x")}, Ref: good}}}),
		"nodes and splits":   NewObject(&Tree{Nodes: []*TreeNode{{Stat: &FileInfo{Name: []byte("a")}, Ref: good}}, Splits: []*Ref{good}}),
		"gap in parts":       NewObject(&File{Parts: []*FilePart{{Offset: 0, Length: 5, Ref: good}, {Offset: 6, Length: 1, Ref: good}}}),
		"gap in a split":     NewObject(&File{Parts: []*FilePart{{Offset: 100, Length: 5, Ref: good}, {Offset: 106, Length: 1, Ref: good}}}),
		"empty part":         NewObject(&File{Parts: []*FilePart{{Offset: 0, Length: 0, Ref: good}}}),
		"hole with a ref":    NewObject(&File{Parts: []*FilePart{{Hole: 5, Ref: good}}}),
		"hole with a length": NewObject(&File{Parts: []*FilePart{{Hole: 5, Length: 4}}}),
		"gap after a hole":   NewObject(&File{Parts: []*FilePart{{Hole: 5}, {Offset: 6, Length: 1, Ref: good}}}),
		"parts and splits":   NewObject(&File{Parts: []*FilePart{{Length: 1, Ref: good}}, Splits: []*Ref{good}}),
		"inline and parts":   NewObject(&File{Parts: []*FilePart{{Length: 1, Ref: good}}, Inline: []byte("x")}),
		"inline too large":   NewObject(&File{Inline: make([]byte, InlineLimit+1)}),
		"unknown chunker":    NewObject(&File{Chunker: 99}),
		"unmeasured split":   NewObject(&File{Splits: []*Ref{good, good}, SplitLengths: []uint64{3}, SplitDepth: 1}),
		"empty split":        NewObject(&File{Splits: []*Ref{good}, SplitLengths: []uint64{0}, SplitDepth: 1}),
		"depth unmeasured":   NewObject(&File{Splits: []*Ref{good}, SplitDepth: 1}),
		"lengths no depth":   NewObject(&File{Splits: []*Ref{good}, SplitLengths: []uint64{3}}),
		"empty object":       {},
	}

	for name, obj := range cases {
		t.Run(name, func(t *testing.T) {
			err := obj.Validate()
			require.ErrorIs(t, err, ErrInvalidObject)
			require.Panics(t, func() { obj.Ref() })
		})
	}
}

func TestASplitHoldsARunOfItsFilesParts(t *testing.T) {
	good := refOf("good")

	split := NewObject(&File{Parts: []*FilePart{{Offset: 100, Length: 5, Ref: good}, {Offset: 105, Length: 1, Ref: good}}})
	require.NoError(t, split.Validate())
}

func TestAHoleIsStoredWithoutALengthOrRef(t *testing.T) {
	good := refOf("good")

	stored := NewObject(&File{Parts: []*FilePart{{Length: 5, Ref: good}, {Offset: 5, Hole: 1 << 44}, {Offset: 5 + 1<<44, Length: 1, Ref: good}}})
	counted := NewObject(&File{Parts: []*FilePart{{Length: 5, Ref: good}, {Offset: 5, Length: 1 << 44, Hole: 1 << 44}, {Offset: 5 + 1<<44, Length: 1, Ref: good}}})
	require.NoError(t, stored.Validate())
	require.True(t, counted.Ref().Equal(stored.Ref()))

	payload, err := counted.Canonical()
	require.NoError(t, err)

	decoded, err := NewObjectFromPayload(payload, ObjectType_FILE)
	require.NoError(t, err)
	require.True(t, pb.Equal(stored, decoded))
}

func TestCanonicalRejectsUnknownFields(t *testing.T) {
	// a commit encoded by a newer schema with a field this build does not know
	raw := []byte{0x08, 0x01, 0x12, 0x22, 0x0a, 0x20}
	raw = append(raw, refOf("tree").Hash...)
	raw = append(raw, 0xf8, 0x7f, 0x01) // field 2047, varint 1

	obj, err := NewObjectFromPayload(raw, ObjectType_COMMIT)
	require.NoError(t, err)
	require.ErrorIs(t, obj.Validate(), ErrInvalidObject)
}

func TestVerifyPayload(t *testing.T) {
	blob := NewObject(&Blob{Data: []byte(strings.Repeat("compressible ", 100))})
	payload, err := blob.Canonical()
	require.NoError(t, err)

	stored, compression := Encode(payload)
	require.Equal(t, Compression_ZSTD, compression)
	require.Less(t, len(stored), len(payload))

	obj, err := NewVerifiedObject(stored, compression, ObjectType_BLOB, blob.Ref())
	require.NoError(t, err)
	require.True(t, pb.Equal(blob, obj))

	// a wrong type must not verify even with the right bytes
	_, err = NewVerifiedObject(stored, compression, ObjectType_FILE, blob.Ref())
	require.ErrorIs(t, err, ErrRefMismatch)

	// a flipped byte must not verify
	stored[len(stored)/2] ^= 0xff
	_, err = VerifyPayload(stored, compression, ObjectType_BLOB, blob.Ref())
	require.Error(t, err)

	// incompressible data is stored as is
	random := make([]byte, 1024)
	for i := range random {
		random[i] = byte(i*7919 ^ i>>3)
	}
	_, compression = Encode(random)
	require.Equal(t, Compression_NONE, compression)
}

func TestACommitsKeyIDIsPartOfItsIdentity(t *testing.T) {
	refs := map[string]bool{}

	for _, keyID := range [][]byte{nil, {}, {1}, {2}} {
		obj := NewObject(&Commit{Timestamp: 1, Tree: refOf("tree"), BackupSet: "s", KeyId: keyID})

		payload, err := obj.Canonical()
		require.NoError(t, err)
		decoded, err := NewObjectFromPayload(payload, ObjectType_COMMIT)
		require.NoError(t, err)
		require.Equal(t, keyID, decoded.GetCommit().KeyId)

		refs[hex.EncodeToString(obj.Ref().Hash)] = true
	}

	require.Len(t, refs, 4)
}

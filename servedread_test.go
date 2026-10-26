package goback

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// piece is a run of a file's content: data, or a hole of length zeros.
type piece struct {
	data []byte
	hole int
}

func (p piece) size() int { return len(p.data) + p.hole }

// servedFile is a file an agent committed through a store server and the
// content it reads back as.
type servedFile struct {
	name   string
	ref    *proto.Ref
	pieces []piece
}

func (f servedFile) size() int {
	var n int
	for _, p := range f.pieces {
		n += p.size()
	}

	return n
}

// from hashes the file's content from offset at on.
func (f servedFile) from(at int) []byte {
	h := sha256.New()

	for _, p := range f.pieces {
		skip := min(at, p.size())
		at -= skip

		if p.hole > 0 {
			_, _ = io.CopyN(h, zeroReader{}, int64(p.hole-skip))
		} else {
			h.Write(p.data[skip:])
		}
	}

	return h.Sum(nil)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func hashed(t *testing.T, r io.Reader) []byte {
	t.Helper()

	h := sha256.New()
	_, err := io.Copy(h, r)
	require.NoError(t, err)

	return h.Sum(nil)
}

// putServedFiles commits, through s, a file of several parts and a file of
// three sub-file objects, the second holding a hole that makes the file
// large enough to be split and the third a part the first holds too, its
// blobs sealed under key when there is one.
func putServedFiles(t *testing.T, s *splitStore, key *storekey.Key) []servedFile {
	t.Helper()

	sctx := s.begin("files")
	client := s.client.Client

	random := func(n int) piece {
		data := make([]byte, n)
		_, _ = rand.Read(data)

		return piece{data: data}
	}

	several := random(3 << 20)
	ref, err := backup.PutFile(sctx, client, key, int64(len(several.data)), bytes.NewReader(several.data))
	require.NoError(t, err)

	files := []servedFile{{name: "several", ref: ref, pieces: []piece{several}}}

	part := func(p piece) *proto.FilePart {
		if p.hole > 0 {
			return &proto.FilePart{Length: uint64(p.hole), Hole: uint64(p.hole)}
		}

		obj := proto.NewObject(&proto.Blob{Data: p.data})
		if key != nil {
			obj = proto.NewObject(backup.SealBlob(key, p.data))
		}

		require.NoError(t, client.Put(sctx, obj))

		return &proto.FilePart{Length: uint64(len(p.data)), Ref: obj.Ref()}
	}

	a := random(100 << 10)
	leaves := [][]piece{
		{a, random(50 << 10), random(70 << 10)},
		{random(30 << 10), {hole: backup.SplitFileSize}, random(60 << 10)},
		{a, random(20 << 10)},
	}

	split := &proto.File{SplitDepth: 1}
	file := servedFile{name: "split"}

	for _, leaf := range leaves {
		sub := &proto.File{}
		var length uint64

		for _, p := range leaf {
			held := part(p)
			held.Offset = length
			length += uint64(p.size())
			sub.Parts = append(sub.Parts, held)
		}

		obj := proto.NewObject(sub)
		require.NoError(t, client.Put(sctx, obj))

		split.Splits = append(split.Splits, obj.Ref())
		split.SplitLengths = append(split.SplitLengths, length)
		file.pieces = append(file.pieces, leaf...)
	}

	obj := proto.NewObject(split)
	require.NoError(t, client.Put(sctx, obj))

	file.ref = obj.Ref()
	files = append(files, file)

	var nodes []*proto.TreeNode
	for _, file := range files {
		nodes = append(nodes, &proto.TreeNode{Stat: stat(file), Ref: file.ref})
	}

	tree, err := backup.PutTree(sctx, client, nodes, nil, nil)
	require.NoError(t, err)
	require.NoError(t, client.Put(sctx, commitOf("files", 1, tree)))

	return files
}

func stat(file servedFile) *proto.FileInfo {
	return &proto.FileInfo{Name: []byte(file.name), Size: int64(file.size()), Mode: 0o644, Type: proto.NodeType_NODE_FILE}
}

func TestABackupReaderOverAStoreServerReadsAndSeeksFilesOfSeveralParts(t *testing.T) {
	key, err := storekey.Generate("s1")
	require.NoError(t, err)

	for _, key := range []*storekey.Key{nil, key} {
		t.Run(fmt.Sprintf("sealed=%v", key != nil), func(t *testing.T) {
			s := servedSplits(t)
			reader := backup.NewBackupReader(s.client.Client).WithKey(key)

			for _, file := range putServedFiles(t, s, key) {
				open := func() io.ReadSeeker {
					content, err := reader.ReadFile(s.ctx, file.ref)
					require.NoError(t, err)

					return content
				}

				require.Equal(t, file.from(0), hashed(t, struct{ io.Reader }{open()}), "%s read part by part", file.name)
				require.Equal(t, file.from(0), hashed(t, open()), "%s written out", file.name)

				for _, at := range []int{150 << 10, file.size() - 1000} {
					content := open()
					_, err = content.Seek(int64(at), io.SeekStart)
					require.NoError(t, err)
					require.Equal(t, file.from(at), hashed(t, struct{ io.Reader }{content}), "%s from %d", file.name, at)
				}
			}
		})
	}
}

func TestASalvageOverAStoreServerRestoresAFileWhoseEveryPartIsThere(t *testing.T) {
	s := servedSplits(t)
	restorer := &backup.Restorer{Store: s.client.Client, Salvage: true}

	for _, file := range putServedFiles(t, s, nil) {
		path := filepath.Join(t.TempDir(), file.name)

		outcome, err := restorer.RestoreFile(s.ctx, path, stat(file), file.ref)
		require.NoError(t, err)
		require.Equal(t, backup.OutcomeWritten, outcome, file.name)

		restored, err := os.Open(path)
		require.NoError(t, err)
		require.Equal(t, file.from(0), hashed(t, restored), file.name)
		require.NoError(t, restored.Close())
	}
}

// Package presence holds the Bloom filter an agent consults before
// uploading a blob: a positive answer is probably right, a negative one is
// certain.
package presence

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"

	"github.com/gobackio/goback/proto"

	pb "google.golang.org/protobuf/proto"
)

// FalsePositiveRate is what filters are sized for.
const FalsePositiveRate = 0.01

// Version is the format written by Proto.
const Version = 1

// minHashLen is the ref length the bit positions are cut from.
const minHashLen = 16

var errFormat = errors.New("presence filter is malformed")

// Filter is a Bloom filter over refs.
type Filter struct {
	entries uint64
	bits    uint64
	hashes  uint32
	data    []byte

	// Commit and Set say which commit the filter was built from.
	Commit *proto.Ref
	Set    string
}

// New returns an empty filter sized for n refs at FalsePositiveRate.
func New(n uint64) *Filter {
	return Sized(n, FalsePositiveRate)
}

// Sized returns an empty filter sized for n refs at false-positive rate p.
func Sized(n uint64, p float64) *Filter {
	if n == 0 {
		n = 1
	}

	m := math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2))
	k := math.Round(m / float64(n) * math.Ln2)
	if k < 1 {
		k = 1
	}

	bits := (uint64(m) + 7) / 8 * 8

	return &Filter{bits: bits, hashes: uint32(k), data: make([]byte, bits/8)}
}

func (f *Filter) position(hash []byte, i uint32) uint64 {
	h1 := binary.LittleEndian.Uint64(hash[0:8])
	h2 := binary.LittleEndian.Uint64(hash[8:16]) | 1

	return (h1 + uint64(i)*h2) % f.bits
}

// Add records a ref. Refs shorter than 16 bytes are ignored.
func (f *Filter) Add(hash []byte) {
	if len(hash) < minHashLen {
		return
	}

	for i := uint32(0); i < f.hashes; i++ {
		pos := f.position(hash, i)
		f.data[pos/8] |= 1 << (pos % 8)
	}

	f.entries++
}

// Test reports whether the ref may have been added.
func (f *Filter) Test(hash []byte) bool {
	if f == nil || len(hash) < minHashLen {
		return false
	}

	for i := uint32(0); i < f.hashes; i++ {
		pos := f.position(hash, i)
		if f.data[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}

	return true
}

// Entries is the number of refs added.
func (f *Filter) Entries() uint64 { return f.entries }

// Size is the byte size of the bit array.
func (f *Filter) Size() int { return len(f.data) }

// Proto encodes the filter for storage and transport.
func (f *Filter) Proto() *proto.PresenceFilter {
	return &proto.PresenceFilter{
		Version:   Version,
		Entries:   f.entries,
		Bits:      f.bits,
		Hashes:    f.hashes,
		Data:      f.data,
		Commit:    f.Commit,
		BackupSet: f.Set,
	}
}

// FromProto decodes a filter written by Proto.
func FromProto(p *proto.PresenceFilter) (*Filter, error) {
	if p.GetVersion() != Version || p.GetHashes() == 0 || p.GetBits() == 0 || p.GetBits()%8 != 0 || uint64(len(p.GetData())) != p.GetBits()/8 {
		return nil, errFormat
	}

	return &Filter{
		entries: p.Entries,
		bits:    p.Bits,
		hashes:  p.Hashes,
		data:    p.Data,
		Commit:  p.Commit,
		Set:     p.BackupSet,
	}, nil
}

// Set is the filters of one scope, consulted together.
type Set []*Filter

// Test reports whether any filter may hold the ref.
func (s Set) Test(hash []byte) bool {
	for _, f := range s {
		if f.Test(hash) {
			return true
		}
	}

	return false
}

// Size is the byte size of all bit arrays.
func (s Set) Size() int {
	total := 0
	for _, f := range s {
		total += f.Size()
	}

	return total
}

// Entries is the number of refs over all filters.
func (s Set) Entries() uint64 {
	var total uint64
	for _, f := range s {
		total += f.Entries()
	}

	return total
}

// Digest names a scope's filters, in whatever order they come: a store
// server answers GetPresence with it, and with no filters when the caller
// already holds the filters of that digest.
func Digest(filters []*proto.PresenceFilter) []byte {
	sums := make([][]byte, len(filters))
	for i, filter := range filters {
		data, err := pb.MarshalOptions{Deterministic: true}.Marshal(filter)
		if err != nil {
			panic(err)
		}

		sum := sha256.Sum256(data)
		sums[i] = sum[:]
	}

	slices.SortFunc(sums, bytes.Compare)

	digest := sha256.New()
	for _, sum := range sums {
		digest.Write(sum)
	}

	return digest.Sum(nil)
}

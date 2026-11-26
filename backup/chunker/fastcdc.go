// Package chunker cuts byte streams into content-defined chunks with a
// FastCDC-style normalised Gear hash.
//
// A Profile fixes how bytes are cut, so every agent cutting with the same
// profile cuts identical bytes identically. A profile's target is a power of
// two 2^k from 64 KiB to 1 MiB; chunks are cut from a quarter of it to four
// times it. The gear table entry for byte value v is the first eight bytes,
// big-endian, of SHA-256 of the single byte v. The masks have k+1 (below the
// target) and k-1 (above it) one bits at every third position from the top
// of the 64-bit fingerprint.
package chunker

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/bits"
)

const (
	// DefaultTarget is the target chunk size of the zero Profile.
	DefaultTarget = 64 << 10
	// MaxTarget is the largest target a Profile takes.
	MaxTarget = 1 << 20
	// SmallestChunk is the smallest chunk any profile cuts before the end
	// of a stream.
	SmallestChunk = DefaultTarget / 4
	// LargestChunk is the largest chunk any profile cuts.
	LargestChunk = 4 * MaxTarget
)

var (
	gear  [256]uint64
	masks = map[int][2]uint64{}
)

func init() {
	for v := 0; v < 256; v++ {
		sum := sha256.Sum256([]byte{byte(v)})
		gear[v] = binary.BigEndian.Uint64(sum[:8])
	}

	for target := DefaultTarget; target <= MaxTarget; target *= 2 {
		k := bits.TrailingZeros(uint(target))
		masks[target] = [2]uint64{spreadMask(k + 1), spreadMask(k - 1)}
	}
}

func spreadMask(ones int) uint64 {
	var mask uint64
	for j := 0; j < ones; j++ {
		mask |= 1 << uint(63-3*j)
	}

	return mask
}

// Profile is how a store cuts content: its target chunk size in bytes. The
// zero Profile cuts with DefaultTarget.
type Profile uint32

// NewProfile returns the profile of target, a power of two from
// DefaultTarget to MaxTarget; 0 is the default.
func NewProfile(target uint32) (Profile, error) {
	if target == 0 {
		return 0, nil
	}

	if _, ok := masks[int(target)]; !ok {
		return 0, fmt.Errorf("a chunk size is a power of two from %d to %d bytes, not %d", DefaultTarget, MaxTarget, target)
	}

	if target == DefaultTarget {
		return 0, nil
	}

	return Profile(target), nil
}

// Target is the size chunks are cut around.
func (p Profile) Target() int {
	if p == 0 {
		return DefaultTarget
	}

	return int(p)
}

// Min is the smallest chunk cut before the end of a stream.
func (p Profile) Min() int { return p.Target() / 4 }

// Max is the largest chunk; a cut is forced there.
func (p Profile) Max() int { return p.Target() * 4 }

// Split cuts a whole buffer and returns the chunk lengths in order.
func (p Profile) Split(data []byte) []int {
	var lengths []int

	c := FastCDC{Profile: p}
	for len(data) > 0 {
		n := c.Scan(data, 0)
		if n == 0 {
			n = len(data)
			c.Reset()
		}

		lengths = append(lengths, n)
		data = data[n:]
	}

	return lengths
}

// FastCDC holds the fingerprint of the chunk currently being scanned with
// its Profile.
type FastCDC struct {
	Profile Profile

	fp uint64
}

// Scan looks at chunk[scanned:], where chunk holds the bytes of the current
// chunk collected so far, and returns the chunk length if a cut point was
// found, or 0 if more bytes are needed. A cut resets the fingerprint.
func (c *FastCDC) Scan(chunk []byte, scanned int) int {
	target, smallest, largest := c.Profile.Target(), c.Profile.Min(), c.Profile.Max()
	m := masks[target]

	i := max(scanned, smallest)

	for ; i < len(chunk); i++ {
		c.fp = c.fp<<1 + gear[chunk[i]]

		mask := m[1]
		if i < target {
			mask = m[0]
		}

		if c.fp&mask == 0 {
			c.fp = 0
			return i
		}

		if i+1 >= largest {
			c.fp = 0
			return i + 1
		}
	}

	return 0
}

// Reset forgets the current chunk, for use when the stream ends mid-chunk.
func (c *FastCDC) Reset() {
	c.fp = 0
}

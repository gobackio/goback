package presence

import (
	"crypto/rand"
	"testing"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

func randomHashes(n int) [][]byte {
	hashes := make([][]byte, n)
	for i := range hashes {
		hashes[i] = make([]byte, 32)
		_, _ = rand.Read(hashes[i])
	}

	return hashes
}

func TestFilterNoFalseNegatives(t *testing.T) {
	added := randomHashes(100_000)
	f := New(uint64(len(added)))

	for _, h := range added {
		f.Add(h)
	}

	require.InDelta(t, len(added), f.Entries(), float64(len(added))*FalsePositiveRate)

	for _, h := range added {
		require.True(t, f.Test(h))
	}
}

func TestFilterFalsePositiveRate(t *testing.T) {
	added := randomHashes(100_000)
	f := New(uint64(len(added)))
	for _, h := range added {
		f.Add(h)
	}

	probes := randomHashes(200_000)
	hits := 0
	for _, h := range probes {
		if f.Test(h) {
			hits++
		}
	}

	rate := float64(hits) / float64(len(probes))
	require.Less(t, rate, 0.015, "measured %.3f%%", rate*100)
	require.Greater(t, rate, 0.005, "measured %.3f%%", rate*100)

	// 9.6 bits per entry
	require.InDelta(t, 9.6*float64(len(added))/8, float64(f.Size()), float64(f.Size())/50)
}

func TestFilterProtoRoundTrip(t *testing.T) {
	added := randomHashes(1000)
	f := New(uint64(len(added)))
	for _, h := range added {
		f.Add(h)
	}

	f.Set = "world"

	msg := f.Proto()

	back, err := FromProto(msg)
	require.NoError(t, err)
	require.Equal(t, "world", back.Set)
	require.Equal(t, f.Entries(), back.Entries())

	for _, h := range added {
		require.True(t, back.Test(h))
	}

	msg.Data = msg.Data[:len(msg.Data)-1]
	_, err = FromProto(msg)
	require.Error(t, err)
}

func TestFilterIgnoresShortHashes(t *testing.T) {
	f := New(10)
	f.Add([]byte("short"))
	require.Zero(t, f.Entries())
	require.False(t, f.Test([]byte("short")))
	require.False(t, (*Filter)(nil).Test(randomHashes(1)[0]))
}

func TestSetTestsEveryFilter(t *testing.T) {
	a, b := New(10), New(10)
	ha, hb, hc := randomHashes(1)[0], randomHashes(1)[0], randomHashes(1)[0]
	a.Add(ha)
	b.Add(hb)

	s := Set{a, b}
	require.True(t, s.Test(ha))
	require.True(t, s.Test(hb))
	require.False(t, s.Test(hc))
	require.Equal(t, a.Size()+b.Size(), s.Size())
	require.EqualValues(t, 2, s.Entries())
	require.False(t, Set(nil).Test(ha))
}

func TestADigestNamesTheFiltersInAnyOrder(t *testing.T) {
	a, b := New(10), New(10)
	a.Add(randomHashes(1)[0])
	b.Add(randomHashes(1)[0])

	digest := Digest([]*proto.PresenceFilter{a.Proto(), b.Proto()})
	require.Equal(t, digest, Digest([]*proto.PresenceFilter{b.Proto(), a.Proto()}))
	require.NotEmpty(t, Digest(nil), "no filters have a digest too")
	require.NotEqual(t, Digest(nil), digest)

	b.Add(randomHashes(1)[0])
	require.NotEqual(t, digest, Digest([]*proto.PresenceFilter{a.Proto(), b.Proto()}))
}

func TestFilterCapacityIsWhatItWasSizedFor(t *testing.T) {
	f := New(100_000)
	require.InDelta(t, 100_000, f.Capacity(), 100)
}

func TestFilterAddCountsOnlyNewRefs(t *testing.T) {
	h := randomHashes(1)[0]
	f := New(10)
	f.Add(h)
	f.Add(h)
	require.EqualValues(t, 1, f.Entries())
}

func TestCloneLeavesTheOriginalAlone(t *testing.T) {
	f := New(10)
	clone := f.Clone()
	h := randomHashes(1)[0]
	clone.Add(h)

	require.True(t, clone.Test(h))
	require.False(t, f.Test(h))
	require.Zero(t, f.Entries())
}

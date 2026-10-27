package packtest

import (
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/storage/pack"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestSessionMarkers checks a pack.SessionMarkers: a begin recorded once,
// an end recorded once however many race for it, the listings, and
// forgetting a session.
func TestSessionMarkers(t *testing.T, markers pack.SessionMarkers) {
	begin := pack.SessionBegin{ID: uuid.New().String(), AgentID: "a", Set: "world", Restore: []byte{1, 2},
		Started: time.Now().UTC().Truncate(time.Millisecond), Sealed: 7}

	require.NoError(t, markers.MarkBegun(begin))
	require.ErrorIs(t, markers.MarkBegun(begin), pack.ErrFileExists)

	got, err := markers.SessionBegin(begin.ID)
	require.NoError(t, err)
	require.True(t, begin.Started.Equal(got.Started))
	got.Started = begin.Started
	require.Equal(t, begin, *got)

	_, err = markers.SessionBegin(uuid.New().String())
	require.ErrorIs(t, err, pack.ErrFileNotFound)

	_, ended, err := markers.SessionEnd(begin.ID)
	require.NoError(t, err)
	require.False(t, ended)

	begun, err := markers.Begun()
	require.NoError(t, err)
	require.True(t, begun[begin.ID])

	outcomes := make([]pack.SessionOutcome, 8)

	var wg sync.WaitGroup
	for i := range outcomes {
		wg.Go(func() {
			want := pack.SessionCommitted
			if i%2 == 1 {
				want = pack.SessionAborted
			}

			outcome, err := markers.MarkEnded(begin.ID, want)
			require.NoError(t, err)
			outcomes[i] = outcome
		})
	}
	wg.Wait()

	for _, outcome := range outcomes {
		require.Equal(t, outcomes[0], outcome, "every caller gets the one outcome that won")
	}

	outcome, ended, err := markers.SessionEnd(begin.ID)
	require.NoError(t, err)
	require.True(t, ended)
	require.Equal(t, outcomes[0], outcome)

	ends, err := markers.Ended()
	require.NoError(t, err)
	require.Contains(t, ends, begin.ID)
	require.WithinDuration(t, time.Now(), ends[begin.ID], time.Minute)

	require.NoError(t, markers.DeleteMarkers(begin.ID))

	_, err = markers.SessionBegin(begin.ID)
	require.ErrorIs(t, err, pack.ErrFileNotFound)

	begun, err = markers.Begun()
	require.NoError(t, err)
	require.NotContains(t, begun, begin.ID)

	ends, err = markers.Ended()
	require.NoError(t, err)
	require.NotContains(t, ends, begin.ID)
}

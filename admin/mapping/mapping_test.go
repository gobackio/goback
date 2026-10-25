package mapping

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestADurationPastWhatOneHoldsIsTheLongest(t *testing.T) {
	require.Equal(t, 90*time.Second, Duration(90))
	require.Equal(t, time.Duration(math.MaxInt64), Duration(18446744074/2+1))
	require.Equal(t, time.Duration(math.MaxInt64), Duration(math.MaxInt64))
	require.Equal(t, time.Duration(math.MinInt64), Duration(math.MinInt64))
}

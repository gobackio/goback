package pack

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
)

// pausingRetirement calls pause before storing a retirement.
type pausingRetirement struct {
	*localArchiveStorage
	pause func()
}

func (p *pausingRetirement) CreateNew(name string, data []byte) error {
	if strings.HasSuffix(name, RetirementExt) {
		p.pause()
	}

	return p.localArchiveStorage.CreateNew(name, data)
}

func TestWhileOneProcessMaintainsTheStoreAnotherMaintainsNothingAndReportsBusy(t *testing.T) {
	for _, holder := range []string{"collect", "compact"} {
		t.Run(holder, func(t *testing.T) {
			base := t.TempDir()
			ctx := context.Background()

			paused, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			pause := func() { once.Do(func() { close(paused); <-resume }) }

			config := []PackOption{WithMaxSize(256 * 1024), WithCompaction(CompactionConfig{MinimumCandidates: 0})}
			holding := newTestStore(t, base, append(config, WithArchiveStorage(&pausingRetirement{localArchiveStorage: newLocal(base), pause: pause}))...)
			t.Cleanup(func() { _ = holding.Close() })

			gone, _ := retiredChain(t, holding)
			commit := gone[len(gone)-1].Ref()

			other := newTestStore(t, base, config...)
			t.Cleanup(func() { _ = other.Close() })

			type held struct {
				busy bool
				err  error
			}

			done := make(chan held, 1)

			if holder == "collect" {
				gcAfterBatch = func(int) error { pause(); return nil }
				t.Cleanup(func() { gcAfterBatch = nil })

				go func() {
					report, err := holding.Collect(ctx, gcOptions(t, 0))
					done <- held{report != nil && report.Busy, err}
				}()
			} else {
				go func() {
					report, err := holding.Compact(ctx)
					done <- held{report != nil && report.Busy, err}
				}()
			}

			<-paused

			compacted, err := other.Compact(ctx)
			require.NoError(t, err)
			require.True(t, compacted.Busy, "compact")

			collected, err := other.Collect(ctx, gcOptions(t, 0))
			require.NoError(t, err)
			require.True(t, collected.Busy, "collect")

			rewritten, err := other.RewritePlan(ctx)
			require.NoError(t, err)
			require.True(t, rewritten.Busy, "rewrite")

			repaired, err := other.Repair(ctx)
			require.NoError(t, err)
			require.True(t, repaired.Busy, "repair")

			revivals, err := other.Revive(ctx, []*proto.Ref{commit}, false)
			require.NoError(t, err)
			require.Len(t, revivals, 1)
			require.True(t, revivals[0].Busy, "revive")
			require.False(t, revivals[0].Whole(), "a busy revival checked nothing")

			resealed, err := other.Reseal(ctx, nil)
			require.NoError(t, err)
			require.True(t, resealed.Busy, "reseal")

			close(resume)

			result := <-done
			require.NoError(t, result.err)
			require.False(t, result.busy)

			compacted, err = other.Compact(ctx)
			require.NoError(t, err)
			require.False(t, compacted.Busy, "the holder released the lease")
		})
	}
}

func TestAMaintenanceLeaseHeldElsewhereKeepsTheStoreUntilReleasedOrUnrenewed(t *testing.T) {
	for _, c := range []struct {
		name    string
		data    []byte
		age     time.Duration
		compact bool
	}{
		{"held", leaseData(), 0, false},
		{"unrenewed", leaseData(), maintenanceLeaseTTL + time.Minute, true},
		{"released", leaseReleased, 0, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			base := t.TempDir()
			store := newTestStore(t, base, WithCompaction(CompactionConfig{MinimumCandidates: 1}))
			defer store.Close()

			objects := makeTestData(t, 20)
			for i := 0; i < len(objects); i += 5 {
				for _, object := range objects[i : i+5] {
					require.NoError(t, store.Put(context.Background(), object))
				}

				require.NoError(t, store.Flush())
			}

			require.NoError(t, store.storage.CreateNew(leaseName(7), c.data))
			modified := time.Now().Add(-c.age)
			require.NoError(t, os.Chtimes(filepath.Join(base, leaseName(7)), modified, modified))

			report := compact(t, context.Background(), store)
			require.Equal(t, !c.compact, report.Busy)
			require.Equal(t, c.compact, report.Rewritten > 0)

			leases, err := store.leases()
			require.NoError(t, err)
			require.Len(t, leases, 1, "a lease is kept until a newer one is stored")

			if c.compact {
				require.EqualValues(t, 8, leases[0].n)

				held, err := store.held(leases[0])
				require.NoError(t, err)
				require.False(t, held, "the compaction released its lease")
			}
		})
	}
}

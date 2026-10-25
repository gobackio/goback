package sql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/index/sql/migrations"
	"github.com/gobackio/goback/testing/testpg"

	"ariga.io/atlas/sql/migrate"
	"github.com/stretchr/testify/require"
)

// concurrentOpen opens several indexes on one empty database at once; the
// migration runs once and every open succeeds.
func concurrentOpen(t *testing.T, open func() *Index) {
	t.Helper()

	var wg sync.WaitGroup
	results := make([]error, 4)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()

			x := open()
			results[i] = x.Open()
			if results[i] == nil {
				results[i] = x.Close()
			}
		}()
	}
	wg.Wait()

	for i, err := range results {
		require.NoError(t, err, "open %d", i)
	}
}

func TestConcurrentOpenOfADirectory(t *testing.T) {
	dir := t.TempDir()
	concurrentOpen(t, func() *Index { return New(dir, newMemStore()) })
}

// TestOpensADirectory keeps an SQLite database in the directory the CLI
// names and finds it again on the next open.
func TestOpensADirectory(t *testing.T) {
	dir := t.TempDir()
	store := newMemStore()

	x := New(dir, store)
	require.NoError(t, x.Open())
	_, err := x.setNamed(context.Background(), x.client, "world", 0)
	require.NoError(t, err)
	require.NoError(t, x.Close())

	_, err = os.Stat(filepath.Join(dir, sqliteFile))
	require.NoError(t, err)

	y := New("sqlite://"+filepath.ToSlash(dir), store)
	require.NoError(t, y.Open())
	t.Cleanup(func() { _ = y.Close() })

	sets, err := y.ListSets(context.Background())
	require.NoError(t, err)
	require.Len(t, sets, 1)
	require.Equal(t, "world", sets[0].Name)
}

// TestStoredDefaultPolicyOutranksTheBuiltIn: the CLI's keep-all default
// applies until an operator stores a default policy, which every set
// without one of its own then follows.
func TestStoredDefaultPolicyOutranksTheBuiltIn(t *testing.T) {
	l := newLocalIndex(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		l.commit("a", "world.dat", int64(i), 0, fmt.Sprint(i))
	}

	commits, err := l.x.CommitInfo(l.ctx, "a", time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, commits, 5)

	require.NoError(t, l.x.SetDefaultPolicy(ctx, &retention.Policy{KeepLast: 3}))
	commits, err = l.x.CommitInfo(l.ctx, "a", time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, commits, 3)

	require.NoError(t, l.x.SetPolicy(ctx, "a", &retention.Policy{KeepLast: 4}))
	commits, err = l.x.CommitInfo(l.ctx, "a", time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, commits, 4, "a set's own policy wins")

	require.NoError(t, l.x.SetPolicy(ctx, "a", nil))
	commits, err = l.x.CommitInfo(l.ctx, "a", time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, commits, 3, "and inherits the default again once cleared")

	require.NoError(t, l.x.SetDefaultPolicy(ctx, nil))
	commits, err = l.x.CommitInfo(l.ctx, "a", time.Now(), 10)
	require.NoError(t, err)
	require.Len(t, commits, 5, "without a stored default the built-in keep-all applies")
}

// TestPostgres runs the suite against a Postgres container; it is skipped
// without Docker.
func TestPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	ctx := context.Background()

	previous := openIndex
	openIndex = func(t testing.TB, store backup.ObjectStore) *Index {
		t.Helper()

		x := New(testpg.Start(t), store)
		require.NoError(t, x.Open())
		t.Cleanup(func() { _ = x.Close() })

		return x
	}
	t.Cleanup(func() { openIndex = previous })

	t.Run("MigrationsMatchSchema", func(t *testing.T) {
		scratch, err := sql.Open("pgx", testpg.Start(t))
		require.NoError(t, err)
		t.Cleanup(func() { _ = scratch.Close() })

		dir, err := migrations.Dir(migrations.Postgres)
		require.NoError(t, err)
		require.ErrorIs(t, migrations.Diff(ctx, scratch, migrations.Postgres, dir, "drift"), migrate.ErrNoPlan)
	})

	t.Run("ConcurrentSetCreation", func(t *testing.T) {
		x := openIndex(t, newMemStore())

		var (
			wg      sync.WaitGroup
			results = make([]error, 8)
			ids     = make([]int64, 8)
		)

		for i := range results {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ids[i], results[i] = x.setNamed(ctx, x.client, "world", 0)
			}(i)
		}
		wg.Wait()

		for i, err := range results {
			require.NoError(t, err, "a racer finds the set another created, not a constraint error")
			require.Equal(t, ids[0], ids[i])
		}
	})

	t.Run("ConcurrentSetCreationWithInitialRetention", func(t *testing.T) {
		x := openIndex(t, newMemStore())

		var (
			wg      sync.WaitGroup
			results = make([]error, 8)
			ids     = make([]int64, 8)
		)

		for i := range results {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				if i%2 == 0 {
					ids[i], results[i] = x.createSet(ctx, "world", &retention.DatabaseBase)
				} else {
					ids[i], results[i] = x.setNamed(ctx, x.client, "world", 0)
				}
			}(i)
		}
		wg.Wait()

		for i, err := range results {
			require.NoError(t, err, "a racer finds the set another created, not a constraint error")
			require.Equal(t, ids[0], ids[i])
		}
	})

	t.Run("ConcurrentOpen", func(t *testing.T) {
		url := testpg.Start(t)
		concurrentOpen(t, func() *Index { return New(url, newMemStore()) })
	})

	for name, test := range map[string]func(*testing.T){
		"RangesFollowVersions":             TestIndexerRangesFollowVersions,
		"KeepsSealedOwnerNames":            TestIndexKeepsSealedOwnerNames,
		"RebuildRestoresOperatorChanges":   TestRebuildRestoresWhatOperatorsChanged,
		"PolicySequenceFollowsChanges":     TestPolicySequenceFollowsEveryChange,
		"KeepsOrderAndRefusesClosedSets":   TestIndexerKeepsOrderAndRefusesClosedSets,
		"RetentionLifecycle":               TestRetentionLifecycle,
		"RetireMarksRowsOnlyAfterFlush":    TestRetireMarksRowsOnlyAfterFlush,
		"RetireReadsOnlyUnwalkedArchives":  TestRetireReadsOnlyTheArchivesItHasNotWalked,
		"RetireFindsALostPin":              TestRetireFindsALostPinOnceTheWalkedArchivesAreForgotten,
		"RetireLeavesOutAnUnpinnedPin":     TestRetireLeavesOutAPinUnpinnedInAnotherArchive,
		"ReIndexForgetsWalkedArchives":     TestReIndexForgetsTheWalkedArchives,
		"PinsFollowTheSetState":            TestPinsFollowTheSetState,
		"DeleteCommitRefusesAPinnedCommit": TestDeleteCommitRefusesAPinnedCommit,
		"RebuiltSetKeepsEverything":        TestRebuiltSetKeepsEverythingWhilePaused,
		"DeletedSetProceedsWhilePaused":    TestDeletedSetProceedsWhilePaused,
		"TombstonePrunesRefs":              TestTombstonePrunesTheRefsOnlyItReaches,
		"RetirementPrunesEachSet":          TestOneRetirementPrunesEachSetsDeepTreesOnItsOwn,
		"ReIndexReconcilesTombstones":      TestReIndexReconcilesTombstonesOnAnExistingDatabase,
		"UnretireBringsCommitsBack":        TestUnretireBringsTombstonedCommitsBackWithTheirRows,
		"UnretireLeavesBrokenCommits":      TestUnretireLeavesACommitMissingAnObjectTombstoned,
		"UnretireRefusesALiveCommit":       TestUnretireRefusesALiveCommit,
		"UnretireCompletesACrashedRevival": TestUnretireCompletesARevivalThatCrashedBeforeItsRows,
		"RestoreLeasesHoldRetirement":      TestRestoreLeasesHoldRetirement,
		"PinsKeepCommitsAlive":             TestPinsKeepCommitsAlive,
		"PartialCheckpointRetires":         TestPartialCheckpointRetiresWhenSuperseded,
		"DeleteSetAndRebuild":              TestDeleteSetAndRebuild,
		"ReIndexReproducesRanges":          TestReIndexReproducesRanges,
		"PresenceFilterNeverLands":         TestPresenceFilterNeverLandsOnATombstonedCommit,
		"EnsureSetFindsASetByName":         TestEnsureSetFindsASetByName,
		"EnsureSetRecreatesUnderCarriedID": TestEnsureSetRecreatesUnderCarriedID,
		"RebuildPlacesUnnamedCommits":      TestReIndexPutsTheCommitsThatNameNoSetUnderAPlaceholderSet,
		"IndexCommitsPlacesUnnamed":        TestIndexCommitsPutsCommitsThatNameNoSetUnderAPlaceholderSet,
		"PresenceFollowsTheHead":           TestPresenceFollowsTheHead,
		"LogicalSizeFollowsCommits":        TestLogicalSizeFollowsCommits,
		"LogicalSizeBeforeMaintenance":     TestLogicalSizeIsKnownBeforeMaintenanceRuns,
		"LogicalSizeSkipsEmptyNodes":       TestLogicalSizeLeavesOutWhatHoldsNoContent,
		"UniqueSizeCountsVersionsOnce":     TestUniqueSizeCountsEachFileVersionOnce,
		"UniqueSizeSkipsRetiring":          TestUniqueSizeLeavesOutVersionsOnlyARetiringCommitHolds,
		"ListingReadsMeasuredSizes":        TestListingReadsTheSizesMeasureSetsStored,
		"FillingSizesRepairsOldRows":       TestFillingSizesRepairsCommitsIndexedWithoutOne,
		"FillingSizesSkipsTombstoned":      TestFillingSizesLeavesATombstonedCommitAlone,
		"RootOwnerNamesTheSet":             TestRootOwnerNamesTheSetBehindACommitOrPin,
		"RootDeathFollowsThePolicy":        TestRootDeathExpectsEachCommitToDieWhenItsPolicyLetsItGo,
		"RecordedSetSizes":                 TestRecordedSetSizesReplaceTheLastRuns,
		"RecordedSetSizesInBatches":        TestRecordedSetSizesReachEverySetAcrossBatchesAndCountUnknownOnes,
		"PendingPresence":                  TestPendingPresenceIsBuiltForEachSetWhoseNewestLiveCommitLacksItOnceItsFilterIsOld,
		"EvaluationRetiresOnce":            TestEvaluationRetiresEachCommitOnceAndKeepsWhatAWiderPolicyCovers,
		"IndexingRecordsEveryChange":       TestIndexingRecordsTheVersionsOfEveryKindOfChange,
		"MarkingManyLost":                  TestMarkingManyVersionsLostReachesEachSetAndPath,
		"BeginCommitGates":                 TestBeginCommitGates,
		"InitialRetentionOnCreation":       TestASetCreatedUnderASessionStartsWithItsInitialRetention,
		"InitialRetentionLeavesExisting":   TestAnExistingSetKeepsItsPolicyWhateverASessionCarries,
		"SessionKeepsInitialRetention":     TestASessionKeepsItsInitialRetention,
		"SetRefsFollowCommits":             TestSetRefsFollowCommits,
		"SameSecondCommitsAreKept":         TestSameSecondCommitsAreKept,
		"LatestCommitFollowsReceiptOrder":  TestLatestCommitFollowsReceiptOrder,
		"PutStampsReceiptTime":             TestPutStampsReceiptTime,
		"PutRejectsDanglingCommit":         TestPutRejectsDanglingCommit,
		"PutRefusesACommitBehind":          TestPutIndexesOrRefusesACommitStoredAfterANewerOne,
		"FileInfoOrdersVersions":           TestFileInfoOrdersVersionsByBackupTime,
		"CommitDetailsBefore":              TestCommitDetailsBeforeKeepsInstantsWhole,
		"VersionsBefore":                   TestVersionsBeforePagesNewestFirst,
		"ReadDirAfter":                     TestReadDirAfterPagesByteByByte,
		"CountCommitsPerPeriod":            TestCountCommitsPerPeriodInAZone,
		"CountCommitsLiveOrDeleted":        TestCountCommitsLiveOrDeleted,
		"QuerySetsByName":                  TestQuerySetsPagesByName,
		"QuerySetsPicks":                   TestQuerySetsPicksByStateAndName,
		"QuerySetsBySize":                  TestQuerySetsPagesBySize,
		"GetSet":                           TestGetSetFindsOneSetByName,
		"GetCommitDetail":                  TestGetCommitDetailFindsOnlyTheSetsLiveCommit,
		"GetCommitDetails":                 TestGetCommitDetailsFindsTheSetsCommitsAmongRefs,
		"PinsOf":                           TestPinsOfListsThePinsOfTheTargets,
		"CommitInfoSkipsCheckpoints":       TestCommitInfoSkipsCheckpoints,
		"IndexesASplitRoot":                TestIndexesASplitRoot,
		"UnknownSetIsEmpty":                TestUnknownSetIsEmpty,
		"OperatorRetentionSurface":         TestOperatorRetentionSurface,
		"StoredDefaultPolicy":              TestStoredDefaultPolicyOutranksTheBuiltIn,
		"ArchiveIndex":                     TestArchiveIndex,
		"ArchiveIndexExclusion":            TestArchiveIndexExclusion,
		"ArchiveIndexSessions":             TestArchiveIndexSessions,
		"ClaimIndex":                       TestClaimIndex,
		"ArchiveIndexCounts":               TestArchiveIndexCountsAndRestoreSessions,
		"PastTheBindLimit":                 TestASetWithMoreCommitsThanOneStatementBindsKeepsWorking,
		"CommitAfterATombstonedNewest":     TestACommitAfterATombstonedNewestOneHoldsOnlyWhatItHolds,
		"CommitPlannedAsNewestTombstoned":  TestACommitPlannedWhileItsSetsNewestIsTombstonedKeepsWhatTheyShare,
	} {
		t.Run(name, test)
	}
}

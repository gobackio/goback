package mysql

import (
	"errors"
	"strings"
	"testing"

	"github.com/gobackio/goback/backup"

	"github.com/stretchr/testify/require"
)

func scanned(t *testing.T, dump string, chunk int) *dumpScanner {
	t.Helper()

	s := &dumpScanner{}
	for len(dump) > 0 {
		n := min(chunk, len(dump))
		_, err := s.Write([]byte(dump[:n]))
		require.NoError(t, err)
		dump = dump[n:]
	}

	return s
}

func TestTheDumpScannerReadsThePositionInEitherForm(t *testing.T) {
	srv := &Server{Version: "5.5.62-log", ServerID: 7}

	for _, line := range []string{
		"-- CHANGE MASTER TO MASTER_LOG_FILE='mysql-bin.000005', MASTER_LOG_POS=107;",
		"-- CHANGE REPLICATION SOURCE TO SOURCE_LOG_FILE='mysql-bin.000005', SOURCE_LOG_POS=107;",
	} {
		dump := "-- MySQL dump 10.13\n\n" + line + "\n\nINSERT INTO `a` VALUES (1);\n-- Dump completed on 2026-10-11  9:37:42\n"

		for _, chunk := range []int{1, 7, len(dump)} {
			meta, err := scanned(t, dump, chunk).metadata(srv, true)
			require.NoError(t, err)
			require.Equal(t, "mysql-bin.000005", meta[MetaBinlogFile])
			require.Equal(t, "107", meta[MetaBinlogPos])
			require.Equal(t, "7", meta[MetaServerID])
		}
	}
}

func TestTheDumpScannerReadsPastLongLines(t *testing.T) {
	insert := "INSERT INTO `a` VALUES " + strings.Repeat("(1),", 1<<20) + "(1);"
	dump := "-- CHANGE MASTER TO MASTER_LOG_FILE='b.000001', MASTER_LOG_POS=4;\n" + insert + "\n-- Dump completed\n"

	meta, err := scanned(t, dump, 4096).metadata(&Server{}, true)
	require.NoError(t, err)
	require.Equal(t, "b.000001", meta[MetaBinlogFile])
}

func TestTheDumpScannerRefusesADumpCutShortOrWithoutPosition(t *testing.T) {
	_, err := scanned(t, "-- CHANGE MASTER TO MASTER_LOG_FILE='b.000001', MASTER_LOG_POS=4;\nINSERT INTO `a` VALUES (1);\n", 64).metadata(&Server{}, true)
	require.Error(t, err)

	_, err = scanned(t, "INSERT INTO `a` VALUES (1);\n-- Dump completed on 2026-10-11\n", 64).metadata(&Server{}, true)
	require.Error(t, err)

	meta, err := scanned(t, "INSERT INTO `a` VALUES (1);\n-- Dump completed on 2026-10-11\n", 64).metadata(&Server{}, false)
	require.NoError(t, err)
	require.NotContains(t, meta, MetaBinlogFile)
}

func TestDumpArgsFollowTheVersionAndTheTables(t *testing.T) {
	require.Equal(t, []string{"--single-transaction", "--routines", "--triggers", "--events", "--all-databases", "--flush-privileges", "--master-data=2"},
		DumpArgs([3]int{5, 5, 62}, false, true, false))
	require.Equal(t, []string{"--lock-all-tables", "--routines", "--triggers", "--events", "--all-databases", "--flush-privileges", "--source-data=2", "--set-gtid-purged=OFF"},
		DumpArgs([3]int{8, 0, 26}, true, true, true))
	require.Equal(t, []string{"--single-transaction", "--routines", "--triggers", "--events", "--all-databases", "--flush-privileges"},
		DumpArgs([3]int{8, 0, 46}, false, false, false))
}

func TestParseDumpVersion(t *testing.T) {
	v, err := parseDumpVersion("mysqldump  Ver 10.13 Distrib 5.5.62, for linux-glibc2.12 (x86_64)")
	require.NoError(t, err)
	require.Equal(t, [3]int{5, 5, 62}, v)

	v, err = parseDumpVersion("mysqldump  Ver 8.0.46 for Linux on x86_64 (MySQL Community Server - GPL)")
	require.NoError(t, err)
	require.Equal(t, [3]int{8, 0, 46}, v)
}

func TestADumpRecordsWhereItStartsAndLocksForNonTransactionalTables(t *testing.T) {
	f := newFixture(t)
	f.srv.nonTransactional = []string{"app.log"}

	result := f.mustDump()
	meta := result.Commit.Metadata
	require.Equal(t, "binlog.000001", meta[MetaBinlogFile])
	require.Equal(t, "all-tables", meta[MetaLock])
	require.Equal(t, "1", meta[MetaChain])
	require.Equal(t, f.srv.uuid, meta[MetaServerUUID])
	require.Contains(t, f.srv.dumpArgs, "--lock-all-tables")
	require.Contains(t, f.srv.dumpArgs, "--source-data=2")
}

func TestAFailedDumpLeavesTheSetWithoutACommit(t *testing.T) {
	f := newFixture(t)
	f.srv.dumpErr = errors.New("mysqldump: exit status 2")

	_, err := f.takeDump()
	require.ErrorContains(t, err, "exit status 2")

	_, err = f.index.LatestCommit(f.ctx, "db-dump")
	require.ErrorIs(t, err, backup.ErrNotFound)
}

func TestADumpOfAnotherServerIsRefused(t *testing.T) {
	f := newFixture(t)
	f.mustDump()

	f.srv.uuid = "99999999-71ca-11e1-9e33-c80aa9429562"
	_, err := f.takeDump()
	require.ErrorIs(t, err, ErrForeignServer)
}

func TestADumpOpensANewChainWhenTheNumberingWentBackwards(t *testing.T) {
	f := newFixture(t)
	f.srv.flush()
	f.srv.flush()
	require.Equal(t, "1", f.mustDump().Commit.Metadata[MetaChain])

	f.srv.flush()
	require.Equal(t, "1", f.mustDump().Commit.Metadata[MetaChain])

	f.srv.resetMaster()
	require.Equal(t, "2", f.mustDump().Commit.Metadata[MetaChain])
}

func TestADumpWithoutPositionNeedsNoBinlog(t *testing.T) {
	f := newFixture(t)
	f.srv.logBin = false
	f.srv.dump = "INSERT INTO t VALUES (1);\n-- Dump completed on 2026-10-11  9:37:42\n"

	result, err := (&Dump{Walker: f.walker("db-dump"), Client: f.srv}).Run(f.ctx)
	require.NoError(t, err)
	require.NotContains(t, result.Commit.Metadata, MetaBinlogFile)
	require.NotContains(t, f.srv.dumpArgs, "--source-data=2")
}

package backup

// MetaKind is the commit metadata key naming what made the commit, one of
// the Kind values.
const MetaKind = "goback.kind"

// The kinds of commit MetaKind names.
const (
	KindPath         = "path"
	KindPostgresBase = "postgres-base"
	KindPostgresWAL  = "postgres-wal"
	KindMySQLDump    = "mysql-dump"
	KindMySQLBinlog  = "mysql-binlog"
)

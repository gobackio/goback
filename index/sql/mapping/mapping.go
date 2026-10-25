// Package mapping turns index rows into the protos and structs the rest
// of goback speaks. The generated mapper asserts against Mapper, which
// would be an import cycle if the interface lived in package sql.
package mapping

import (
	"encoding/hex"
	"path"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/set"
	"github.com/gobackio/goback/proto"
	"github.com/gobackio/goback/storage/pack"
)

//go:generate mapper .

// mapper:generate
type Mapper interface {
	// field:Timestamp method:"Unix"
	// field:ReceivedAtNs from:"ReceivedAt" method:"UnixNano"
	// field:Tree using:"Ref"
	// field:BackupSet from:"Edges.Set.Name"
	// field:Parent using:"Ref"
	// field:KeyId from:"KeyID" using:"KeyID"
	Commit(in *ent.CommitRow) *proto.Commit

	// field:Stat from:"."
	// field:Ref using:"Ref"
	TreeNode(in *ent.File) *proto.TreeNode

	// field:Name from:"Path" using:"Base"
	// field:User using:"Owner"
	// field:Group using:"Owner"
	// field:Type using:"NodeType"
	// field:CtimeNs from:"-"
	// field:BirthtimeNs from:"-"
	// field:Attributes from:"-"
	// field:Security from:"-"
	// field:Streams from:"-"
	// field:ReparseTag from:"-"
	// field:Lost from:"-"
	// field:Uid from:"-"
	// field:Gid from:"-"
	// field:Xattrs from:"-"
	// field:LinuxFlags from:"-"
	// field:DarwinFlags from:"-"
	// field:HardLink from:"-"
	FileInfo(in *ent.File) *proto.FileInfo

	// field:Ref using:"Ref"
	// field:Target using:"Ref"
	// field:ReceivedAtNs from:"ReceivedAt" method:"UnixNano"
	Pin(in *ent.Pin) *proto.PinInfo

	// field:Set from:"BackupSet"
	// field:Restore from:"RestoreRef" using:"Ref"
	// field:Started from:"StartedAt"
	Session(in *ent.Session) *backup.Session

	// field:Name from:"ID"
	// field:Session from:"SessionID"
	// field:Created from:"CreatedAt"
	Archive(in *ent.Archive) pack.ArchiveInfo

	// field:Archive from:"ArchiveID"
	// field:Record from:"."
	Location(in *ent.Object) pack.IndexLocation

	// field:Sum from:"Ref" using:"Sum"
	// field:Offset from:"Start"
	Record(in *ent.Object) pack.IndexRecord

	Set(in *ent.Set) index.SetInfo

	// field:Commit from:"."
	// field:Ref using:"Ref"
	// field:Files from:"FileCount"
	CommitDetail(in *ent.CommitRow) index.CommitDetail

	// field:Ref using:"Ref"
	// field:Commit from:"."
	// field:Size from:"."
	// field:DeletedAtNs from:"DeletedAt" using:"Nanos"
	// field:ExpiresAtNs from:"ExpiresAt" using:"Nanos"
	TrashedCommit(in *ent.CommitRow) *proto.TrashedCommit

	// field:LogicalBytes from:"LogicalSize"
	// field:Files from:"FileCount"
	CommitSize(in *ent.CommitRow) *proto.CommitSize

	// field:Node from:"."
	// field:From from:"ValidFrom"
	Version(in *ent.File) index.Version

	// field:Ref using:"Ref"
	// field:Set from:"-"
	// field:Open from:"ValidUntil" using:"Open"
	FilePath(in *ent.File) backup.FilePath

	// field:WritePolicy from:"Policy"
	// field:WritePolicyVersion from:"PolicyVersion"
	// field:KeyAcknowledgedAtNs from:"KeyAcknowledgedAt" using:"Nanos"
	// field:DefaultRetention from:"RetentionPolicy"
	// field:HoldDays from:"-"
	StoreScope(in *ent.Settings) *proto.StoreScope

	// field:SetId from:"ID"
	// field:Retention from:"RetentionPolicy"
	// field:State using:"SetState"
	// field:ClosedAtNs from:"-"
	SetScope(in *ent.Set) *proto.SetScope
}

// Nanos is a stored time in nanoseconds since the Unix epoch, 0 for none.
func Nanos(t *time.Time) int64 {
	if t == nil {
		return 0
	}

	return t.UnixNano()
}

// Open is whether a version has no end yet.
func Open(validUntil *time.Time) bool {
	return validUntil == nil
}

// SetState is a stored set state as policies record it.
func SetState(s set.State) proto.SetState {
	return setStates[s]
}

var setStates = map[set.State]proto.SetState{
	set.StateActive:  proto.SetState_SET_ACTIVE,
	set.StateClosing: proto.SetState_SET_CLOSING,
	set.StateDeleted: proto.SetState_SET_DELETED,
}

// KeyID is a stored commit key id: nil when the commit records none,
// empty but not nil when it records a plain commit.
func KeyID(stored *string) []byte {
	if stored == nil {
		return nil
	}

	id, _ := hex.DecodeString(*stored)
	if id == nil {
		return []byte{}
	}

	return id
}

// Ref wraps a stored hash, nil for none.
func Ref(hash []byte) *proto.Ref {
	if len(hash) == 0 {
		return nil
	}

	return &proto.Ref{Hash: hash}
}

// Sum is a stored hash as an index record sum.
func Sum(hash []byte) [proto.HashSize]byte {
	var sum [proto.HashSize]byte
	copy(sum[:], hash)

	return sum
}

// NodeType is a stored node type.
func NodeType(t uint32) proto.NodeType {
	return proto.NodeType(t)
}

// Owner is a stored user or group name, which an encrypted store keeps
// sealed.
func Owner(stored string) []byte {
	return proto.NameFromComponent(stored)
}

// Base is the name component of a stored path.
func Base(p string) []byte {
	return proto.NameFromComponent(path.Base(p))
}

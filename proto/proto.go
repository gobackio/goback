package proto

//go:generate protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. api.proto blob.proto commit.proto encryption.proto file.proto object.proto pin.proto policy.proto presence.proto ref.proto retention.proto tree.proto
//go:generate protoc -I . --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. admin/admin.proto

import (
	"os"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	pb "google.golang.org/protobuf/proto"
)

// Bytes marshals a message and panics on the (unreachable) marshal errors.
func Bytes(m pb.Message) []byte {
	data, err := pb.Marshal(m)
	if err != nil {
		panic(err)
	}

	return data
}

// EncodeVarint appends x as a protobuf varint.
func EncodeVarint(buf []byte, x uint64) []byte {
	return protowire.AppendVarint(buf, x)
}

// DecodeVarint reads a protobuf varint and the number of bytes it took;
// n is negative on a malformed input.
func DecodeVarint(buf []byte) (x uint64, n int) {
	return protowire.ConsumeVarint(buf)
}

// Metadata reports whether objects of the type describe a backup rather
// than carry its content: commits, trees and files, which metadata caches
// keep.
func (x ObjectType) Metadata() bool {
	switch x {
	case ObjectType_COMMIT, ObjectType_TREE, ObjectType_FILE:
		return true
	}

	return false
}

// Type is the object's type; a sealed object reports the type it carries,
// an empty wrapper INVALID.
func (o *Object) Type() ObjectType {
	switch t := o.GetObject().(type) {
	case *Object_Commit:
		return ObjectType_COMMIT
	case *Object_Tree:
		return ObjectType_TREE
	case *Object_Blob:
		return ObjectType_BLOB
	case *Object_File:
		return ObjectType_FILE
	case *Object_Sealed:
		return t.Sealed.GetType()
	case *Object_Pin:
		return ObjectType_PIN
	case *Object_Policy:
		return ObjectType_POLICY
	default:
		return ObjectType_INVALID
	}
}

// Bytes marshals the Object wrapper; it is a transport encoding, not the
// hashed payload (see Canonical).
func (o *Object) Bytes() []byte {
	return Bytes(o)
}

// NewObject wraps a Commit, Tree, Blob, File, Sealed, Pin or Policy; it panics on
// anything else.
func NewObject(in interface{}) *Object {
	var out isObject_Object

	switch t := in.(type) {
	case *Commit:
		out = &Object_Commit{t}
	case *Tree:
		out = &Object_Tree{t}
	case *Blob:
		out = &Object_Blob{t}
	case *File:
		out = &Object_File{t}
	case *Sealed:
		out = &Object_Sealed{t}
	case *Pin:
		out = &Object_Pin{t}
	case *Policy:
		out = &Object_Policy{t}
	default:
		panic("Unsupported object type")
	}

	return &Object{Object: out}
}

// NewObjectHeaderFromBytes decodes a marshaled ObjectHeader.
func NewObjectHeaderFromBytes(bytes []byte) (*ObjectHeader, error) {
	hdr := new(ObjectHeader)

	return hdr, pb.Unmarshal(bytes, hdr)
}

// NewObjectFromBytes decodes an Object wrapper written by Object.Bytes.
func NewObjectFromBytes(bytes []byte) (*Object, error) {
	obj := new(Object)

	return obj, pb.Unmarshal(bytes, obj)
}

// GetFileInfo converts stat data into a FileInfo. Owner names and the
// symlink target come from a FileInfo returned by Sys(), as WithDetails
// and GetOSFileInfo arrange.
func GetFileInfo(info os.FileInfo) *FileInfo {
	fi := &FileInfo{
		Name:    []byte(info.Name()),
		Mode:    uint32(info.Mode()),
		MtimeNs: info.ModTime().UnixNano(),
		Size:    info.Size(),
		Type:    NodeTypeOf(info.Mode()),
	}

	if details, ok := info.Sys().(*FileInfo); ok {
		fi.User = details.User
		fi.Group = details.Group
		fi.LinkTarget = details.LinkTarget
	}

	return fi
}

// WithDetails attaches owner names and a symlink target to stat data so
// GetFileInfo records them.
func WithDetails(info os.FileInfo, user, group, linkTarget string) os.FileInfo {
	return &detailedFileInfo{
		FileInfo: info,
		details:  &FileInfo{User: []byte(user), Group: []byte(group), LinkTarget: []byte(linkTarget)},
	}
}

type detailedFileInfo struct {
	os.FileInfo
	details *FileInfo
}

func (d *detailedFileInfo) Sys() interface{} {
	return d.details
}

// NodeTypeOf maps a file mode to the node type recorded in a tree.
func NodeTypeOf(mode os.FileMode) NodeType {
	switch {
	case mode.IsDir():
		return NodeType_NODE_DIRECTORY
	case mode&os.ModeSymlink != 0:
		return NodeType_NODE_SYMLINK
	default:
		return NodeType_NODE_FILE
	}
}

// IsDir reports whether the node describes a directory.
func (fi *FileInfo) IsDir() bool {
	return fi.GetType() == NodeType_NODE_DIRECTORY
}

// MetadataRefs lists the File objects the entry's security descriptor and
// streams are stored in.
func (fi *FileInfo) MetadataRefs() []*Ref {
	var refs []*Ref
	if fi.GetSecurity() != nil {
		refs = append(refs, fi.Security)
	}

	for _, stream := range fi.GetStreams() {
		if stream.GetRef() != nil {
			refs = append(refs, stream.Ref)
		}
	}

	return refs
}

// ModTime returns the recorded modification time.
func (fi *FileInfo) ModTime() time.Time {
	return time.Unix(0, fi.GetMtimeNs())
}

type backupFileInfo struct {
	*FileInfo
}

func (bi *backupFileInfo) IsDir() bool {
	return bi.FileInfo.IsDir()
}

func (bi *backupFileInfo) Name() string {
	return string(bi.FileInfo.Name)
}

func (bi *backupFileInfo) ModTime() time.Time {
	return bi.FileInfo.ModTime()
}

func (bi *backupFileInfo) Mode() os.FileMode {
	return os.FileMode(bi.FileInfo.Mode)
}

func (bi *backupFileInfo) Size() int64 {
	return bi.FileInfo.Size
}

// Sys returns the recorded FileInfo, so restore can read the symlink target
// and owner names.
func (bi *backupFileInfo) Sys() interface{} {
	return bi.FileInfo
}

// GetOSFileInfo presents a recorded FileInfo as an os.FileInfo whose Sys
// returns the FileInfo itself.
func GetOSFileInfo(info *FileInfo) os.FileInfo {
	return &backupFileInfo{info}
}

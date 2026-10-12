package backup

import (
	"strings"

	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/proto"

	pb "google.golang.org/protobuf/proto"
)

// sealInfo returns a copy of the stat with its name-like fields sealed for
// the directory whose token is parent. Without a key it is the identity.
func sealInfo(key *storekey.Key, parent []byte, info *proto.FileInfo) *proto.FileInfo {
	if key == nil {
		return info
	}

	sealed := pb.Clone(info).(*proto.FileInfo)
	sealed.Name = key.SealField(parent, storekey.FieldName, info.Name)
	sealed.User = key.SealField(parent, storekey.FieldUser, info.User)
	sealed.Group = key.SealField(parent, storekey.FieldGroup, info.Group)
	sealed.LinkTarget = key.SealField(parent, storekey.FieldTarget, info.LinkTarget)

	return sealed
}

// openInfo reverses sealInfo.
func openInfo(key *storekey.Key, parent []byte, info *proto.FileInfo) (*proto.FileInfo, error) {
	if key == nil {
		return info, nil
	}

	opened := pb.Clone(info).(*proto.FileInfo)
	var err error

	fields := []struct {
		field storekey.Field
		dst   *[]byte
		src   []byte
	}{
		{storekey.FieldName, &opened.Name, info.Name},
		{storekey.FieldUser, &opened.User, info.User},
		{storekey.FieldGroup, &opened.Group, info.Group},
		{storekey.FieldTarget, &opened.LinkTarget, info.LinkTarget},
	}

	for _, f := range fields {
		*f.dst, err = key.OpenField(parent, f.field, f.src)
		if err != nil {
			return nil, err
		}
	}

	return opened, nil
}

// NameToken is the stored form of a directory entry's name, which is the
// parent token of everything inside that directory.
func NameToken(key *storekey.Key, parent []byte, name []byte) []byte {
	if key == nil {
		return nil
	}

	return key.SealField(parent, storekey.FieldName, name)
}

// sealNodes returns copies of the nodes with sealed stats, in the order the
// canonical tree encoding requires.
func sealNodes(key *storekey.Key, parent []byte, nodes []*proto.TreeNode) []*proto.TreeNode {
	if key == nil {
		return SortNodes(nodes)
	}

	sealed := make([]*proto.TreeNode, len(nodes))
	for i, node := range nodes {
		sealed[i] = &proto.TreeNode{Stat: sealInfo(key, parent, node.Stat), Ref: node.Ref}
	}

	return SortNodes(sealed)
}

// openNodes returns copies of stored nodes with their stats opened.
func openNodes(key *storekey.Key, parent []byte, nodes []*proto.TreeNode) ([]*proto.TreeNode, error) {
	if key == nil {
		return nodes, nil
	}

	opened := make([]*proto.TreeNode, len(nodes))
	for i, node := range nodes {
		stat, err := openInfo(key, parent, node.Stat)
		if err != nil {
			return nil, err
		}

		opened[i] = &proto.TreeNode{Stat: stat, Ref: node.Ref}
	}

	return opened, nil
}

// IndexPath converts a slash-separated plaintext path into the path the
// index stores: name tokens computed top-down when the store is
// encrypted, escaped plain names otherwise.
func IndexPath(key *storekey.Key, path string) string {
	out, _ := indexPath(key, path)

	return out
}

// indexPath is IndexPath and the token of the directory path names, which
// the stats of its entries are sealed under.
func indexPath(key *storekey.Key, path string) (out string, parent []byte) {
	for _, part := range strings.Split(strings.Trim(path, "/"), "/") {
		if part == "" {
			continue
		}

		name := []byte(part)
		if key != nil {
			name = key.SealField(parent, storekey.FieldName, name)
			parent = name
		}

		out = proto.JoinPath(out, name)
	}

	return out, parent
}

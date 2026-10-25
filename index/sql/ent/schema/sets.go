// Package schema is the ent schema of the index: store settings, sets with
// their commits, versions, refs and retention state, pins, tombstoned refs
// and the archive index, in one database.
package schema

import (
	"github.com/gobackio/goback/proto"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Set is a backup set, with its retention policy and lifecycle state.
type Set struct {
	ent.Schema
}

// Fields of Set.
func (Set) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id"),
		field.String("name"),
		field.Enum("state").Values("active", "closing", "deleted").Default("active"),
		field.String("retention_policy").Optional().Nillable(),
		field.Bool("retention_paused").Default(false),
		field.Bool("erase").Default(false),
		field.Bool("rescan").Default(false),
		field.Int64("physical_size").Optional().Nillable(),
		field.Int64("deduplicated_size").Optional().Nillable(),
		field.Int64("alone_size").Optional().Nillable(),
		field.Int64("exclusive_size").Optional().Nillable(),
		field.Int64("deduplicated_alone_size").Optional().Nillable(),
		// what the set's live commits held when last measured: the newest
		// of them and every distinct file version they hold; sizes_digest
		// is the digest of those commits
		field.Int64("logical_size").Optional().Nillable(),
		field.Int64("unique_size").Optional().Nillable(),
		field.Bytes("sizes_digest").Optional(),
		// when the oldest tombstone whose refs and rows retirement has not
		// pruned yet was marked
		field.Time("prune_pending_since").Optional().Nillable(),
	}
}

// Edges of Set.
func (Set) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("files", File.Type),
		edge.To("trees", Tree.Type),
		edge.To("refs", SetRef.Type),
		edge.To("damaged", DamagedPath.Type),
	}
}

// Indexes of Set.
func (Set) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("name").Unique(),
		index.Fields("physical_size", "name").Annotations(entsql.DescColumns("physical_size")).
			StorageKey("sets_by_size"),
	}
}

// CommitRow is an indexed commit with its receipt order, presence filter,
// verified size and retention lifecycle: retire_at and expires_at are set
// by policy, deleted_at by an operator, tombstoned_at once the tombstone
// is durable; a live commit has none of them. (Commit is what ent calls
// ending a transaction.)
type CommitRow struct {
	ent.Schema
}

// Fields of CommitRow.
func (CommitRow) Fields() []ent.Field {
	return []ent.Field{
		field.Bytes("ref").Immutable(),
		field.Time("timestamp"),
		field.Time("received_at"),
		field.Bytes("tree"),
		field.Bytes("parent").Optional(),
		field.String("agent_id").Default(""),
		field.Int64("scan_start_ns").Default(0),
		field.Uint32("policy_version").Default(0),
		// the commit's key_id in hex: empty for a plain commit, null for one that
		// records none
		field.String("key_id").Optional().Nillable(),
		field.Bool("consistent").Default(false),
		field.Int64("set_id"),
		field.Bytes("presence").Optional(),
		field.Bool("partial").Default(false),
		// incomplete marks a commit a rebuild indexed around objects the
		// store no longer holds
		field.Bool("incomplete").Default(false),
		field.String("retained_by").Default(""),
		field.Time("retire_at").Optional().Nillable(),
		// the policy that retired the commit, as the set's retention_policy
		// keeps one
		field.String("retire_policy").Optional().Nillable(),
		field.Time("deleted_at").Optional().Nillable(),
		field.Time("expires_at").Optional().Nillable(),
		field.Time("tombstoned_at").Optional().Nillable(),
		field.Int64("logical_size").Optional().Nillable(),
		// how many files the set held at the commit, measured with
		// logical_size
		field.Int64("file_count").Optional().Nillable(),
		field.JSON("metadata", map[string]string{}).Optional(),
	}
}

// Edges of CommitRow.
func (CommitRow) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("set", Set.Type).Unique().Required().Field("set_id"),
	}
}

// Indexes of CommitRow.
func (CommitRow) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("ref").Unique(),
		index.Fields("set_id", "received_at"),
		index.Fields("set_id").Annotations(entsql.IndexWhere("presence IS NOT NULL")).StorageKey("commits_presence"),
		index.Fields("expires_at").Annotations(entsql.IndexWhere("tombstoned_at IS NULL AND expires_at IS NOT NULL")).StorageKey("commits_expiring"),
	}
}

// Annotations of CommitRow.
func (CommitRow) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "commits", Checks: map[string]string{
			"commits_ref_width":  "length(ref) = 32",
			"commits_tree_width": "length(tree) = 32",
		}},
	}
}

// File is one version of a path in a set, valid over the range of commits
// that contain it. A symlink has a link target and no ref.
type File struct {
	ent.Schema
}

// Fields of File.
func (File) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("set_id"),
		field.String("path"),
		field.String("dir"),
		field.Time("valid_from"),
		field.Time("valid_until").Optional().Nillable(),
		field.Bytes("ref").Optional(),
		field.Int64("mtime_ns"),
		field.Uint32("mode"),
		field.String("user"),
		field.String("group"),
		field.Int64("size"),
		field.Uint32("type").Default(uint32(proto.NodeType_NODE_FILE)),
		field.Bytes("link_target").Optional(),
		field.Bool("lost").Default(false),
	}
}

// Edges of File.
func (File) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("set", Set.Type).Ref("files").Unique().Required().Field("set_id"),
	}
}

// Indexes of File.
func (File) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("set_id", "path", "valid_from").Unique(),
		index.Fields("set_id", "dir").Annotations(entsql.IndexWhere("valid_until IS NULL")).StorageKey("files_open"),
		// a set's size is summed from this index alone
		index.Fields("set_id", "type", "size").Annotations(entsql.IndexWhere("valid_until IS NULL")).StorageKey("files_open_sizes"),
		index.Fields("set_id").Annotations(entsql.IndexWhere("lost AND valid_until IS NULL")).StorageKey("files_open_lost"),
		index.Fields("set_id", "ref", "size", "valid_from", "valid_until").Annotations(entsql.IndexWhere("ref IS NOT NULL")).
			StorageKey("files_versions"),
		// the versions of a file object, whatever set holds them
		index.Fields("ref").Annotations(entsql.IndexWhere("ref IS NOT NULL")).StorageKey("files_by_ref"),
		// what a directory held at any time
		index.Fields("set_id", "dir", "valid_until").StorageKey("files_by_dir"),
	}
}

// Annotations of File.
func (File) Annotations() []schema.Annotation {
	return []schema.Annotation{refWidth("files", "ref")}
}

// Tree is one version of a directory in a set, valid like a File.
type Tree struct {
	ent.Schema
}

// Fields of Tree.
func (Tree) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("set_id"),
		field.String("path"),
		field.String("dir"),
		field.Time("valid_from"),
		field.Time("valid_until").Optional().Nillable(),
		field.Bytes("ref"),
	}
}

// Edges of Tree.
func (Tree) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("set", Set.Type).Ref("trees").Unique().Required().Field("set_id"),
	}
}

// Indexes of Tree.
func (Tree) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("set_id", "path", "valid_from").Unique(),
		index.Fields("set_id", "dir").Annotations(entsql.IndexWhere("valid_until IS NULL")).StorageKey("trees_open"),
		index.Fields("set_id", "dir", "valid_until").StorageKey("trees_by_dir"),
	}
}

// Annotations of Tree.
func (Tree) Annotations() []schema.Annotation {
	return []schema.Annotation{refWidth("trees", "ref")}
}

// SetRef records that a set references a commit, tree or file object,
// which is what makes the object readable.
type SetRef struct {
	ent.Schema
}

// Fields of SetRef.
func (SetRef) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("set_id"),
		field.Bytes("ref"),
	}
}

// Edges of SetRef.
func (SetRef) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("set", Set.Type).Ref("refs").Unique().Required().Field("set_id"),
	}
}

// Indexes of SetRef.
func (SetRef) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("set_id", "ref").Unique(),
		index.Fields("ref"),
	}
}

// Annotations of SetRef.
func (SetRef) Annotations() []schema.Annotation {
	return []schema.Annotation{refWidth("set_refs", "ref")}
}

// refWidth is the check every ref column carries.
func refWidth(table, column string) schema.Annotation {
	return entsql.Annotation{Checks: map[string]string{
		table + "_" + column + "_width": "length(" + column + ") = 32",
	}}
}

func cascade() entsql.Annotation {
	return entsql.Annotation{OnDelete: entsql.Cascade}
}

// DamagedPath is a path whose stored content the store could not keep, so
// the next backup of its set reads it again instead of trusting that it is
// unchanged.
type DamagedPath struct {
	ent.Schema
}

// Fields of DamagedPath.
func (DamagedPath) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("set_id"),
		field.String("path"),
		field.Time("found_at"),
	}
}

// Edges of DamagedPath.
func (DamagedPath) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("set", Set.Type).Ref("damaged").Unique().Required().Field("set_id"),
	}
}

// Indexes of DamagedPath.
func (DamagedPath) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("set_id", "path").Unique(),
	}
}

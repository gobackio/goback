package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Settings is the one row of store settings: the write policy with its
// version and key acknowledgement, the retention defaults, and the
// sequence policy objects are numbered by.
type Settings struct {
	ent.Schema
}

// Fields of Settings.
func (Settings) Fields() []ent.Field {
	return []ent.Field{
		field.Int("id"),
		field.String("policy").Optional().Nillable(),
		field.Uint32("policy_version").Default(0),
		field.Time("key_acknowledged_at").Optional().Nillable(),
		field.String("retention_policy").Optional().Nillable(),
		field.Int("trash_days").Default(14),
		// the sequence of the last policy object the store wrote
		field.Uint64("policy_sequence").Default(0),
		// the now of the last retirement run
		field.Time("retired_at").Optional().Nillable(),
	}
}

// Pin caches a PIN object: the commit it keeps and when it was dropped.
type Pin struct {
	ent.Schema
}

// Fields of Pin.
func (Pin) Fields() []ent.Field {
	return []ent.Field{
		field.Bytes("ref").Immutable(),
		field.Bytes("target"),
		field.Time("received_at"),
		field.Time("deleted_at").Optional().Nillable(),
		// the labels the pin object carries; the store keeps them and
		// never interprets them
		field.JSON("metadata", map[string]string{}).Optional(),
	}
}

// Indexes of Pin.
func (Pin) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("ref").Unique(),
		index.Fields("target").Annotations(entsql.IndexWhere("deleted_at IS NULL")).StorageKey("pins_target"),
	}
}

// Annotations of Pin.
func (Pin) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Checks: map[string]string{
			"pins_ref_width":    "length(ref) = 32",
			"pins_target_width": "length(target) = 32",
		}},
	}
}

// DeletedRef is a ref a tombstone names: written before the tombstone is
// durable, consulted by every commit and pin Put.
type DeletedRef struct {
	ent.Schema
}

// Fields of DeletedRef.
func (DeletedRef) Fields() []ent.Field {
	return []ent.Field{
		field.Bytes("ref").Immutable(),
		field.Time("tombstoned_at"),
	}
}

// Indexes of DeletedRef.
func (DeletedRef) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("ref").Unique(),
	}
}

// Annotations of DeletedRef.
func (DeletedRef) Annotations() []schema.Annotation {
	return []schema.Annotation{refWidth("deleted_refs", "ref")}
}

// Reindex is a table's statistics counters as they stood when its
// indexes were last rebuilt.
type Reindex struct {
	ent.Schema
}

// Fields of Reindex.
func (Reindex) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").StorageKey("table_name"),
		// rows inserted, updated outside HOT and deleted, as counted
		field.Int64("churn"),
		// when the database's statistics were last reset, nil for never
		field.Time("stats_reset").Optional().Nillable(),
		field.Time("reindexed_at"),
	}
}

// Annotations of Reindex.
func (Reindex) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Table("reindexes")}
}

// WalkedArchive names a finished archive whose pins the index holds.
type WalkedArchive struct {
	ent.Schema
}

// Fields of WalkedArchive.
func (WalkedArchive) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").StorageKey("name"),
	}
}

package schema

import (
	"fmt"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/proto"
)

// Archive is a pack archive with its state and owning session: a pending
// archive is visible only to its session and goes with it.
type Archive struct {
	ent.Schema
}

// Fields of Archive.
func (Archive) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("session_id").Optional().Nillable(),
		field.Int("state").Default(0),
		// opened_at is when an open archive was claimed, by the index's
		// clock; it is cleared once the archive is finalized
		field.Time("opened_at").Optional().Nillable(),
		// created_at is when the bucket created the archive's index file
		field.Time("created_at").Optional().Nillable(),
	}
}

// Edges of Archive.
func (Archive) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("session", Session.Type).Ref("archives").Unique().Field("session_id").Annotations(cascade()),
		edge.To("objects", Object.Type).Annotations(cascade()),
	}
}

// Indexes of Archive.
func (Archive) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("session_id").Annotations(entsql.IndexWhere("session_id IS NOT NULL")).StorageKey("archives_session"),
	}
}

// Object locates one object within an archive.
type Object struct {
	ent.Schema
}

// Fields of Object.
func (Object) Fields() []ent.Field {
	return []ent.Field{
		field.Bytes("ref"),
		field.String("archive_id"),
		field.Uint32("start"),
		field.Uint32("length"),
		field.Uint32("type"),
		// the version a rewrite carried the object over with, unset for
		// one that has its archive's
		field.Int64("carried_time").Optional(),
		field.Uint32("carried_offset").Optional(),
		// incomplete is set on a copy that reaches content a commit marked
		// lost
		field.Bool("incomplete").Default(false),
	}
}

// Edges of Object.
func (Object) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("archive", Archive.Type).Ref("objects").Unique().Required().Field("archive_id"),
	}
}

// Indexes of Object.
func (Object) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("ref", "archive_id").Unique(),
		index.Fields("archive_id"),
		index.Fields("ref").Annotations(entsql.IndexWhere(fmt.Sprintf("type = %d", proto.ObjectType_TOMBSTONE))).StorageKey("objects_tombstones"),
		index.Fields("ref").Annotations(entsql.IndexWhere(fmt.Sprintf("type = %d", proto.ObjectType_COMMIT))).StorageKey("objects_commits"),
	}
}

// Annotations of Object.
func (Object) Annotations() []schema.Annotation {
	return []schema.Annotation{refWidth("objects", "ref")}
}

// Session is a live upload or restore session; its pending archives are
// visible to it alone and a restore's object is kept while it lives.
type Session struct {
	ent.Schema
}

// Fields of Session.
func (Session) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("agent_id"),
		field.String("backup_set"),
		field.Time("started_at"),
		field.Time("last_seen"),
		field.Bytes("restore_ref").Optional(),
		// the policy the session's set starts with if the session creates it
		field.JSON("initial_retention", &retention.Policy{}).Optional(),
	}
}

// Edges of Session.
func (Session) Edges() []ent.Edge {
	return []ent.Edge{
		edge.To("archives", Archive.Type).Annotations(cascade()),
	}
}

// SessionMarker is what a session began as and how it ended, kept until
// no archive of the session is stored, so a rebuild tells its archives
// committed or abandoned.
type SessionMarker struct {
	ent.Schema
}

// Fields of SessionMarker.
func (SessionMarker) Fields() []ent.Field {
	return []ent.Field{
		field.String("id"),
		field.String("agent_id"),
		field.String("backup_set"),
		field.Bytes("restore").Optional(),
		field.Time("started"),
		field.Uint64("sealed").Default(0),
		field.String("outcome").Optional().Nillable(),
		field.Time("ended_at").Optional().Nillable(),
	}
}

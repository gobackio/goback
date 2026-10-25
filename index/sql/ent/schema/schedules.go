package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// Schedule is what an agent last reported it expects of a set, by the
// set's name, which needs no set of that name yet.
type Schedule struct {
	ent.Schema
}

// Fields of Schedule.
func (Schedule) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").StorageKey("set_name"),
		field.String("agent_id"),
		field.String("kind"),
		field.Int64("longest_gap").GoType(time.Duration(0)),
		field.Int64("jitter").GoType(time.Duration(0)),
		field.Int64("commit_every").GoType(time.Duration(0)),
		// when agent_id first reported the set
		field.Time("since"),
		field.Time("reported_at"),
	}
}

// Indexes of Schedule.
func (Schedule) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("agent_id"),
		index.Fields("reported_at"),
	}
}

// Annotations of Schedule.
func (Schedule) Annotations() []schema.Annotation {
	return []schema.Annotation{entsql.Table("schedules")}
}

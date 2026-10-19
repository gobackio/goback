// Package mapping turns what the store service carries into goback's own
// types and back.
package mapping

import (
	"time"

	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"
)

//go:generate mapper .

// mapper:generate
type Mapper interface {
	StorePolicy(in *storekey.Policy) *proto.StorePolicy
	FromStorePolicy(in *proto.StorePolicy) *storekey.Policy

	// field:KeepWithin using:"Seconds"
	RetentionPolicy(in *retention.Policy) *proto.RetentionPolicy
	// field:KeepWithin using:"Duration"
	FromRetentionPolicy(in *proto.RetentionPolicy) *retention.Policy

	SetRetention(in index.SetRetention) *proto.GetRetentionResponse
	FromSetRetention(in *proto.GetRetentionResponse) index.SetRetention

	// Effective is a policy in force, which is never unset.
	// field:KeepWithin using:"Seconds"
	Effective(in retention.Policy) *proto.RetentionPolicy
	// FromEffective is Effective's inverse.
	// field:KeepWithin using:"Duration"
	FromEffective(in *proto.RetentionPolicy) retention.Policy

	RetentionBracket(in retention.Bracket) *proto.RetentionBracket
	FromRetentionBracket(in *proto.RetentionBracket) retention.Bracket
}

// Seconds is d in whole seconds, as the store service carries durations.
func Seconds(d time.Duration) int64 {
	return int64(d / time.Second)
}

// Duration is a count of seconds as a duration.
func Duration(seconds int64) time.Duration {
	return time.Duration(seconds) * time.Second
}

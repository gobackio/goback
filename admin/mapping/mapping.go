// Package mapping turns what the index reports into the admin protos.
package mapping

import (
	"math"
	"time"

	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index"
	pb "github.com/gobackio/goback/proto/admin"
)

//go:generate mapper .

// mapper:generate
type Mapper interface {
	Set(in index.SetInfo) *pb.BackupSet

	// field:Version from:"Policy.Version"
	// field:Mode from:"Policy.Mode"
	// field:SizeThreshold from:"Policy.SizeThreshold"
	// field:EntropyEstimator from:"Policy.EntropyEstimator"
	// field:EntropyThreshold from:"Policy.EntropyThreshold"
	// field:PresenceScope from:"Policy.PresenceScope"
	Policy(in index.StorePolicy) *pb.StorePolicy

	// field:Version from:"-"
	WritePolicy(in *pb.SetStorePolicyRequest) storekey.Policy

	// field:KeepWithin using:"Seconds"
	Retention(in retention.Policy) *pb.RetentionPolicy

	// field:KeepWithin using:"Duration"
	FromRetention(in *pb.RetentionPolicy) retention.Policy

	Bracket(in retention.Bracket) *pb.RetentionBracket
	FromBracket(in *pb.RetentionBracket) retention.Bracket
}

// Seconds is d in whole seconds, as the admin protos carry durations.
func Seconds(d time.Duration) int64 {
	return int64(d / time.Second)
}

// Duration is a count of seconds as a duration; a count past what a
// duration holds is the longest one, or the shortest.
func Duration(seconds int64) time.Duration {
	const most = math.MaxInt64 / int64(time.Second)

	switch {
	case seconds >= most:
		return math.MaxInt64
	case seconds <= -most:
		return math.MinInt64
	}

	return time.Duration(seconds) * time.Second
}

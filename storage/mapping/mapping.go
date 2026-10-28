// Package mapping turns what the store service carries into goback's own
// types and back.
package mapping

import (
	"math"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/retention"
	"github.com/gobackio/goback/backup/schedule"
	"github.com/gobackio/goback/backup/storekey"
	"github.com/gobackio/goback/index"
	"github.com/gobackio/goback/proto"
)

//go:generate go tool mapper .

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

	// field:UploadId from:"ID"
	// field:Commit using:"Encoded"
	// field:Commits using:"EncodedAll"
	// field:CommitsMissing using:"MissingRefs"
	UploadStatus(in backup.UploadStatus) (*proto.UploadStatus, error)
	// field:ID from:"UploadId"
	// field:Commit using:"Decoded"
	// field:Commits using:"DecodedAll"
	// field:CommitsMissing using:"FromMissingRefs"
	FromUploadStatus(in *proto.UploadStatus) (backup.UploadStatus, error)

	// field:AgentId from:"Agent"
	ReportSchedule(in schedule.Report) *proto.ReportScheduleRequest
	// field:Agent from:"AgentId"
	FromReportSchedule(in *proto.ReportScheduleRequest) schedule.Report

	// field:BackupSet from:"Set"
	// field:LongestGapSeconds from:"LongestGap" using:"Seconds"
	// field:JitterSeconds from:"Jitter" using:"Seconds"
	// field:CommitEverySeconds from:"CommitEvery" using:"Seconds"
	SetSchedule(in schedule.Expectation) *proto.SetSchedule
	// field:Set from:"BackupSet"
	// field:LongestGap from:"LongestGapSeconds" using:"Duration"
	// field:Jitter from:"JitterSeconds" using:"Duration"
	// field:CommitEvery from:"CommitEverySeconds" using:"Duration"
	FromSetSchedule(in *proto.SetSchedule) schedule.Expectation
}

// Encoded is an object as the store service carries it, nil for none.
func Encoded(object *proto.Object) (*proto.Stored, error) {
	if object == nil {
		return nil, nil
	}

	return object.Encoded()
}

// EncodedAll is Encoded of each of objects.
func EncodedAll(objects []*proto.Object) ([]*proto.Stored, error) {
	if objects == nil {
		return nil, nil
	}

	out := make([]*proto.Stored, len(objects))
	for i, object := range objects {
		stored, err := object.Encoded()
		if err != nil {
			return nil, err
		}

		out[i] = stored
	}

	return out, nil
}

// Decoded is Encoded's inverse.
func Decoded(stored *proto.Stored) (*proto.Object, error) {
	if stored == nil {
		return nil, nil
	}

	return stored.Object()
}

// DecodedAll is EncodedAll's inverse.
func DecodedAll(stored []*proto.Stored) ([]*proto.Object, error) {
	if stored == nil {
		return nil, nil
	}

	out := make([]*proto.Object, len(stored))
	for i, s := range stored {
		object, err := s.Object()
		if err != nil {
			return nil, err
		}

		out[i] = object
	}

	return out, nil
}

// MissingRefs is what each of several commits lacks, as the store service
// carries it.
func MissingRefs(refs [][]*proto.Ref) []*proto.MissingRefs {
	if refs == nil {
		return nil
	}

	out := make([]*proto.MissingRefs, len(refs))
	for i, r := range refs {
		out[i] = &proto.MissingRefs{Refs: r}
	}

	return out
}

// FromMissingRefs is MissingRefs's inverse.
func FromMissingRefs(refs []*proto.MissingRefs) [][]*proto.Ref {
	if refs == nil {
		return nil
	}

	out := make([][]*proto.Ref, len(refs))
	for i, r := range refs {
		out[i] = r.GetRefs()
	}

	return out
}

// Seconds is d in whole seconds, as the store service carries durations.
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

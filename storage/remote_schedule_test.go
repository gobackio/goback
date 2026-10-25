package storage

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/backup/schedule"
	"github.com/gobackio/goback/proto"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// reporting is a memIndex that keeps the schedule reports it is given.
type reporting struct {
	*memIndex
	reports []schedule.Report
}

func (r *reporting) ReportSchedule(_ context.Context, report schedule.Report) error {
	r.reports = append(r.reports, report)
	return nil
}

// reaching is a reporting index whose callers reach only the sets named.
type reaching struct {
	*reporting
	sets []string
}

func (r reaching) Reaches(_ context.Context, set string) (bool, error) {
	return slices.Contains(r.sets, set), nil
}

var (
	dailyWorld = schedule.Expectation{Set: "world", Kind: schedule.Cron, LongestGap: 72 * time.Hour, Jitter: time.Minute}
	walLog     = schedule.Expectation{Set: "db/wal", Kind: schedule.Receiver, CommitEvery: time.Minute}
)

func TestAReportReachesTheIndexAsTheAgentSentIt(t *testing.T) {
	index := &reporting{memIndex: newMemIndex()}
	client := startServerWith(t, index, nil)("node-1")

	report := schedule.Report{Agent: "node-1", Sets: []schedule.Expectation{dailyWorld, walLog}}
	require.NoError(t, client.ReportSchedule(context.Background(), report))
	require.Equal(t, []schedule.Report{report}, index.reports)
}

func TestAnAgentReportsOnlyForItself(t *testing.T) {
	index := &reporting{memIndex: newMemIndex()}
	client := startServerWith(t, index, nil)("node-1")

	err := client.ReportSchedule(context.Background(), schedule.Report{Agent: "node-2", Sets: []schedule.Expectation{dailyWorld}})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Empty(t, index.reports)
}

func TestAKeyScopedToSetsReportsOnlyForThoseItReaches(t *testing.T) {
	index := reaching{reporting: &reporting{memIndex: newMemIndex()}, sets: []string{"world"}}
	client := startServerWith(t, index, nil)("node-1")
	ctx := context.Background()

	err := client.ReportSchedule(ctx, schedule.Report{Agent: "node-1", Sets: []schedule.Expectation{dailyWorld, walLog}})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Empty(t, index.reports)

	require.NoError(t, client.ReportSchedule(ctx, schedule.Report{Agent: "node-1", Sets: []schedule.Expectation{dailyWorld}}))
	require.Len(t, index.reports, 1)
}

func TestAReportNoPlanMakesIsInvalid(t *testing.T) {
	index := &reporting{memIndex: newMemIndex()}
	client := startServerWith(t, index, nil)("node-1")

	err := client.ReportSchedule(context.Background(), schedule.Report{Agent: "node-1", Sets: []schedule.Expectation{{Set: "world", Kind: "hourly"}}})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Empty(t, index.reports)
}

func TestAStoreWhoseIndexTakesNoReportsDoesNotSupportThem(t *testing.T) {
	client := startServerWith(t, newMemIndex(), nil)("node-1")

	err := client.ReportSchedule(context.Background(), schedule.Report{Agent: "node-1", Sets: []schedule.Expectation{dailyWorld}})
	require.ErrorIs(t, err, backup.ErrNotImplemented)
}

func TestAServerWithoutTheCallDoesNotSupportReports(t *testing.T) {
	remote := NewServer(NewStore(&reporting{memIndex: newMemIndex()}, nil))

	older := proto.Store_ServiceDesc
	older.Methods = slices.DeleteFunc(slices.Clone(older.Methods), func(m grpc.MethodDesc) bool { return m.MethodName == "ReportSchedule" })

	dial := serveRemote(t, remote, nil, func(s *grpc.Server) { s.RegisterService(&older, remote) })

	err := dial(testSecret, "node-1").ReportSchedule(context.Background(), schedule.Report{Agent: "node-1", Sets: []schedule.Expectation{dailyWorld}})
	require.ErrorIs(t, err, backup.ErrNotImplemented)
}

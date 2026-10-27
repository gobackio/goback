package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/index/sql/ent"
	"github.com/gobackio/goback/index/sql/ent/sessionmarker"
	"github.com/gobackio/goback/storage/pack"
)

var _ pack.SessionMarkers = (*Index)(nil)

// MarkBegun implements pack.SessionMarkers.
func (x *Index) MarkBegun(begin pack.SessionBegin) error {
	err := x.client.SessionMarker.Create().SetID(begin.ID).SetAgentID(begin.AgentID).SetBackupSet(begin.Set).
		SetRestore(begin.Restore).SetStarted(begin.Started.UTC()).SetSealed(begin.Sealed).Exec(context.Background())
	if ent.IsConstraintError(err) {
		return pack.ErrFileExists
	}

	return err
}

// MarkEnded implements pack.SessionMarkers, ending the session by the
// index's clock.
func (x *Index) MarkEnded(id string, want pack.SessionOutcome) (pack.SessionOutcome, error) {
	ctx := context.Background()

	now, err := x.clock(ctx)
	if err != nil {
		return "", err
	}

	n, err := x.client.SessionMarker.Update().Where(sessionmarker.ID(id), sessionmarker.OutcomeIsNil()).
		SetOutcome(string(want)).SetEndedAt(now).Save(ctx)
	if err != nil {
		return "", err
	}

	if n == 1 {
		return want, nil
	}

	outcome, ended, err := x.SessionEnd(id)
	if err != nil {
		return "", err
	}

	if !ended {
		return "", fmt.Errorf("session %s has no markers: %w", id, backup.ErrNoSession)
	}

	return outcome, nil
}

// SessionEnd implements pack.SessionMarkers.
func (x *Index) SessionEnd(id string) (pack.SessionOutcome, bool, error) {
	row, err := x.client.SessionMarker.Get(context.Background(), id)
	if ent.IsNotFound(err) {
		return "", false, nil
	}

	if err != nil || row.Outcome == nil {
		return "", false, err
	}

	return pack.SessionOutcome(*row.Outcome), true, nil
}

// SessionBegin implements pack.SessionMarkers.
func (x *Index) SessionBegin(id string) (*pack.SessionBegin, error) {
	row, err := x.client.SessionMarker.Get(context.Background(), id)
	if ent.IsNotFound(err) {
		return nil, pack.ErrFileNotFound
	}

	if err != nil {
		return nil, err
	}

	begin := m.SessionBegin(row)

	return &begin, nil
}

// Begun implements pack.SessionMarkers.
func (x *Index) Begun() (map[string]bool, error) {
	ids, err := x.client.SessionMarker.Query().IDs(context.Background())
	if err != nil {
		return nil, err
	}

	begun := make(map[string]bool, len(ids))
	for _, id := range ids {
		begun[id] = true
	}

	return begun, nil
}

// Ended implements pack.SessionMarkers.
func (x *Index) Ended() (map[string]time.Time, error) {
	rows, err := x.client.SessionMarker.Query().Where(sessionmarker.OutcomeNotNil()).
		Select(sessionmarker.FieldID, sessionmarker.FieldEndedAt).All(context.Background())
	if err != nil {
		return nil, err
	}

	ended := make(map[string]time.Time, len(rows))
	for _, row := range rows {
		ended[row.ID] = *row.EndedAt
	}

	return ended, nil
}

// DeleteMarkers implements pack.SessionMarkers.
func (x *Index) DeleteMarkers(id string) error {
	_, err := x.client.SessionMarker.Delete().Where(sessionmarker.ID(id)).Exec(context.Background())

	return err
}

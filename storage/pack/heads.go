package pack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/gobackio/goback/backup"
	"github.com/gobackio/goback/proto"
)

// HeadExt names the pointers to each set's latest commit:
// sets/<set id>.head.
const HeadExt = ".head"

// Head is the latest complete commit of a set the store holds, as kept in
// the storage beside the archives, so the set's backups are found without
// any index.
type Head struct {
	SetID        uint64 `json:"setId"`
	Set          string `json:"set"`
	Commit       []byte `json:"commit"`
	ReceivedAtNs int64  `json:"receivedAtNs"`
}

func headName(setID uint64) string {
	return fmt.Sprintf("sets/%d%s", setID, HeadExt)
}

// Heads lists the head of every set that has committed.
func (ps *PackStorage) Heads() ([]Head, error) {
	return headsOf(ps.storage, ps.logger)
}

// HeadsOf lists the heads a store's storage holds, without opening the
// store. A head that does not decode is logged and left out.
func HeadsOf(storage ArchiveStorage) ([]Head, error) {
	return headsOf(storage, slog.Default())
}

func headsOf(storage ArchiveStorage, logger *slog.Logger) ([]Head, error) {
	names, err := storage.List(HeadExt)
	if err != nil {
		return nil, err
	}

	var heads []Head
	for _, name := range names {
		id, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "sets/"), HeadExt), 10, 64)
		if err != nil {
			continue
		}

		head, ok, err := readHead(storage, id)
		if errors.Is(err, errHeadUnreadable) {
			logger.Warn("leaving out a head that does not decode", "set", id, "err", err)
			continue
		}

		if err != nil {
			return nil, err
		}

		if ok {
			heads = append(heads, head)
		}
	}

	return heads, nil
}

// errHeadUnreadable is a head file whose bytes do not decode.
var errHeadUnreadable = errors.New("head does not decode")

func readHead(storage ArchiveStorage, setID uint64) (Head, bool, error) {
	file, err := storage.Open(headName(setID))
	if notExist(err) {
		return Head{}, false, nil
	}

	if err != nil {
		return Head{}, false, err
	}
	defer file.Close()

	var head Head
	if err := json.NewDecoder(file).Decode(&head); err != nil {
		return Head{}, false, fmt.Errorf("reading the head of set %d: %w: %w", setID, errHeadUnreadable, err)
	}

	return head, true, nil
}

var _ backup.HeadKeeper = (*PackStorage)(nil)

// AdvanceHead implements backup.HeadKeeper. Checkpoints never become
// heads.
func (ps *PackStorage) AdvanceHead(object *proto.Object) error {
	commit := object.GetCommit()
	if commit == nil || commit.GetPartial() || commit.GetSetId() == 0 {
		return nil
	}

	held := &ps.headMtx[commit.GetSetId()%uint64(len(ps.headMtx))]
	held.Lock()
	defer held.Unlock()

	if ps.headsFor != nil && !ps.headsFor(commit.GetSetId()) {
		err := ps.storage.Delete(headName(commit.GetSetId()))
		if notExist(err) {
			return nil
		}

		return err
	}

	// a head that does not decode is replaced
	current, ok, err := readHead(ps.storage, commit.GetSetId())
	if errors.Is(err, errHeadUnreadable) {
		ps.logger.Warn("replacing a head that does not decode", "set", commit.GetSetId(), "err", err)
	} else if err != nil {
		return err
	}

	if ok && current.ReceivedAtNs >= commit.GetReceivedAtNs() {
		return nil
	}

	return ps.writeHead(object)
}

// moveHead points the set of replacement at it where the set's head names
// old.
func (ps *PackStorage) moveHead(old *proto.Ref, replacement *proto.Object) error {
	commit := replacement.GetCommit()
	if commit.GetSetId() == 0 {
		return nil
	}

	held := &ps.headMtx[commit.GetSetId()%uint64(len(ps.headMtx))]
	held.Lock()
	defer held.Unlock()

	current, ok, err := readHead(ps.storage, commit.GetSetId())
	if err != nil || !ok || !bytes.Equal(current.Commit, old.Hash) {
		return err
	}

	return ps.writeHead(replacement)
}

// writeHead points the commit's set at it.
func (ps *PackStorage) writeHead(object *proto.Object) error {
	commit := object.GetCommit()

	data, err := json.Marshal(Head{SetID: commit.GetSetId(), Set: commit.GetBackupSet(), Commit: object.Ref().Hash, ReceivedAtNs: commit.GetReceivedAtNs()})
	if err != nil {
		return err
	}

	file, err := ps.storage.Create(headName(commit.GetSetId()))
	if err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}

	return file.Close()
}

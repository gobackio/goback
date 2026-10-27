package pack

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// SealExt names the seals collections leave in the storage:
// <generation>.seal holds the versions of the tombstones that generation
// condemned with. A session that commits checks what it relied on against
// the seals written since it began.
const SealExt = ".seal"

type seal struct {
	Generation uint64      `json:"generation"`
	Condemned  []time.Time `json:"condemned"`
}

func sealName(generation uint64) string {
	return fmt.Sprintf("%020d%s", generation, SealExt)
}

func (ps *PackStorage) writeSeal(s seal) error {
	file, err := ps.storage.Create(sealName(s.Generation))
	if err != nil {
		return err
	}

	if err := json.NewEncoder(file).Encode(s); err != nil {
		_ = file.Close()
		return err
	}

	if err := file.Close(); err != nil {
		return err
	}

	ps.sawSeal(s.Generation)

	return nil
}

// sawSeal notes generation as one that left a seal.
func (ps *PackStorage) sawSeal(generation uint64) {
	for seen := ps.sealSeen.Load(); generation > seen && !ps.sealSeen.CompareAndSwap(seen, generation); seen = ps.sealSeen.Load() {
	}
}

// sealGenerations lists the generations that left a seal, oldest first.
func (ps *PackStorage) sealGenerations() ([]uint64, error) {
	names, err := ps.storage.List(SealExt)
	if err != nil {
		return nil, err
	}

	var generations []uint64
	for _, name := range names {
		generation, err := strconv.ParseUint(strings.TrimSuffix(name, SealExt), 10, 64)
		if err != nil {
			continue
		}

		generations = append(generations, generation)
	}

	slices.Sort(generations)

	if len(generations) > 0 {
		ps.sawSeal(generations[len(generations)-1])
	}

	return generations, nil
}

// lastSeal returns the newest generation that left a seal, 0 for none.
func (ps *PackStorage) lastSeal() (uint64, error) {
	generations, err := ps.sealGenerations()
	if err != nil || len(generations) == 0 {
		return 0, err
	}

	return generations[len(generations)-1], nil
}

// sealedSince returns the versions of the tombstones the seals newer than
// generation condemned with.
func (ps *PackStorage) sealedSince(generation uint64) (map[int64]bool, error) {
	generations, err := ps.sealGenerations()
	if err != nil {
		return nil, err
	}

	sealed := make(map[int64]bool)

	for _, g := range generations {
		if g <= generation {
			continue
		}

		file, err := ps.storage.Open(sealName(g))
		if err != nil {
			return nil, fmt.Errorf("reading seal %d: %w", g, err)
		}

		var s seal
		err = json.NewDecoder(file).Decode(&s)
		_ = file.Close()

		if err != nil {
			return nil, fmt.Errorf("reading seal %d: %w", g, err)
		}

		for _, t := range s.Condemned {
			sealed[t.UnixNano()] = true
		}
	}

	return sealed, nil
}

// pruneSeals removes the seals no live session needs: those every live
// session began after.
func (ps *PackStorage) pruneSeals(live map[string]bool) error {
	generations, err := ps.sealGenerations()
	if err != nil || len(generations) == 0 {
		return err
	}

	oldest := generations[len(generations)-1]

	for id := range live {
		marker, err := ps.markers.SessionBegin(id)
		if err != nil {
			// a session without its begin may need every seal
			return nil
		}

		oldest = min(oldest, marker.Sealed)
	}

	for _, g := range generations {
		if g > oldest {
			break
		}

		if err := ps.storage.Delete(sealName(g)); err != nil && !notExist(err) {
			return err
		}
	}

	return nil
}

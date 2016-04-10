package storage

import (
	"fmt"
	"regexp"

	"github.com/chronos-tachyon/go-raft/internal/util"
	pb "github.com/chronos-tachyon/go-raft/proto"
)

var reSnapshot = regexp.MustCompile(`^snapshot\.(\d+)$`)

func IsSnapshotFile(name string) bool {
	return reSnapshot.MatchString(name)
}

func LoadSnapshot(storage Storage, snapver uint64) ([]byte, error) {
	var snapshot []byte
	if snapver > 0 {
		name := fmt.Sprintf("snapshot.%08d", snapver)
		var tmp pb.Snapshot
		err := readRecordFromFile(storage, name, &tmp)
		if err != nil {
			return nil, err
		}
		snapshot = tmp.Payload
	}
	return snapshot, nil
}

func SaveSnapshot(storage Storage, snapver uint64, snapshot []byte) error {
	util.Assert(snapver > 0, "SnapshotVersion must be > 0")
	name := fmt.Sprintf("snapshot.%08d", snapver)
	var tmp pb.Snapshot
	tmp.Payload = snapshot
	return writeRecordToFile(storage, name, &tmp)
}

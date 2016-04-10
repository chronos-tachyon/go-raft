package storage

import (
	"fmt"
	"regexp"

	"github.com/chronos-tachyon/go-raft/internal/util"
	pb "github.com/chronos-tachyon/go-raft/proto"
)

const numMetadataFiles = 5

var reMetadata = regexp.MustCompile(`^metadata\.([01234])$`)

func IsMetadataFile(name string) bool {
	return reMetadata.MatchString(name)
}

func LoadMetadata(storage Storage, meta *pb.Metadata) error {
	*meta = pb.Metadata{StartIndex: 1}
	for i := 0; i < numMetadataFiles; i++ {
		name := fmt.Sprintf("metadata.%d", i)
		var tmp pb.Metadata
		if err := readRecordFromFile(storage, name, &tmp); err != nil {
			if storage.IsNotExist(err) {
				continue
			}
			return err
		}
		if tmp.MetadataVersion > meta.MetadataVersion {
			*meta = tmp
		}
	}
	if meta.StartIndex != 1 && meta.SnapshotVersion == 0 {
		return fmt.Errorf("StartIndex %d but no snapshot", meta.StartIndex)
	}
	return nil
}

func SaveMetadata(storage Storage, meta *pb.Metadata) error {
	util.Assert(meta.MetadataVersion > 0, "MetadataVersion must be > 0")
	i := int(meta.MetadataVersion % numMetadataFiles)
	name := fmt.Sprintf("metadata.%d", i)
	err := storage.Delete(name)
	if err != nil && !storage.IsNotExist(err) {
		return err
	}
	return writeRecordToFile(storage, name, meta)
}

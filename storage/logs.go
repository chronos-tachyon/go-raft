package storage

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"

	"github.com/chronos-tachyon/go-raft/internal/multierror"
	"github.com/chronos-tachyon/go-raft/internal/util"
	pb "github.com/chronos-tachyon/go-raft/proto"
)

var reClosedSegment = regexp.MustCompile(`^log\.(\d+)-(\d+)$`)
var reOpenSegment = regexp.MustCompile(`^log\.(\d+)-open$`)

const (
	patClosedSegment = "log.%08d-%08d"
	patOpenSegment   = "log.%08d-open"
)

func IsClosedLogFile(name string) bool {
	match := reClosedSegment.FindStringSubmatch(name)
	if match == nil {
		return false
	}
	_, err1 := strconv.ParseUint(match[1], 10, 64)
	_, err2 := strconv.ParseUint(match[2], 10, 64)
	return err1 == nil && err2 == nil
}

func IsOpenLogFile(name string) bool {
	match := reOpenSegment.FindStringSubmatch(name)
	if match == nil {
		return false
	}
	_, err := strconv.ParseUint(match[1], 10, 64)
	return err == nil
}

type LogSegment struct {
	Name         string
	WasOpen      bool
	PermitRename bool
	FirstIndex   uint64
	LastIndex    uint64
	NextIndex    uint64
	Storage      Storage
	File         File
}

func NewLogSegment(storage Storage, firstIndex uint64) *LogSegment {
	return &LogSegment{
		Name:         fmt.Sprintf(patOpenSegment, firstIndex),
		WasOpen:      true,
		PermitRename: true,
		FirstIndex:   firstIndex,
		LastIndex:    0,
		NextIndex:    firstIndex,
		Storage:      storage,
	}
}

func ScanLogSegments(storage Storage) ([]*LogSegment, error) {
	names, err := storage.List()
	sort.Sort(util.ByVersion(names))
	var segments []*LogSegment
	for _, name := range names {
		match := reClosedSegment.FindStringSubmatch(name)
		if match != nil {
			p, err1 := strconv.ParseUint(match[1], 10, 64)
			q, err2 := strconv.ParseUint(match[2], 10, 64)
			if err1 == nil && err2 == nil && p != 0 && q >= p {
				segments = append(segments, &LogSegment{
					Name:       name,
					WasOpen:    false,
					FirstIndex: p,
					LastIndex:  q,
					NextIndex:  p,
					Storage:    storage,
				})
			}
			continue
		}
		match = reOpenSegment.FindStringSubmatch(name)
		if match != nil {
			p, err1 := strconv.ParseUint(match[1], 10, 64)
			if err1 == nil && p != 0 {
				segments = append(segments, &LogSegment{
					Name:         name,
					WasOpen:      true,
					PermitRename: true,
					FirstIndex:   p,
					LastIndex:    p - 1,
					NextIndex:    p,
					Storage:      storage,
				})
			}
			continue
		}
	}
	return segments, err
}

func (seg *LogSegment) Open() error {
	var err error
	seg.File, err = seg.Storage.Open(seg.Name, ReadOnly)
	return err
}

func (seg *LogSegment) Read() (*pb.LogEntry, error) {
	var e pb.LogEntry
	err := ReadRecord(seg.File, &e)
	if err != nil {
		seg.PermitRename = false
		return nil, err
	}
	if e.Index != seg.NextIndex {
		err = fmt.Errorf("index mismatch: expected %d, got %d", seg.NextIndex, e.Index)
		seg.PermitRename = false
		seg.NextIndex = e.Index + 1
		return nil, err
	}
	seg.NextIndex++
	return &e, nil
}

func (seg *LogSegment) Create() error {
	var err error
	seg.File, err = seg.Storage.Open(seg.Name, WriteOnly)
	return err
}

func (seg *LogSegment) Append(entries []*pb.LogEntry) (int, error) {
	var numWritten int
	var errs []error
	for _, entry := range entries {
		var dupe pb.LogEntry
		dupe = *entry
		util.Assert(dupe.Index == 0 || dupe.Index == seg.NextIndex, "index mismatch: expected %d, got %d", seg.NextIndex, dupe.Index)
		dupe.Index = seg.NextIndex
		_, err := WriteRecord(seg.File, &dupe)
		errs = append(errs, err)
		if err != nil {
			break
		}
		seg.NextIndex++
		numWritten++
	}
	errs = append(errs, seg.File.Sync())
	return numWritten, multierror.New(errs)
}

func (seg *LogSegment) Close() error {
	var err1, err2, err3 error
	err1 = seg.File.Sync()
	if seg.WasOpen && seg.PermitRename {
		seg.LastIndex = seg.NextIndex - 1
		if seg.LastIndex >= seg.FirstIndex {
			newname := fmt.Sprintf(patClosedSegment, seg.FirstIndex, seg.LastIndex)
			err2 = seg.Storage.Rename(seg.Name, newname)
		} else {
			err2 = seg.Storage.Delete(seg.Name)
		}
	}
	err3 = seg.File.Close()
	return multierror.Of(err1, err2, err3)
}

func (seg *LogSegment) Delete() error {
	return seg.Storage.Delete(seg.Name)
}

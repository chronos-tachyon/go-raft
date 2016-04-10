package storage

import (
	"sort"

	pb "github.com/chronos-tachyon/go-raft/proto"
)

type byIndex []*pb.LogEntry

func (x byIndex) Len() int           { return len(x) }
func (x byIndex) Less(i, j int) bool { return x[i].Index < x[j].Index }
func (x byIndex) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

var _ sort.Interface = byIndex(nil)

package server

import (
	"sort"
)

type byId []*peer

func (x byId) Len() int           { return len(x) }
func (x byId) Less(i, j int) bool { return x[i].id < x[j].id }
func (x byId) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

var _ sort.Interface = byId(nil)

func (x byId) Reverse() {
	i := 0
	j := len(x) - 1
	for i < j {
		x.Swap(i, j)
		i++
		j--
	}
}

type byIndex []uint64

func (x byIndex) Len() int           { return len(x) }
func (x byIndex) Less(i, j int) bool { return x[i] < x[j] }
func (x byIndex) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

var _ sort.Interface = byIndex(nil)

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

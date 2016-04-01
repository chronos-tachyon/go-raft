package raft

import (
	"fmt"
	"hash/crc32"
	"net"
	"sort"
)

type byId []PeerId

func (x byId) Len() int           { return len(x) }
func (x byId) Less(i, j int) bool { return x[i] < x[j] }
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

type byIndex []index

func (x byIndex) Len() int           { return len(x) }
func (x byIndex) Less(i, j int) bool { return x[i] < x[j] }
func (x byIndex) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

var _ sort.Interface = byIndex(nil)

func assert(ok bool, pat string, args ...interface{}) {
	if !ok {
		panic(fmt.Errorf(pat, args...))
	}
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func crc(data []byte) uint32 {
	raw := crc32.Checksum(data, castagnoliTable)
	return ((raw >> 15) | (raw << 17)) + 0xa282ead8
}

func equalUDPAddr(a, b *net.UDPAddr) bool {
	return a.IP.Equal(b.IP) && a.Port == b.Port && a.Zone == b.Zone
}

func minIndex(list ...index) index {
	least := list[0]
	for _, index := range list {
		if index < least {
			least = index
		}
	}
	return least
}

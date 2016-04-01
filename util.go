package raft

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"sort"

	"github.com/golang/protobuf/proto"
)

type byId []uint32

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

type byIndex []uint64

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

func min(list ...uint64) uint64 {
	least := list[0]
	for _, x := range list {
		if x < least {
			least = x
		}
	}
	return least
}

func mustUnmarshalProto(data []byte, msg proto.Message) {
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}
}

func packData(msg proto.Message) ([]byte, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	n := len(data)
	data = append(data, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(data[n:], crc(data[:n]))
	return data, nil
}

func unpackData(data []byte, msg proto.Message) error {
	if len(data) == 0 {
		return proto.Unmarshal(nil, msg)
	}
	if len(data) < 4 {
		return fmt.Errorf("truncated data")
	}
	n := len(data) - 4
	expected := binary.BigEndian.Uint32(data[n:])
	actual := crc(data[:n])
	if expected != actual {
		return fmt.Errorf("corrupt data")
	}
	return proto.Unmarshal(data[:n], msg)
}

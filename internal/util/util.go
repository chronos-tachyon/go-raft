package util

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
)

func Assert(ok bool, pat string, args ...interface{}) {
	if !ok {
		panic(fmt.Errorf(pat, args...))
	}
}

type ByVersion []string

func (x ByVersion) Len() int {
	return len(x)
}

func (x ByVersion) Swap(i, j int) {
	x[i], x[j] = x[j], x[i]
}

func (x ByVersion) Less(i, j int) bool {
	return versionCompare(versionSplit(x[i]), versionSplit(x[j])) < 0
}

var _ sort.Interface = ByVersion(nil)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

func CRC(data []byte) uint32 {
	raw := crc32.Checksum(data, castagnoliTable)
	return ((raw >> 15) | (raw << 17)) + 0xa282ead8
}

func EqualUDPAddr(a, b *net.UDPAddr) bool {
	return a.IP.Equal(b.IP) && a.Port == b.Port && a.Zone == b.Zone
}

func Min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func MustMarshalProto(msg proto.Message) []byte {
	if data, err := proto.Marshal(msg); err != nil {
		panic(err)
	} else {
		return data
	}
}

func MustUnmarshalProto(data []byte, msg proto.Message) {
	if err := proto.Unmarshal(data, msg); err != nil {
		panic(err)
	}
}

func PackData(msg proto.Message) []byte {
	data := MustMarshalProto(msg)
	n := len(data)
	data = append(data, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(data[n:], CRC(data[:n]))
	return data
}

func UnpackData(data []byte, msg proto.Message) error {
	if len(data) == 0 {
		return proto.Unmarshal(nil, msg)
	}
	if len(data) < 4 {
		return fmt.Errorf("truncated data")
	}
	n := len(data) - 4
	expected := binary.BigEndian.Uint32(data[n:])
	actual := CRC(data[:n])
	if expected != actual {
		return fmt.Errorf("corrupt data")
	}
	return proto.Unmarshal(data[:n], msg)
}

func WithLock(x sync.Locker, fn func() error) error{
	x.Lock()
	defer x.Unlock()
	return fn()
}

type versionPiece struct {
	text   string
	number uint64
}

var reVersionSplit = regexp.MustCompile(`^(\D*)(\d+)(.*)$`)

func versionSplit(str string) []versionPiece {
	var out []versionPiece
	for {
		match := reVersionSplit.FindStringSubmatch(str)
		if match == nil {
			break
		}
		n, err := strconv.ParseUint(match[2], 10, 64)
		if err != nil {
			panic(err)
		}
		out = append(out, versionPiece{text: match[1], number: n})
		str = match[3]
	}
	if len(str) > 0 {
		out = append(out, versionPiece{text: str})
	}
	return out
}

func versionCompare(a, b []versionPiece) int {
	length := len(a)
	if length > len(b) {
		length = len(b)
	}
	for i := 0; i < length; i++ {
		cmp := strings.Compare(a[i].text, b[i].text)
		if cmp != 0 {
			return cmp
		}
		cmp = u64Compare(a[i].number, b[i].number)
		if cmp != 0 {
			return cmp
		}
	}
	return u64Compare(uint64(len(a)), uint64(len(b)))
}

func u64Compare(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

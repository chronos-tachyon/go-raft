package packet

import (
	"encoding/binary"
	"hash/crc32"
)

type PeerId uint8
type Term uint32

type Packet interface {
	Pack() []byte
}

type VoteRequest struct {
	Term Term
}

func (r VoteRequest) Pack() []byte {
	buf := make([]byte, 16)
	buf[0] = 0x01
	binary.BigEndian.PutUint32(buf[1:5], uint32(r.Term))
	binary.BigEndian.PutUint32(buf[12:16], crc(buf[0:12]))
	return buf
}

type VoteResponse struct {
	Term    Term
	Granted bool
}

func (r VoteResponse) Pack() []byte {
	buf := make([]byte, 16)
	buf[0] = 0x02
	binary.BigEndian.PutUint32(buf[1:5], uint32(r.Term))
	buf[5] = boolToByte(r.Granted)
	binary.BigEndian.PutUint32(buf[12:16], crc(buf[0:12]))
	return buf
}

type HeartbeatRequest struct {
	Term  Term
	Nonce uint32
}

func (r HeartbeatRequest) Pack() []byte {
	buf := make([]byte, 16)
	buf[0] = 0x03
	binary.BigEndian.PutUint32(buf[1:5], uint32(r.Term))
	binary.BigEndian.PutUint32(buf[5:9], r.Nonce)
	binary.BigEndian.PutUint32(buf[12:16], crc(buf[0:12]))
	return buf
}

type HeartbeatResponse struct {
	Term    Term
	Nonce   uint32
	Success bool
}

func (r HeartbeatResponse) Pack() []byte {
	buf := make([]byte, 16)
	buf[0] = 0x04
	binary.BigEndian.PutUint32(buf[1:5], uint32(r.Term))
	binary.BigEndian.PutUint32(buf[5:9], r.Nonce)
	buf[9] = boolToByte(r.Success)
	binary.BigEndian.PutUint32(buf[12:16], crc(buf[0:12]))
	return buf
}

type NominateRequest struct {
	Term Term
}

func (r NominateRequest) Pack() []byte {
	buf := make([]byte, 16)
	buf[0] = 0x05
	binary.BigEndian.PutUint32(buf[1:5], uint32(r.Term))
	binary.BigEndian.PutUint32(buf[12:16], crc(buf[0:12]))
	return buf
}

type InformRequest struct {
	Term   Term
	Leader PeerId
}

func (r InformRequest) Pack() []byte {
	buf := make([]byte, 16)
	buf[0] = 0x06
	binary.BigEndian.PutUint32(buf[1:5], uint32(r.Term))
	buf[5] = byte(r.Leader)
	binary.BigEndian.PutUint32(buf[12:16], crc(buf[0:12]))
	return buf
}

func Unpack(buf []byte) Packet {
	if len(buf) == 16 {
		actual := binary.BigEndian.Uint32(buf[12:16])
		expected := crc(buf[0:12])
		if actual == expected {
			switch buf[0] {
			case 0x01:
				var r VoteRequest
				r.Term = Term(binary.BigEndian.Uint32(buf[1:5]))
				return r

			case 0x02:
				var r VoteResponse
				r.Term = Term(binary.BigEndian.Uint32(buf[1:5]))
				r.Granted = (buf[5] != 0)
				return r

			case 0x03:
				var r HeartbeatRequest
				r.Term = Term(binary.BigEndian.Uint32(buf[1:5]))
				r.Nonce = binary.BigEndian.Uint32(buf[5:9])
				return r

			case 0x04:
				var r HeartbeatResponse
				r.Term = Term(binary.BigEndian.Uint32(buf[1:5]))
				r.Nonce = binary.BigEndian.Uint32(buf[5:9])
				r.Success = (buf[9] != 0)
				return r

			case 0x05:
				var r NominateRequest
				r.Term = Term(binary.BigEndian.Uint32(buf[1:5]))
				return r

			case 0x06:
				var r InformRequest
				r.Term = Term(binary.BigEndian.Uint32(buf[1:5]))
				r.Leader = PeerId(buf[5])
				return r
			}
		}
	}
	return nil
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

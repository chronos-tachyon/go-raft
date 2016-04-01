package raft

import (
	"encoding/binary"
)

type PeerId uint8
type Term uint32
type Index uint32

type Packet interface {
	Pack() []byte
}

type VoteRequest struct {
	// The currentTerm of the caller.
	Term Term

	// Term of the last entry in the caller's log.
	LatestTerm Term

	// Index of the last entry in the caller's log.
	LatestIndex Index
}

func (pkt VoteRequest) Pack() []byte {
	buf := make([]byte, 20)
	buf[0] = 0x01
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.Term))
	binary.BigEndian.PutUint32(buf[8:12], uint32(pkt.LatestTerm))
	binary.BigEndian.PutUint32(buf[12:16], uint32(pkt.LatestIndex))
	binary.BigEndian.PutUint32(buf[16:20], crc(buf[0:16]))
	return buf
}

type VoteResponse struct {
	// The currentTerm of the callee.
	Term Term

	// true iff the follower/callee granted its vote to the candidate/caller.
	Granted bool

	LogIsOK bool
}

func (pkt VoteResponse) Pack() []byte {
	buf := make([]byte, 12)
	buf[0] = 0x02
	buf[1] = boolToByte(pkt.Granted)
	buf[2] = boolToByte(pkt.LogIsOK)
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.Term))
	binary.BigEndian.PutUint32(buf[8:12], crc(buf[0:8]))
	return buf
}

type AppendEntry struct {
	// Term in which the entry was proposed.
	Term Term

	// The state machine command to be committed.
	Command []byte
}

type AppendEntriesRequest struct {
	// The currentTerm of the caller.
	Term Term

	// Term of the entry preceding Entries.
	PrevLogTerm Term

	// Index of the entry preceding Entries,
	// or log.LatestIndex() if Entries is empty.
	PrevLogIndex Index

	// The log.CommitIndex of the leader.
	LeaderCommit Index

	// The log entries to append.  May be empty.
	Entries []AppendEntry
}

func (pkt AppendEntriesRequest) Pack() []byte {
	n := 24
	for _, entry := range pkt.Entries {
		assert(len(entry.Command) < 256, "command too long")
		n += len(entry.Command) + 5
	}
	assert(n <= 1024, "AppendEntriesRequest too long")
	buf := make([]byte, n)
	buf[0] = 0x03
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(pkt.Entries)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.Term))
	binary.BigEndian.PutUint32(buf[8:12], uint32(pkt.PrevLogTerm))
	binary.BigEndian.PutUint32(buf[12:16], uint32(pkt.PrevLogIndex))
	binary.BigEndian.PutUint32(buf[16:20], uint32(pkt.LeaderCommit))
	n = 20
	for _, entry := range pkt.Entries {
		binary.BigEndian.PutUint32(buf[n:n+4], uint32(entry.Term))
		buf[n+4] = uint8(len(entry.Command))
		copy(buf[n+5:], entry.Command)
		n += len(entry.Command) + 5
	}
	binary.BigEndian.PutUint32(buf[n:], crc(buf[0:n]))
	return buf
}

type AppendEntriesResponse struct {
	// True iff new entries were added to the log.
	Success bool

	// The currentTerm of the callee.
	Term Term

	// The callee's resulting log.LatestIndex().
	LatestIndex Index

	// The callee's resulting log.CommitIndex.
	CommitIndex Index
}

func (pkt AppendEntriesResponse) Pack() []byte {
	buf := make([]byte, 20)
	buf[0] = 0x04
	buf[1] = boolToByte(pkt.Success)
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.Term))
	binary.BigEndian.PutUint32(buf[8:12], uint32(pkt.LatestIndex))
	binary.BigEndian.PutUint32(buf[12:16], uint32(pkt.CommitIndex))
	binary.BigEndian.PutUint32(buf[16:20], crc(buf[0:16]))
	return buf
}

type NominateRequest struct {
	// The currentTerm of the caller.
	Term Term
}

func (pkt NominateRequest) Pack() []byte {
	buf := make([]byte, 12)
	buf[0] = 0x05
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.Term))
	binary.BigEndian.PutUint32(buf[8:12], crc(buf[0:8]))
	return buf
}

type InformRequest struct {
	// The currentTerm of the caller.
	Term Term

	// The caller's current leader, i.e. the peer which sent an
	// AppendEntriesRequest to the caller.
	Leader PeerId
}

func (pkt InformRequest) Pack() []byte {
	buf := make([]byte, 12)
	buf[0] = 0x06
	buf[1] = byte(pkt.Leader)
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.Term))
	binary.BigEndian.PutUint32(buf[8:12], crc(buf[0:8]))
	return buf
}

func Unpack(buf []byte) Packet {
	if len(buf) < 8 {
		return nil
	}

	n := len(buf) - 4
	actual := binary.BigEndian.Uint32(buf[n:])
	expected := crc(buf[0:n])
	if actual != expected {
		return nil
	}
	buf = buf[0:n]

	switch {
	case buf[0] == 0x01 && len(buf) == 16:
		var pkt VoteRequest
		pkt.Term = Term(binary.BigEndian.Uint32(buf[4:8]))
		pkt.LatestTerm = Term(binary.BigEndian.Uint32(buf[8:12]))
		pkt.LatestIndex = Index(binary.BigEndian.Uint32(buf[12:16]))
		return pkt

	case buf[0] == 0x02 && len(buf) == 8:
		var pkt VoteResponse
		pkt.Granted = (buf[1] != 0)
		pkt.LogIsOK = (buf[2] != 0)
		pkt.Term = Term(binary.BigEndian.Uint32(buf[4:8]))
		return pkt

	case buf[0] == 0x03 && len(buf) >= 20:
		var pkt AppendEntriesRequest
		count := int(binary.BigEndian.Uint16(buf[2:4]))
		pkt.Term = Term(binary.BigEndian.Uint32(buf[4:8]))
		pkt.PrevLogTerm = Term(binary.BigEndian.Uint32(buf[8:12]))
		pkt.PrevLogIndex = Index(binary.BigEndian.Uint32(buf[12:16]))
		pkt.LeaderCommit = Index(binary.BigEndian.Uint32(buf[16:20]))
		if count > 0 {
			pkt.Entries = make([]AppendEntry, count)
			n = 20
			for i := 0; i < count; i++ {
				if n+5 > len(buf) {
					return nil
				}
				term := Term(binary.BigEndian.Uint32(buf[n : n+4]))
				length := int(buf[n+4])
				n += 5
				if n+length > len(buf) {
					return nil
				}
				var cmd []byte
				if length > 0 {
					cmd = make([]byte, length)
					copy(cmd, buf[n : n+length])
				}
				pkt.Entries[i] = AppendEntry{
					Term:    term,
					Command: cmd,
				}
				n += length
			}
		}
		return pkt

	case buf[0] == 0x04 && len(buf) == 16:
		var pkt AppendEntriesResponse
		pkt.Success = (buf[1] != 0)
		pkt.Term = Term(binary.BigEndian.Uint32(buf[4:8]))
		pkt.LatestIndex = Index(binary.BigEndian.Uint32(buf[8:12]))
		pkt.CommitIndex = Index(binary.BigEndian.Uint32(buf[12:16]))
		return pkt

	case buf[0] == 0x05 && len(buf) == 8:
		var pkt NominateRequest
		pkt.Term = Term(binary.BigEndian.Uint32(buf[4:8]))
		return pkt

	case buf[0] == 0x06 && len(buf) == 8:
		var pkt InformRequest
		pkt.Leader = PeerId(buf[1])
		pkt.Term = Term(binary.BigEndian.Uint32(buf[4:8]))
		return pkt
	}
	return nil
}

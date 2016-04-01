package raft

import (
	"encoding/binary"
)

type PeerId uint8
type term uint32
type index uint32

type packet interface {
	pack() []byte
}

type voteRequest struct {
	// The currentTerm of the caller.
	term term

	// Term of the last entry in the caller's log.
	latestTerm term

	// Index of the last entry in the caller's log.
	latestIndex index
}

func (pkt voteRequest) pack() []byte {
	buf := make([]byte, 20)
	buf[0] = 0x01
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.term))
	binary.BigEndian.PutUint32(buf[8:12], uint32(pkt.latestTerm))
	binary.BigEndian.PutUint32(buf[12:16], uint32(pkt.latestIndex))
	binary.BigEndian.PutUint32(buf[16:20], crc(buf[0:16]))
	return buf
}

type voteResponse struct {
	// The currentTerm of the callee.
	term term

	// true iff the follower/callee granted its vote to the candidate/caller.
	granted bool

	logIsOK bool
}

func (pkt voteResponse) pack() []byte {
	buf := make([]byte, 12)
	buf[0] = 0x02
	buf[1] = boolToByte(pkt.granted)
	buf[2] = boolToByte(pkt.logIsOK)
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.term))
	binary.BigEndian.PutUint32(buf[8:12], crc(buf[0:8]))
	return buf
}

type appendEntry struct {
	// Term in which the entry was proposed.
	term term

	// The state machine command to be committed.
	command []byte
}

type appendEntriesRequest struct {
	// The currentTerm of the caller.
	term term

	// Term of the entry preceding Entries.
	prevLogTerm term

	// Index of the entry preceding Entries,
	// or log.LatestIndex() if Entries is empty.
	prevLogIndex index

	// The log.CommitIndex of the leader.
	leaderCommit index

	// The log entries to append.  May be empty.
	entries []appendEntry
}

func (pkt appendEntriesRequest) pack() []byte {
	n := 24
	for _, entry := range pkt.entries {
		assert(len(entry.command) < 256, "command too long")
		n += len(entry.command) + 5
	}
	assert(n <= 1024, "AppendEntriesRequest too long")
	buf := make([]byte, n)
	buf[0] = 0x03
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(pkt.entries)))
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.term))
	binary.BigEndian.PutUint32(buf[8:12], uint32(pkt.prevLogTerm))
	binary.BigEndian.PutUint32(buf[12:16], uint32(pkt.prevLogIndex))
	binary.BigEndian.PutUint32(buf[16:20], uint32(pkt.leaderCommit))
	n = 20
	for _, entry := range pkt.entries {
		binary.BigEndian.PutUint32(buf[n:n+4], uint32(entry.term))
		buf[n+4] = uint8(len(entry.command))
		copy(buf[n+5:], entry.command)
		n += len(entry.command) + 5
	}
	binary.BigEndian.PutUint32(buf[n:], crc(buf[0:n]))
	return buf
}

type appendEntriesResponse struct {
	// True iff new entries were added to the log.
	success bool

	// The currentTerm of the callee.
	term term

	// The callee's resulting log.LatestIndex().
	latestIndex index

	// The callee's resulting log.CommitIndex.
	commitIndex index
}

func (pkt appendEntriesResponse) pack() []byte {
	buf := make([]byte, 20)
	buf[0] = 0x04
	buf[1] = boolToByte(pkt.success)
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.term))
	binary.BigEndian.PutUint32(buf[8:12], uint32(pkt.latestIndex))
	binary.BigEndian.PutUint32(buf[12:16], uint32(pkt.commitIndex))
	binary.BigEndian.PutUint32(buf[16:20], crc(buf[0:16]))
	return buf
}

type nominateRequest struct {
	// The currentTerm of the caller.
	term term
}

func (pkt nominateRequest) pack() []byte {
	buf := make([]byte, 12)
	buf[0] = 0x05
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.term))
	binary.BigEndian.PutUint32(buf[8:12], crc(buf[0:8]))
	return buf
}

type informRequest struct {
	// The currentTerm of the caller.
	term term

	// The caller's current leader, i.e. the peer which sent an
	// AppendEntriesRequest to the caller.
	leader PeerId
}

func (pkt informRequest) pack() []byte {
	buf := make([]byte, 12)
	buf[0] = 0x06
	buf[1] = byte(pkt.leader)
	binary.BigEndian.PutUint32(buf[4:8], uint32(pkt.term))
	binary.BigEndian.PutUint32(buf[8:12], crc(buf[0:8]))
	return buf
}

func unpack(buf []byte) packet {
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
		var pkt voteRequest
		pkt.term = term(binary.BigEndian.Uint32(buf[4:8]))
		pkt.latestTerm = term(binary.BigEndian.Uint32(buf[8:12]))
		pkt.latestIndex = index(binary.BigEndian.Uint32(buf[12:16]))
		return pkt

	case buf[0] == 0x02 && len(buf) == 8:
		var pkt voteResponse
		pkt.granted = (buf[1] != 0)
		pkt.logIsOK = (buf[2] != 0)
		pkt.term = term(binary.BigEndian.Uint32(buf[4:8]))
		return pkt

	case buf[0] == 0x03 && len(buf) >= 20:
		var pkt appendEntriesRequest
		count := int(binary.BigEndian.Uint16(buf[2:4]))
		pkt.term = term(binary.BigEndian.Uint32(buf[4:8]))
		pkt.prevLogTerm = term(binary.BigEndian.Uint32(buf[8:12]))
		pkt.prevLogIndex = index(binary.BigEndian.Uint32(buf[12:16]))
		pkt.leaderCommit = index(binary.BigEndian.Uint32(buf[16:20]))
		if count > 0 {
			pkt.entries = make([]appendEntry, count)
			n = 20
			for i := 0; i < count; i++ {
				if n+5 > len(buf) {
					return nil
				}
				term := term(binary.BigEndian.Uint32(buf[n : n+4]))
				length := int(buf[n+4])
				n += 5
				if n+length > len(buf) {
					return nil
				}
				var cmd []byte
				if length > 0 {
					cmd = make([]byte, length)
					copy(cmd, buf[n:n+length])
				}
				pkt.entries[i] = appendEntry{term, cmd}
				n += length
			}
		}
		return pkt

	case buf[0] == 0x04 && len(buf) == 16:
		var pkt appendEntriesResponse
		pkt.success = (buf[1] != 0)
		pkt.term = term(binary.BigEndian.Uint32(buf[4:8]))
		pkt.latestIndex = index(binary.BigEndian.Uint32(buf[8:12]))
		pkt.commitIndex = index(binary.BigEndian.Uint32(buf[12:16]))
		return pkt

	case buf[0] == 0x05 && len(buf) == 8:
		var pkt nominateRequest
		pkt.term = term(binary.BigEndian.Uint32(buf[4:8]))
		return pkt

	case buf[0] == 0x06 && len(buf) == 8:
		var pkt informRequest
		pkt.leader = PeerId(buf[1])
		pkt.term = term(binary.BigEndian.Uint32(buf[4:8]))
		return pkt
	}
	return nil
}

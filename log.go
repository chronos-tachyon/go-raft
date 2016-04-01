package raft

import (
	"fmt"
)

type raftEntry struct {
	index    uint64
	term     uint64
	command  []byte
	callback func(bool)
}

type raftLog struct {
	// The term of the hypothetical entry before entries.
	startTerm uint64

	// The index of the first entry in entries.
	startIndex uint64

	// The index of the most recently committed entry.
	commitIndex uint64

	// The unsnapshotted entries.
	entries []raftEntry

	// The most recent snapshot.
	snapshot []byte
}

func (log *raftLog) checkInvariants() {
	assert(log.startIndex != 0, "log not initialized")
	end := log.startIndex + uint64(len(log.entries))
	assert(log.commitIndex < end, "commit %d ahead of end %d", log.commitIndex, end)
	for i, entry := range log.entries {
		idx := log.startIndex + uint64(i)
		assert(entry.index == idx, "index mismatch: expected %d got %d", idx, entry.index)
	}
}

func (log *raftLog) nextIndex() uint64 {
	log.checkInvariants()
	p := log.startIndex + uint64(len(log.entries))
	if len(log.entries) > 0 {
		q := log.entries[len(log.entries)-1].index + 1
		assert(p == q, "index mismatch: expected %d got %d", p, q)
	}
	return p
}

func (log *raftLog) latestIndex() uint64 {
	return log.nextIndex() - 1
}

func (log *raftLog) at(idx uint64) raftEntry {
	log.checkInvariants()
	lo := log.startIndex
	hi := lo + uint64(len(log.entries))
	assert(idx >= lo, "idx=%d outside of [%d,%d)", idx, lo, hi)
	assert(idx < hi, "idx=%d outside of [%d,%d)", idx, lo, hi)
	entry := log.entries[idx-log.startIndex]
	assert(entry.index == idx, "index mismatch: expected %d got %d", idx, entry.index)
	return entry
}

func (log *raftLog) term(idx uint64) uint64 {
	log.checkInvariants()
	lo := log.startIndex
	hi := lo + uint64(len(log.entries))
	if idx < lo {
		return log.startTerm
	}
	assert(idx < hi, "idx=%d outside of [%d,%d)", idx, lo, hi)
	entry := log.entries[idx-log.startIndex]
	assert(entry.index == idx, "index mismatch: expected %d got %d", idx, entry.index)
	return entry.term
}

// deleteEntriesBefore removes all entries from StartIndex to one before idx.
func (log *raftLog) deleteEntriesBefore(idx uint64) {
	log.checkInvariants()
	if idx > log.startIndex {
		delta := idx - log.startIndex
		assert(uint64(len(log.entries)) >= delta, "have %d items, attempting to delete %d items", len(log.entries), delta)
		n := uint64(len(log.entries)) - delta
		startTerm := log.term(idx - 1)
		var entries []raftEntry
		if n > 0 {
			entries = make([]raftEntry, n)
			copy(entries, log.entries[idx:idx+n])
		}
		log.entries = entries
		log.startIndex = idx
		log.startTerm = startTerm
	}
	log.checkInvariants()
}

// deleteEntriesAfter removes all entries that come after idx.
func (log *raftLog) deleteEntriesAfter(idx uint64) {
	log.checkInvariants()
	if idx < log.startIndex {
		log.entries = nil
		return
	}
	if idx < log.latestIndex() {
		log.entries = log.entries[0 : idx-log.startIndex+1]
	}
	log.checkInvariants()
}

func (log *raftLog) appendEntry(trm uint64, command []byte, callback func(bool)) {
	log.checkInvariants()
	idx := log.nextIndex()
	log.entries = append(log.entries, raftEntry{idx, trm, command, callback})
	log.checkInvariants()
}

func (log *raftLog) String() string {
	var entries string
	for _, entry := range log.entries {
		entries += fmt.Sprintf("%d,", entry.term)
	}
	if len(entries) > 0 {
		entries = entries[0 : len(entries)-1]
	}
	return fmt.Sprintf("%d %d %d [%s]", log.startTerm, log.startIndex, log.commitIndex, entries)
}

func (log *raftLog) takeSnapshot(sm StateMachine) {
	log.snapshot = sm.Snapshot()
	log.deleteEntriesBefore(log.commitIndex + 1)
}

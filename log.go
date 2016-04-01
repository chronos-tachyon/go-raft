package raft

import (
	"fmt"
)

type raftEntry struct {
	index    index
	term     term
	command  []byte
	callback func(bool)
}

type raftLog struct {
	// The Term of the hypothetical entry before Entries.
	startTerm term

	// The index of the first entry in Entries.
	startIndex index

	// The index of the most recently committed entry.
	commitIndex index

	// The unsnapshotted entries.
	entries []raftEntry
}

func newLog() *raftLog {
	return &raftLog{startIndex: 1}
}

func (log *raftLog) checkInvariants() {
	assert(log.startIndex != 0, "log not initialized")
	end := log.startIndex + index(len(log.entries))
	assert(log.commitIndex < end, "commit %d ahead of end %d", log.commitIndex, end)
	for i, entry := range log.entries {
		idx := log.startIndex + index(i)
		assert(entry.index == idx, "index mismatch: expected %d got %d", idx, entry.index)
	}
}

func (log *raftLog) nextIndex() index {
	log.checkInvariants()
	p := log.startIndex + index(len(log.entries))
	if len(log.entries) > 0 {
		q := log.entries[len(log.entries)-1].index + 1
		assert(p == q, "index mismatch: expected %d got %d", p, q)
	}
	return p
}

func (log *raftLog) latestIndex() index {
	return log.nextIndex() - 1
}

func (log *raftLog) at(idx index) raftEntry {
	log.checkInvariants()
	lo := log.startIndex
	hi := lo + index(len(log.entries))
	assert(idx >= lo, "idx=%d outside of [%d,%d)", idx, lo, hi)
	assert(idx < hi, "idx=%d outside of [%d,%d)", idx, lo, hi)
	entry := log.entries[idx-log.startIndex]
	assert(entry.index == idx, "index mismatch: expected %d got %d", idx, entry.index)
	return entry
}

func (log *raftLog) term(idx index) term {
	log.checkInvariants()
	lo := log.startIndex
	hi := lo + index(len(log.entries))
	if idx < lo {
		return log.startTerm
	}
	assert(idx < hi, "idx=%d outside of [%d,%d)", idx, lo, hi)
	entry := log.entries[idx-log.startIndex]
	assert(entry.index == idx, "index mismatch: expected %d got %d", idx, entry.index)
	return entry.term
}

// deleteEntriesBefore removes all entries from StartIndex to one before idx.
func (log *raftLog) deleteEntriesBefore(idx index) {
	log.checkInvariants()
	if idx > log.startIndex {
		n := log.startIndex + index(len(log.entries)) - idx
		entries := make([]raftEntry, n)
		copy(entries, log.entries[idx:idx+n])
		log.entries = entries
		log.startIndex = idx
	}
	log.checkInvariants()
}

// deleteEntriesAfter removes all entries that come after idx.
func (log *raftLog) deleteEntriesAfter(idx index) {
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

func (log *raftLog) appendEntry(trm term, command []byte, callback func(bool)) {
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

package raft

import (
	"fmt"
)

type raftEntry struct {
	index    Index
	term     Term
	command  []byte
	callback func(bool)
}

type raftLog struct {
	// The Term of the hypothetical entry before Entries.
	startTerm Term

	// The index of the first entry in Entries.
	startIndex Index

	// The index of the most recently committed entry.
	commitIndex Index

	// The unsnapshotted entries.
	entries []raftEntry
}

func newLog() *raftLog {
	return &raftLog{startIndex: 1}
}

func (log *raftLog) checkInvariants() {
	assert(log.startIndex != 0, "log not initialized")
	end := log.startIndex + Index(len(log.entries))
	assert(log.commitIndex < end, "commit %d ahead of end %d", log.commitIndex, end)
	for i, entry := range log.entries {
		index := log.startIndex + Index(i)
		assert(entry.index == index, "index mismatch: expected %d got %d", index, entry.index)
	}
}

func (log *raftLog) nextIndex() Index {
	log.checkInvariants()
	p := log.startIndex + Index(len(log.entries))
	if len(log.entries) > 0 {
		q := log.entries[len(log.entries)-1].index + 1
		assert(p == q, "index mismatch: expected %d got %d", p, q)
	}
	return p
}

func (log *raftLog) latestIndex() Index {
	return log.nextIndex() - 1
}

func (log *raftLog) at(index Index) raftEntry {
	log.checkInvariants()
	lo := log.startIndex
	hi := lo + Index(len(log.entries))
	assert(index >= lo, "index=%d outside of [%d,%d)", lo, hi)
	assert(index < hi, "index=%d outside of [%d,%d)", index, lo, hi)
	entry := log.entries[index-log.startIndex]
	assert(entry.index == index, "index mismatch: expected %d got %d", index, entry.index)
	return entry
}

func (log *raftLog) term(index Index) Term {
	log.checkInvariants()
	lo := log.startIndex
	hi := lo + Index(len(log.entries))
	if index < lo {
		return log.startTerm
	}
	assert(index < hi, "index=%d outside of [%d,%d)", index, lo, hi)
	entry := log.entries[index-log.startIndex]
	assert(entry.index == index, "index mismatch: expected %d got %d", index, entry.index)
	return entry.term
}

// deleteEntriesBefore removes all entries from StartIndex to one before index.
func (log *raftLog) deleteEntriesBefore(index Index) {
	log.checkInvariants()
	if index > log.startIndex {
		n := log.startIndex + Index(len(log.entries)) - index
		entries := make([]raftEntry, n)
		copy(entries, log.entries[index:index+n])
		log.entries = entries
		log.startIndex = index
	}
	log.checkInvariants()
}

// deleteEntriesAfter removes all entries that come after index.
func (log *raftLog) deleteEntriesAfter(index Index) {
	log.checkInvariants()
	if index < log.startIndex {
		log.entries = nil
		return
	}
	if index < log.latestIndex() {
		log.entries = log.entries[0 : index-log.startIndex+1]
	}
	log.checkInvariants()
}

func (log *raftLog) appendEntry(term Term, command []byte, callback func(bool)) {
	log.checkInvariants()
	index := log.nextIndex()
	log.entries = append(log.entries, raftEntry{index, term, command, callback})
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

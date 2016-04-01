package raft

import (
	"fmt"
)

type raftEntry struct {
	// The index of the raftEntry.  Can be computed from startIndex plus
	// the offset in the raftLog.entries array, but the redundancy lets us
	// add some asserts.
	index    uint64

	// The election term in which this entry was proposed.
	term     uint64

	// The state machine command which this entry proposed.
	command  []byte

	// An optional callback to invoke when this entry is finally committed
	// or rejected.
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

// String returns a human-readable compact string summary of the log.
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

// checkInvariants asserts that all log invariants are held.
func (log *raftLog) checkInvariants() {
	assert(log.startIndex != 0, "log not initialized")
	end := log.startIndex + uint64(len(log.entries))
	assert(log.commitIndex < end, "commit %d ahead of end %d", log.commitIndex, end)
	for i, entry := range log.entries {
		index := log.startIndex + uint64(i)
		assert(entry.index == index, "index mismatch: expected %d got %d", index, entry.index)
	}
}

// nextIndex returns the index which the next entry will have.
func (log *raftLog) nextIndex() uint64 {
	log.checkInvariants()
	p := log.startIndex + uint64(len(log.entries))
	if len(log.entries) > 0 {
		q := log.entries[len(log.entries)-1].index + 1
		assert(p == q, "index mismatch: expected %d got %d", p, q)
	}
	return p
}

// latestIndex returns the index of the most recently added entry.
func (log *raftLog) latestIndex() uint64 {
	return log.nextIndex() - 1
}

// at returns the entry with the given index.  Panics if the entry has been
// discarded by snapshotting, if the index is 0, or if no entry with that index
// exists yet.
func (log *raftLog) at(index uint64) raftEntry {
	log.checkInvariants()
	lo := log.startIndex
	hi := lo + uint64(len(log.entries))
	assert(index >= lo, "index=%d outside of [%d,%d)", index, lo, hi)
	assert(index < hi, "index=%d outside of [%d,%d)", index, lo, hi)
	entry := log.entries[index-log.startIndex]
	assert(entry.index == index, "index mismatch: expected %d got %d", index, entry.index)
	return entry
}

// term returns the term of the entry at the given index.
// Guesses if the index is 0 or if the entry has been discarded by
// snapshotting.  (The guess is accurate if the index is 0 or startIndex-1.)
// Panics if no entry with that index exists yet.
func (log *raftLog) term(index uint64) uint64 {
	log.checkInvariants()
	lo := log.startIndex
	hi := lo + uint64(len(log.entries))
	if index < lo {
		return log.startTerm
	}
	assert(index < hi, "index=%d outside of [%d,%d)", index, lo, hi)
	entry := log.entries[index-log.startIndex]
	assert(entry.index == index, "index mismatch: expected %d got %d", index, entry.index)
	return entry.term
}

// deleteEntriesBefore prunes entries that come before the referenced entry.
func (log *raftLog) deleteEntriesBefore(index uint64) {
	log.checkInvariants()
	if index > log.startIndex {
		delta := index - log.startIndex
		assert(uint64(len(log.entries)) >= delta, "have %d items, attempting to delete %d items", len(log.entries), delta)
		n := uint64(len(log.entries)) - delta
		startTerm := log.term(index - 1)
		var entries []raftEntry
		if n > 0 {
			entries = make([]raftEntry, n)
			copy(entries, log.entries[index:index+n])
		}
		log.entries = entries
		log.startIndex = index
		log.startTerm = startTerm
	}
	log.checkInvariants()
}

// deleteEntriesAfter removes entries that come after the referenced entry.
func (log *raftLog) deleteEntriesAfter(index uint64) {
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

// appendEntry adds a new entry to the log.
func (log *raftLog) appendEntry(term uint64, command []byte, callback func(bool)) {
	log.checkInvariants()
	index := log.nextIndex()
	log.entries = append(log.entries, raftEntry{index, term, command, callback})
	log.checkInvariants()
}

// takeSnapshot replaces all committed entries with a snapshot.
func (log *raftLog) takeSnapshot(sm StateMachine) {
	log.snapshot = sm.Snapshot()
	log.deleteEntriesBefore(log.commitIndex + 1)
}

package raft

import (
	"fmt"
)

type LogEntry struct {
	Index    Index
	Term     Term
	Command  []byte
	Callback func(bool)
}

type Log struct {
	// The Term of the hypothetical entry before Entries.
	StartTerm   Term

	// The index of the first entry in Entries.
	StartIndex  Index

	// The index of the most recently committed entry.
	CommitIndex Index

	// The unsnapshotted entries.
	Entries     []LogEntry
}

func NewLog() *Log {
	return &Log{StartIndex: 1}
}

func (log *Log) NextIndex() Index {
	log.checkInvariants()
	p := log.StartIndex + Index(len(log.Entries))
	if len(log.Entries) > 0 {
		q := log.Entries[len(log.Entries)-1].Index + 1
		assert(p == q, "index mismatch: expected %d got %d", p, q)
	}
	return p
}

func (log *Log) LatestIndex() Index {
	return log.NextIndex() - 1
}

func (log *Log) At(index Index) LogEntry {
	log.checkInvariants()
	lo := log.StartIndex
	hi := lo + Index(len(log.Entries))
	assert(index >= lo, "index=%d outside of [%d,%d)", lo, hi)
	assert(index < hi, "index=%d outside of [%d,%d)", index, lo, hi)
	entry := log.Entries[index-log.StartIndex]
	assert(entry.Index == index, "index mismatch: expected %d got %d", index, entry.Index)
	return entry
}

func (log *Log) Term(index Index) Term {
	log.checkInvariants()
	lo := log.StartIndex
	hi := lo + Index(len(log.Entries))
	if index < lo {
		return log.StartTerm
	}
	assert(index < hi, "index=%d outside of [%d,%d)", index, lo, hi)
	entry := log.Entries[index-log.StartIndex]
	assert(entry.Index == index, "index mismatch: expected %d got %d", index, entry.Index)
	return entry.Term
}

// DeleteEntriesBefore removes all entries from StartIndex to one before index.
func (log *Log) DeleteEntriesBefore(index Index) {
	log.checkInvariants()
	if index > log.StartIndex {
		n := log.StartIndex + Index(len(log.Entries)) - index
		entries := make([]LogEntry, n)
		copy(entries, log.Entries[index:index+n])
		log.Entries = entries
		log.StartIndex = index
	}
	log.checkInvariants()
}

// DeleteEntriesAfter removes all entries that come after index.
func (log *Log) DeleteEntriesAfter(index Index) {
	log.checkInvariants()
	if index < log.StartIndex {
		log.Entries = nil
		return
	}
	if index < log.LatestIndex() {
		log.Entries = log.Entries[0 : index - log.StartIndex + 1]
	}
	log.checkInvariants()
}

func (log *Log) Append(term Term, command []byte, callback func(bool)) {
	log.checkInvariants()
	index := log.NextIndex()
	log.Entries = append(log.Entries, LogEntry{index, term, command, callback})
	log.checkInvariants()
}

func (log *Log) String() string {
	var entries string
	for _, entry := range log.Entries {
		entries += fmt.Sprintf("%d,", entry.Term)
	}
	if len(entries) > 0 {
		entries = entries[0:len(entries)-1]
	}
	return fmt.Sprintf("%d %d %d [%s]", log.StartTerm, log.StartIndex, log.CommitIndex, entries)
}

func (log *Log) checkInvariants() {
	assert(log.StartIndex != 0, "log not initialized")
	end := log.StartIndex + Index(len(log.Entries))
	assert(log.CommitIndex < end, "commit %d ahead of end %d", log.CommitIndex, end)
	for i, entry := range log.Entries {
		index := log.StartIndex + Index(i)
		assert(entry.Index == index, "index mismatch: expected %d got %d", index, entry.Index)
	}
}

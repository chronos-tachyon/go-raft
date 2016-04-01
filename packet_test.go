package raft

import (
	"bytes"
	"testing"
)

func TestVoteRequest(t *testing.T) {
	for i, before := range []VoteRequest{
		VoteRequest{},
		VoteRequest{5, 3, 42},
	} {
		packed := before.Pack()
		after := Unpack(packed).(VoteRequest)
		if before != after {
			t.Errorf("[%d] failed to round-trip: expected %#v, got %#v", i, before, after)
		}
	}
}

func TestVoteResponse(t *testing.T) {
	for i, before := range []VoteResponse{
		VoteResponse{},
		VoteResponse{3, false, false},
		VoteResponse{5, true, false},
		VoteResponse{7, false, true},
		VoteResponse{9, true, true},
	} {
		packed := before.Pack()
		after := Unpack(packed).(VoteResponse)
		if before != after {
			t.Errorf("[%d] failed to round-trip: expected %#v, got %#v", i, before, after)
		}
	}
}

func TestAppendEntriesRequest(t *testing.T) {
	a := []byte{}
	b := []byte{'1'}
	c := []byte{'2', '2'}
	d := []byte(nil)

	for i, before := range []AppendEntriesRequest{
		AppendEntriesRequest{},
		AppendEntriesRequest{5, 3, 42, 50, []AppendEntry{
			AppendEntry{3, a},
			AppendEntry{3, b},
			AppendEntry{3, c},
			AppendEntry{4, d},
		}},
	} {
		packed := before.Pack()
		after := Unpack(packed).(AppendEntriesRequest)
		if !equalAppendEntriesRequest(before, after) {
			t.Errorf("[%d] failed to round-trip: expected %#v, got %#v", i, before, after)
		}
	}
}

func TestAppendEntriesResponse(t *testing.T) {
	before := AppendEntriesResponse{
		Term:    5,
		Success: true,
	}
	packed := before.Pack()
	after := Unpack(packed).(AppendEntriesResponse)
	if before != after {
		t.Errorf("failed to round-trip: expected %#v, got %#v", before, after)
	}
}

func TestNominateRequest(t *testing.T) {
	before := NominateRequest{
		Term: 5,
	}
	packed := before.Pack()
	after := Unpack(packed).(NominateRequest)
	if before != after {
		t.Errorf("failed to round-trip: expected %#v, got %#v", before, after)
	}
}

func TestInformRequest(t *testing.T) {
	before := InformRequest{
		Term:   5,
		Leader: 23,
	}
	packed := before.Pack()
	after := Unpack(packed).(InformRequest)
	if before != after {
		t.Errorf("failed to round-trip: expected %#v, got %#v", before, after)
	}
}

func equalAppendEntriesRequest(a, b AppendEntriesRequest) bool {
	return (a.Term == b.Term &&
		a.PrevLogTerm == b.PrevLogTerm &&
		a.PrevLogIndex == b.PrevLogIndex &&
		a.LeaderCommit == b.LeaderCommit &&
		equalEntries(a.Entries, b.Entries))
}

func equalEntries(a, b []AppendEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Term != b[i].Term || !bytes.Equal(a[i].Command, b[i].Command) {
			return false
		}
	}
	return true
}

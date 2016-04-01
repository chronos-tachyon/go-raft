package raft

import (
	"bytes"
	"testing"
)

func TestVoteRequest(t *testing.T) {
	for i, before := range []voteRequest{
		{},
		{5, 3, 42},
	} {
		packed := before.pack()
		after := unpack(packed).(voteRequest)
		if before != after {
			t.Errorf("[%d] failed to round-trip: expected %#v, got %#v", i, before, after)
		}
	}
}

func TestVoteResponse(t *testing.T) {
	for i, before := range []voteResponse{
		{},
		{3, false, false},
		{5, true, false},
		{7, false, true},
		{9, true, true},
	} {
		packed := before.pack()
		after := unpack(packed).(voteResponse)
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

	for i, before := range []appendEntriesRequest{
		{},
		{5, 3, 42, 50, []appendEntry{
			{3, a},
			{3, b},
			{3, c},
			{4, d},
		}},
	} {
		packed := before.pack()
		after := unpack(packed).(appendEntriesRequest)
		if !equalAppendEntriesRequest(before, after) {
			t.Errorf("[%d] failed to round-trip: expected %#v, got %#v", i, before, after)
		}
	}
}

func TestAppendEntriesResponse(t *testing.T) {
	before := appendEntriesResponse{
		Term:    5,
		Success: true,
	}
	packed := before.pack()
	after := unpack(packed).(appendEntriesResponse)
	if before != after {
		t.Errorf("failed to round-trip: expected %#v, got %#v", before, after)
	}
}

func TestNominateRequest(t *testing.T) {
	before := nominateRequest{
		Term: 5,
	}
	packed := before.pack()
	after := unpack(packed).(nominateRequest)
	if before != after {
		t.Errorf("failed to round-trip: expected %#v, got %#v", before, after)
	}
}

func TestInformRequest(t *testing.T) {
	before := informRequest{
		Term:   5,
		Leader: 23,
	}
	packed := before.pack()
	after := unpack(packed).(informRequest)
	if before != after {
		t.Errorf("failed to round-trip: expected %#v, got %#v", before, after)
	}
}

func equalAppendEntriesRequest(a, b appendEntriesRequest) bool {
	return (a.Term == b.Term &&
		a.PrevLogTerm == b.PrevLogTerm &&
		a.PrevLogIndex == b.PrevLogIndex &&
		a.LeaderCommit == b.LeaderCommit &&
		equalEntries(a.Entries, b.Entries))
}

func equalEntries(a, b []appendEntry) bool {
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

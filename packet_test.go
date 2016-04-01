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
		term:    5,
		success: true,
	}
	packed := before.pack()
	after := unpack(packed).(appendEntriesResponse)
	if before != after {
		t.Errorf("failed to round-trip: expected %#v, got %#v", before, after)
	}
}

func TestNominateRequest(t *testing.T) {
	before := nominateRequest{
		term: 5,
	}
	packed := before.pack()
	after := unpack(packed).(nominateRequest)
	if before != after {
		t.Errorf("failed to round-trip: expected %#v, got %#v", before, after)
	}
}

func TestInformRequest(t *testing.T) {
	before := informRequest{
		term:   5,
		leader: 23,
	}
	packed := before.pack()
	after := unpack(packed).(informRequest)
	if before != after {
		t.Errorf("failed to round-trip: expected %#v, got %#v", before, after)
	}
}

func equalAppendEntriesRequest(a, b appendEntriesRequest) bool {
	return (a.term == b.term &&
		a.prevLogTerm == b.prevLogTerm &&
		a.prevLogIndex == b.prevLogIndex &&
		a.leaderCommit == b.leaderCommit &&
		equalEntries(a.entries, b.entries))
}

func equalEntries(a, b []appendEntry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].term != b[i].term || !bytes.Equal(a[i].command, b[i].command) {
			return false
		}
	}
	return true
}

package server

import (
	"fmt"

	pb "github.com/chronos-tachyon/go-raft/proto"
)

type vote uint8

const (
	unknownVote vote = iota
	yeaVote
	nayVote
)

var voteChars = []byte{'u', 'Y', 'N'}

func (v vote) Char() byte {
	if int(v) >= len(voteChars) {
		return '?'
	}
	return voteChars[v]
}

type peer struct {
	id         uint32
	addr       string
	state      pb.Peer_State
	vote       vote
	nextIndex  uint64
	matchIndex uint64
}

func (p *peer) String() string {
	return fmt.Sprintf("%d%c %c %02d,%02d", p.id, p.state.Char(),
		p.vote.Char(), p.matchIndex, p.nextIndex)
}

func (p *peer) export() *pb.Peer {
	return &pb.Peer{Id: p.id, Addr: p.addr, State: p.state}
}

func importPeer(in *pb.Peer, old *configuration, id uint32, addr string) *peer {
	var tmp peer
	if old != nil {
		if x, found := old.byId[id]; found {
			tmp = *x
		}
	}
	tmp.id = id
	if in == nil {
		tmp.addr = addr
		tmp.state = pb.Peer_BLANK
	} else {
		tmp.addr = in.Addr
		tmp.state = in.State
	}
	return &tmp
}

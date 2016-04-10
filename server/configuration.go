package server

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/chronos-tachyon/go-raft/internal/util"
	pb "github.com/chronos-tachyon/go-raft/proto"
)

type configuration struct {
	id      uint64
	peers   []*peer
	byId    map[uint32]*peer
}

func (c *configuration) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%d[", c.id)
	for _, p := range c.peers {
		buf.WriteString(p.String())
		buf.WriteByte(' ')
	}
	if slice := buf.Bytes(); slice[len(slice)-1] == ' ' {
		buf.Truncate(buf.Len() - 1)
	}
	buf.WriteByte(']')
	return buf.String()
}

func (c *configuration) export() *pb.Configuration {
	c.checkInvariants()

	var out pb.Configuration
	out.Id = c.id
	out.Peers = make([]*pb.Peer, 0, len(c.peers))
	for _, p := range c.peers {
		if pbp := p.export(); pbp != nil {
			out.Peers = append(out.Peers, pbp)
		}
	}
	return &out
}

func importConfiguration(in *pb.Configuration, old *configuration, selfId uint32, selfAddr string) *configuration {
	var c configuration
	if in != nil {
		c.id = in.Id
		c.peers = make([]*peer, len(in.Peers))
		c.byId = make(map[uint32]*peer, len(in.Peers))
		for i, pbp := range in.Peers {
			p := importPeer(pbp, old, pbp.Id, pbp.Addr)
			c.peers[i] = p
			c.byId[pbp.Id] = p
		}
	} else {
		c.id = 1
		c.byId = make(map[uint32]*peer)
	}
	if _, found := c.byId[selfId]; !found {
		p := importPeer(nil, old, selfId, selfAddr)
		c.peers = append(c.peers, p)
		c.byId[selfId] = p
	}
	sort.Sort(byId(c.peers))
	c.checkInvariants()
	return &c
}

func (c *configuration) yeaQuorum() bool {
	c.checkInvariants()
	var totalOld, yeaOld uint
	var totalNew, yeaNew uint
	for _, p := range c.peers {
		if p.state == pb.Peer_LEAVING || p.state == pb.Peer_STABLE {
			totalOld++
			if p.vote == yeaVote {
				yeaOld++
			}
		}
		if p.state == pb.Peer_STABLE || p.state == pb.Peer_JOINING {
			totalNew++
			if p.vote == yeaVote {
				yeaNew++
			}
		}
	}
	quorumOld := totalOld/2 + 1
	quorumNew := totalNew/2 + 1
	return yeaOld >= quorumOld && yeaNew >= quorumNew
}

func (c *configuration) matchQuorum() uint64 {
	c.checkInvariants()
	listOld := make([]uint64, 0, len(c.peers))
	listNew := make([]uint64, 0, len(c.peers))
	for _, p := range c.peers {
		switch p.state {
		case pb.Peer_LEAVING:
			listOld = append(listOld, p.matchIndex)
		case pb.Peer_STABLE:
			listOld = append(listOld, p.matchIndex)
			listNew = append(listNew, p.matchIndex)
		case pb.Peer_JOINING:
			listNew = append(listNew, p.matchIndex)
		}
	}
	sort.Sort(byIndex(listOld))
	sort.Sort(byIndex(listNew))
	medianOld := (len(listOld) - 1) / 2
	medianNew := (len(listNew) - 1) / 2
	return util.Min(listOld[medianOld], listNew[medianNew])
}

func (c *configuration) checkInvariants() {
	util.Assert(c.id > 0, "c.id not set")
	for _, p := range c.peers {
		switch p.state {
		case pb.Peer_BLANK, pb.Peer_LEAVING, pb.Peer_STABLE, pb.Peer_JOINING, pb.Peer_LEARNING:
			// pass
		default:
			panic(fmt.Errorf("unknown pb.Peer_State %d", p.state))
		}
		switch p.vote {
		case unknownVote, yeaVote, nayVote:
			// pass
		default:
			panic(fmt.Errorf("unknown server.vote %d", p.vote))
		}
		target, found := c.byId[p.id]
		util.Assert(found, "peer %v not in c.byId", p)
		util.Assert(p == target, "c.peers peer %v but c.byId peer %v", p, target)
	}
	util.Assert(sort.IsSorted(byId(c.peers)), "c.peers not sorted")
}

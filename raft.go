// Package raft implements the Raft distributed consensus protocol.
package raft

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"

	"github.com/chronos-tachyon/go-raft/packet"
)

type state uint8

const (
	follower state = iota
	candidate
	leader
)

type peer struct {
	id   packet.NodeId
	addr *net.UDPAddr
}

type Node struct {
	self  packet.NodeId
	peers map[packet.NodeId]peer
	conn  *net.UDPConn

	wg            sync.WaitGroup
	mutex         sync.Mutex
	state         state
	votedFor      packet.NodeId
	currentLeader packet.NodeId
	currentTerm   packet.Term
	time0         uint32
	time1         uint32
	currentNonce  uint32
	yeaVotes      map[packet.NodeId]struct{}

	onGainLeadership func(*Node)
	onLoseLeadership func(*Node)
}

/*
 * follower:
 *	time0		ticks until become candidate
 *	time1		unused (0)
 *	currentNonce	unused (0)
 *	yeaVotes	unused (nil)
 *
 * candidate:
 *	time0		ticks until bump currentTerm and try again
 *	time1		ticks until retransmit VoteRequests
 *	currentNonce	unused (0)
 *	yeaVotes	set of yea votes to become leader
 *
 * leader:
 *	time0		ticks until drop leadership
 *	time1		ticks until heartbeat sent
 *	currentNonce	heartbeat nonce
 *	yeaVotes	set of yea votes to retain leadership
 */

// New constructs a new Node.
func New(cfg *Config, self uint8) (*Node, error) {
	selfid := packet.NodeId(self)
	peermap, err := processConfig(cfg)
	if err != nil {
		return nil, err
	}
	if _, found := peermap[selfid]; !found {
		return nil, fmt.Errorf("missing self id %d", selfid)
	}
	return &Node{
		self:  selfid,
		peers: peermap,
	}, nil
}

// Start acquires the resources needed for this Node.
func (n *Node) Start() error {
	conn, err := net.ListenUDP("udp", n.peers[n.self].addr)
	if err != nil {
		return err
	}
	n.conn = conn
	n.state = follower
	n.time0 = 5 + uint32(rand.Intn(10))
	n.wg.Add(1)
	go n.loop()
	return nil
}

// Stop releases the resources associated with this Node.
func (n *Node) Stop() error {
	err := n.conn.Close()
	n.wg.Wait()
	return err
}

// Id returns the identifier for this node.
func (n *Node) Id() packet.NodeId {
	return n.self
}

// Quorum returns the number of peers required to form a quorum.
func (n *Node) Quorum() int {
	return len(n.peers)/2 + 1
}

// OnGainLeadership sets the callback which, if non-nil, will be executed when
// this node becomes the leader.
//
// Another node may believe it is still the leader for up to 150 ticks.
func (n *Node) OnGainLeadership(fn func(*Node)) {
	n.mutex.Lock()
	n.onGainLeadership = fn
	n.mutex.Unlock()
}

// OnLoseLeadership sets the callback which, if non-nil, will be executed when
// this node is no longer the leader.
func (n *Node) OnLoseLeadership(fn func(*Node)) {
	n.mutex.Lock()
	n.onLoseLeadership = fn
	n.mutex.Unlock()
}

// ForceElection forces a new leadership election, nominating this node as the
// proposed leader.
func (n *Node) ForceElection() {
	n.mutex.Lock()
	n.becomeCandidate()
	n.mutex.Unlock()
}

// Nominate proposes that the identified node ought to become the leader.
func (n *Node) Nominate(id packet.NodeId) {
	n.mutex.Lock()
	if id == n.self {
		if n.state != leader {
			n.becomeCandidate()
		}
		return
	}
	n.send(id, packet.NominateRequest{n.currentTerm})
	n.mutex.Unlock()
}

// String returns a condensed but human-readable summary of this node's state.
func (n *Node) String() string {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	var ch byte
	switch n.state {
	case follower:
		ch = 'F'

	case candidate:
		ch = 'C'

	case leader:
		ch = 'L'

	default:
		panic("unknown state")
	}
	return fmt.Sprintf("%d[%c %d %03d/%02d %s %d %d]", n.self, ch,
		n.currentTerm, n.time0, n.time1, n.yeaVotesToString(),
		n.votedFor, n.currentLeader)
}

func (n *Node) loop() {
	var buf [16]byte
	for {
		num, addr, err := n.conn.ReadFromUDP(buf[:])
		if err != nil {
			log.Printf("raft: %v", err)
			break
		}
		packet := packet.Unpack(buf[:num])
		if packet != nil {
			for _, peer := range n.peers {
				if equalUDPAddr(addr, peer.addr) {
					n.recv(peer.id, packet)
					break
				}
			}
		}
	}
	n.conn.Close()
	n.wg.Done()
}

var faultInjectorMutex sync.Mutex
var faultInjectorFunction func(from, to packet.NodeId) bool

// SetFaultInjectorFunction assigns a fault injector function.  If the fault
// injector function is non-nil, then it is called with the source and
// destination NodeId values.  If the function then returns true, a fault is
// injected and the packet is dropped.
func SetFaultInjectorFunction(fn func(from, to packet.NodeId) bool) {
	faultInjectorMutex.Lock()
	faultInjectorFunction = fn
	faultInjectorMutex.Unlock()
}

func (n *Node) send(to packet.NodeId, p packet.Packet) {
	faultInjectorMutex.Lock()
	fn := faultInjectorFunction
	faultInjectorMutex.Unlock()
	if fn != nil && fn(n.self, to) {
		return
	}
	buf := p.Pack()
	peer := n.peers[to]
	_, err := n.conn.WriteToUDP(buf, peer.addr)
	if err != nil {
		log.Printf("raft: %v", err)
	}
}

func (n *Node) sendNotVoted(p packet.Packet) {
	for _, peer := range n.peers {
		if _, found := n.yeaVotes[peer.id]; !found {
			n.send(peer.id, p)
		}
	}
}

func (n *Node) sendAllButSelfAndSource(from packet.NodeId, p packet.Packet) {
	for _, peer := range n.peers {
		if peer.id != n.self && peer.id != from {
			n.send(peer.id, p)
		}
	}
}

func (n *Node) recv(from packet.NodeId, p packet.Packet) {
	n.mutex.Lock()
	origState := n.state
	switch v := p.(type) {
	case packet.VoteRequest:
		n.recvVoteRequest(from, v)

	case packet.VoteResponse:
		n.recvVoteResponse(from, v)

	case packet.HeartbeatRequest:
		n.recvHeartbeatRequest(from, v)

	case packet.HeartbeatResponse:
		n.recvHeartbeatResponse(from, v)

	case packet.NominateRequest:
		n.recvNominateRequest(from, v)

	case packet.InformRequest:
		n.recvInformRequest(from, v)

	default:
		panic(fmt.Sprintf("unknown type %T", p))
	}
	var fn func(*Node)
	switch {
	case n.state == leader && origState != leader:
		fn = n.onGainLeadership
	case n.state != leader && origState == leader:
		fn = n.onLoseLeadership
	}
	n.mutex.Unlock()
	if fn != nil {
		fn(n)
	}
}

func (n *Node) recvVoteRequest(from packet.NodeId, r packet.VoteRequest) {
	granted := false
	switch {
	case r.Term > n.currentTerm:
		n.becomeFollower()
		n.votedFor = from
		n.currentTerm = r.Term
		granted = true

	case r.Term == n.currentTerm && (n.votedFor == 0 || n.votedFor == from):
		n.votedFor = from
		n.time0 = 50 + uint32(rand.Intn(100))
		granted = true
	}
	n.send(from, packet.VoteResponse{n.currentTerm, granted})
}

func (n *Node) recvVoteResponse(from packet.NodeId, r packet.VoteResponse) {
	switch {
	case r.Term > n.currentTerm:
		n.becomeFollower()
		n.currentTerm = r.Term

	case r.Term == n.currentTerm && n.state == candidate && r.Granted:
		n.yeaVotes[from] = struct{}{}
		if len(n.yeaVotes) >= n.Quorum() {
			n.state = leader
			n.currentLeader = n.self
			n.time0 = 50 + uint32(rand.Intn(100))
			n.time1 = 5 + uint32(rand.Intn(10))
			n.currentNonce = 0
			n.yeaVotes = make(map[packet.NodeId]struct{}, len(n.peers))
			n.yeaVotes[n.self] = struct{}{}
			n.sendNotVoted(packet.HeartbeatRequest{n.currentTerm, n.currentNonce})
		}
	}
}

func (n *Node) recvHeartbeatRequest(from packet.NodeId, r packet.HeartbeatRequest) {
	success := false
	switch {
	case r.Term > n.currentTerm:
		n.becomeFollower()
		n.votedFor = from
		n.currentLeader = from
		n.currentTerm = r.Term
		success = true

	case r.Term == n.currentTerm && n.state != leader:
		n.becomeFollower()
		n.votedFor = from
		n.currentLeader = from
		success = true
	}
	n.send(from, packet.HeartbeatResponse{n.currentTerm, r.Nonce, success})
	n.sendAllButSelfAndSource(from, packet.InformRequest{n.currentTerm, n.currentLeader})
}

func (n *Node) recvHeartbeatResponse(from packet.NodeId, r packet.HeartbeatResponse) {
	switch {
	case r.Term > n.currentTerm:
		n.becomeFollower()
		n.currentTerm = r.Term

	case r.Term == n.currentTerm && n.state == leader && r.Nonce == n.currentNonce && r.Success:
		n.yeaVotes[from] = struct{}{}
		if len(n.yeaVotes) >= n.Quorum() {
			n.currentNonce += 1
			n.time0 = 50 + uint32(rand.Intn(100))
		}
	}
}

func (n *Node) recvNominateRequest(from packet.NodeId, r packet.NominateRequest) {
	switch {
	case r.Term > n.currentTerm:
		n.becomeFollower()
		n.currentTerm = r.Term
		n.becomeCandidate()

	case r.Term == n.currentTerm && n.state == follower:
		n.becomeCandidate()
	}
}

func (n *Node) recvInformRequest(from packet.NodeId, r packet.InformRequest) {
	switch {
	case r.Term > n.currentTerm:
		n.becomeFollower()
		n.currentTerm = r.Term
		n.currentLeader = r.Leader
		n.send(from, packet.NominateRequest{n.currentTerm})

	case r.Term == n.currentTerm && n.currentLeader != r.Leader:
		n.becomeFollower()
		n.currentLeader = r.Leader
		n.send(from, packet.NominateRequest{n.currentTerm})
	}
}

func (n *Node) Tick() {
	n.mutex.Lock()
	origState := n.state
	if n.time0 > 0 {
		n.time0 -= 1
	}
	if n.time1 > 0 {
		n.time1 -= 1
	}
	switch {
	case n.time0 == 0:
		n.becomeCandidate()

	case n.state == candidate && n.time1 == 0:
		n.time1 = 5 + uint32(rand.Intn(10))
		n.sendNotVoted(packet.VoteRequest{n.currentTerm})

	case n.state == leader && n.time1 == 0:
		n.currentNonce += 1
		n.time1 = 5 + uint32(rand.Intn(10))
		n.yeaVotes = make(map[packet.NodeId]struct{}, len(n.peers))
		n.yeaVotes[n.self] = struct{}{}
		n.sendNotVoted(packet.HeartbeatRequest{n.currentTerm, n.currentNonce})
	}
	var fn func(*Node)
	switch {
	case n.state == leader && origState != leader:
		fn = n.onGainLeadership
	case n.state != leader && origState == leader:
		fn = n.onLoseLeadership
	}
	n.mutex.Unlock()
	if fn != nil {
		fn(n)
	}
}

func (n *Node) becomeFollower() {
	n.state = follower
	n.votedFor = 0
	n.currentLeader = 0
	n.time0 = 50 + uint32(rand.Intn(100))
	n.time1 = 0
	n.currentNonce = 0
	n.yeaVotes = nil
}

func (n *Node) becomeCandidate() {
	n.state = candidate
	n.currentLeader = 0
	n.currentTerm += 1
	n.votedFor = n.self
	n.time0 = 50 + uint32(rand.Intn(100))
	n.time1 = 5 + uint32(rand.Intn(10))
	n.currentNonce = 0
	n.yeaVotes = make(map[packet.NodeId]struct{}, len(n.peers))
	n.yeaVotes[n.self] = struct{}{}
	if len(n.yeaVotes) >= n.Quorum() {
		n.state = leader
		n.sendNotVoted(packet.HeartbeatRequest{n.currentTerm, n.currentNonce})
	} else {
		n.sendNotVoted(packet.VoteRequest{n.currentTerm})
	}
}

func equalUDPAddr(a, b *net.UDPAddr) bool {
	return a.IP.Equal(b.IP) && a.Port == b.Port && a.Zone == b.Zone
}

type byId []packet.NodeId

func (x byId) Len() int           { return len(x) }
func (x byId) Less(i, j int) bool { return x[i] < x[j] }
func (x byId) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func (n *Node) yeaVotesToString() string {
	idList := make([]packet.NodeId, 0, len(n.peers))
	for id := range n.peers {
		idList = append(idList, id)
	}
	sort.Sort(byId(idList))
	var buf bytes.Buffer
	for _, id := range idList {
		if _, found := n.yeaVotes[id]; found {
			fmt.Fprintf(&buf, "%d", id)
		} else {
			buf.Write([]byte("."))
		}
	}
	return buf.String()
}

func processConfig(cfg *Config) (map[packet.NodeId]peer, error) {
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("must configure at least one Raft node")
	}
	result := make(map[packet.NodeId]peer, len(cfg.Nodes))
	for _, item := range cfg.Nodes {
		id := packet.NodeId(item.Id)
		if id == 0 {
			return nil, fmt.Errorf("invalid id: 0")
		}
		_, found := result[id]
		if found {
			return nil, fmt.Errorf("duplicate id: %d", id)
		}
		addr, err := net.ResolveUDPAddr("udp", item.Addr)
		if err != nil {
			return nil, err
		}
		result[id] = peer{id, addr}
	}
	return result, nil
}

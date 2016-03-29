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

// Raft represents a single participant in a Raft network.
type Raft struct {
	conn             *net.UDPConn
	self             packet.PeerId
	wg               sync.WaitGroup
	mutex            sync.Mutex
	peers            map[packet.PeerId]*net.UDPAddr
	state            state
	votedFor         packet.PeerId
	currentLeader    packet.PeerId
	currentTerm      packet.Term
	lastLeaderTerm   packet.Term
	time0            uint32
	time1            uint32
	currentNonce     uint32
	yeaVotes         map[packet.PeerId]struct{}
	inLonelyState    bool
	onGainLeadership func(*Raft)
	onLoseLeadership func(*Raft)
	onLonely         func(*Raft)
	onNotLonely      func(*Raft)
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

// New constructs a new Raft.
func New(cfg *Config, self uint8) (*Raft, error) {
	selfid := packet.PeerId(self)
	peermap, err := processConfig(cfg)
	if err != nil {
		return nil, err
	}
	if _, found := peermap[selfid]; !found {
		return nil, fmt.Errorf("missing self id %d", selfid)
	}
	return &Raft{self: selfid, peers: peermap}, nil
}

// Start acquires the resources needed for this Raft.
func (raft *Raft) Start() error {
	conn, err := net.ListenUDP("udp", raft.peers[raft.self])
	if err != nil {
		return err
	}
	raft.conn = conn
	raft.state = follower
	raft.time0 = 5 + uint32(rand.Intn(10))
	raft.wg.Add(1)
	go raft.loop()
	return nil
}

// Stop releases the resources associated with this Raft.
func (raft *Raft) Stop() error {
	err := raft.conn.Close()
	raft.wg.Wait()
	return err
}

func (raft *Raft) AddPeer(peerId uint8, peerAddr string) error {
	id := packet.PeerId(peerId)
	addr, err := net.ResolveUDPAddr("udp", peerAddr)
	if err != nil {
		return err
	}
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	if id == 0 {
		return fmt.Errorf("invalid id: 0")
	}
	if _, found := raft.peers[id]; found {
		return fmt.Errorf("duplicate id: %d", id)
	}
	raft.peers[id] = addr
	return nil
}

func (raft *Raft) RemovePeer(peerId uint8) error {
	id := packet.PeerId(peerId)
	if id == raft.self {
		return fmt.Errorf("cannot remove self id: %d", id)
	}
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	if _, found := raft.peers[id]; !found {
		return fmt.Errorf("no such id: %d", id)
	}
	delete(raft.peers, id)
	delete(raft.yeaVotes, id)
	return nil
}

// Id returns the identifier for this node.
func (raft *Raft) Id() packet.PeerId {
	return raft.self
}

// Quorum returns the number of peers required to form a quorum.
func (raft *Raft) Quorum() int {
	return len(raft.peers)/2 + 1
}

// OnGainLeadership sets the callback which, if non-nil, will be executed when
// this node becomes the leader.
//
// Another node may believe it is still the leader for up to 150 ticks.
func (raft *Raft) OnGainLeadership(fn func(*Raft)) {
	raft.mutex.Lock()
	raft.onGainLeadership = fn
	raft.mutex.Unlock()
}

// OnLoseLeadership sets the callback which, if non-nil, will be executed when
// this node is no longer the leader.
func (raft *Raft) OnLoseLeadership(fn func(*Raft)) {
	raft.mutex.Lock()
	raft.onLoseLeadership = fn
	raft.mutex.Unlock()
}

func (raft *Raft) OnLonely(fn func(*Raft)) {
	raft.mutex.Lock()
	raft.onLonely = fn
	raft.mutex.Unlock()
}

func (raft *Raft) OnNotLonely(fn func(*Raft)) {
	raft.mutex.Lock()
	raft.onNotLonely = fn
	raft.mutex.Unlock()
}

// ForceElection forces a new leadership election, nominating this node as the
// proposed leader.
func (raft *Raft) ForceElection() {
	raft.mutex.Lock()
	raft.becomeCandidate()
	raft.mutex.Unlock()
}

// Nominate proposes that the identified node ought to become the leader.
func (raft *Raft) Nominate(id packet.PeerId) {
	raft.mutex.Lock()
	if id == raft.self {
		if raft.state != leader {
			raft.becomeCandidate()
		}
		return
	}
	raft.send(id, packet.NominateRequest{raft.currentTerm})
	raft.mutex.Unlock()
}

// String returns a condensed but human-readable summary of this node's state.
func (raft *Raft) String() string {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	var ch byte
	switch raft.state {
	case follower:
		ch = 'F'

	case candidate:
		ch = 'C'

	case leader:
		ch = 'L'

	default:
		panic("unknown state")
	}
	return fmt.Sprintf("%d[%c %d %03d/%02d %s %d %d]", raft.self, ch,
		raft.currentTerm, raft.time0, raft.time1,
		raft.yeaVotesToString(), raft.votedFor, raft.currentLeader)
}

func (raft *Raft) loop() {
	var buf [16]byte
	for {
		size, pktAddr, err := raft.conn.ReadFromUDP(buf[:])
		if err != nil {
			operr, ok := err.(*net.OpError)
			if !ok || operr.Op != "read" || operr.Err.Error() != "use of closed network connection" {
				log.Printf("raft: %#v", err)
			}
			break
		}
		pkt := packet.Unpack(buf[:size])
		if pkt != nil {
			var pktId packet.PeerId
			raft.mutex.Lock()
			for id, addr := range raft.peers {
				if equalUDPAddr(pktAddr, addr) {
					pktId = id
					break
				}
			}
			raft.mutex.Unlock()
			if pktId != 0 {
				raft.recv(pktId, pkt)
			}
		}
	}
	raft.conn.Close()
	raft.wg.Done()
}

var faultInjectorMutex sync.Mutex
var faultInjectorFunction func(from, to packet.PeerId) bool

// SetFaultInjectorFunction assigns a fault injector function.  If the fault
// injector function is non-nil, then it is called with the source and
// destination PeerId values.  If the function then returns true, a fault is
// injected and the packet is dropped.
func SetFaultInjectorFunction(fn func(from, to packet.PeerId) bool) {
	faultInjectorMutex.Lock()
	faultInjectorFunction = fn
	faultInjectorMutex.Unlock()
}

func (raft *Raft) send(to packet.PeerId, p packet.Packet) {
	faultInjectorMutex.Lock()
	fn := faultInjectorFunction
	faultInjectorMutex.Unlock()
	if fn != nil && fn(raft.self, to) {
		return
	}
	buf := p.Pack()
	addr := raft.peers[to]
	_, err := raft.conn.WriteToUDP(buf, addr)
	if err != nil {
		log.Printf("raft: %v", err)
	}
}

func (raft *Raft) sendNotVoted(p packet.Packet) {
	for id := range raft.peers {
		if _, found := raft.yeaVotes[id]; !found {
			raft.send(id, p)
		}
	}
}

func (raft *Raft) sendAllButSelfAndSource(from packet.PeerId, p packet.Packet) {
	for id := range raft.peers {
		if id != raft.self && id != from {
			raft.send(id, p)
		}
	}
}

func (raft *Raft) recv(from packet.PeerId, p packet.Packet) {
	raft.mutex.Lock()
	origState := raft.state
	switch v := p.(type) {
	case packet.VoteRequest:
		raft.recvVoteRequest(from, v)

	case packet.VoteResponse:
		raft.recvVoteResponse(from, v)

	case packet.HeartbeatRequest:
		raft.recvHeartbeatRequest(from, v)

	case packet.HeartbeatResponse:
		raft.recvHeartbeatResponse(from, v)

	case packet.NominateRequest:
		raft.recvNominateRequest(from, v)

	case packet.InformRequest:
		raft.recvInformRequest(from, v)

	default:
		panic(fmt.Sprintf("unknown type %T", p))
	}
	var fns []func(*Raft)
	if raft.inLonelyState && raft.currentLeader != 0 {
		fns = append(fns, raft.onNotLonely)
		raft.inLonelyState = false
	}
	switch {
	case raft.state == leader && origState != leader:
		fns = append(fns, raft.onGainLeadership)
	case raft.state != leader && origState == leader:
		fns = append(fns, raft.onLoseLeadership)
	}
	raft.mutex.Unlock()
	for _, fn := range fns {
		fn(raft)
	}
}

func (raft *Raft) recvVoteRequest(from packet.PeerId, pkt packet.VoteRequest) {
	granted := false
	switch {
	case pkt.Term > raft.currentTerm:
		raft.becomeFollower()
		raft.votedFor = from
		raft.currentTerm = pkt.Term
		granted = true

	case pkt.Term == raft.currentTerm && (raft.votedFor == 0 || raft.votedFor == from):
		raft.votedFor = from
		raft.time0 = 50 + uint32(rand.Intn(100))
		granted = true
	}
	raft.send(from, packet.VoteResponse{raft.currentTerm, granted})
}

func (raft *Raft) recvVoteResponse(from packet.PeerId, pkt packet.VoteResponse) {
	switch {
	case pkt.Term > raft.currentTerm:
		raft.becomeFollower()
		raft.currentTerm = pkt.Term

	case pkt.Term == raft.currentTerm && raft.state == candidate && pkt.Granted:
		raft.yeaVotes[from] = struct{}{}
		if len(raft.yeaVotes) >= raft.Quorum() {
			raft.state = leader
			raft.currentLeader = raft.self
			raft.lastLeaderTerm = raft.currentTerm
			raft.time0 = 50 + uint32(rand.Intn(100))
			raft.time1 = 5 + uint32(rand.Intn(10))
			raft.currentNonce = 0
			raft.yeaVotes = make(map[packet.PeerId]struct{}, len(raft.peers))
			raft.yeaVotes[raft.self] = struct{}{}
			raft.sendNotVoted(packet.HeartbeatRequest{raft.currentTerm, raft.currentNonce})
		}
	}
}

func (raft *Raft) recvHeartbeatRequest(from packet.PeerId, pkt packet.HeartbeatRequest) {
	success := false
	switch {
	case pkt.Term > raft.currentTerm:
		raft.becomeFollower()
		raft.votedFor = from
		raft.currentLeader = from
		raft.currentTerm = pkt.Term
		raft.lastLeaderTerm = raft.currentTerm
		success = true

	case pkt.Term == raft.currentTerm && raft.state != leader:
		raft.becomeFollower()
		raft.votedFor = from
		raft.currentLeader = from
		raft.lastLeaderTerm = raft.currentTerm
		success = true
	}
	raft.send(from, packet.HeartbeatResponse{raft.currentTerm, pkt.Nonce, success})
	raft.sendAllButSelfAndSource(from, packet.InformRequest{raft.currentTerm, raft.currentLeader})
}

func (raft *Raft) recvHeartbeatResponse(from packet.PeerId, pkt packet.HeartbeatResponse) {
	switch {
	case pkt.Term > raft.currentTerm:
		raft.becomeFollower()
		raft.currentTerm = pkt.Term

	case pkt.Term == raft.currentTerm && raft.state == leader && pkt.Nonce == raft.currentNonce && pkt.Success:
		raft.yeaVotes[from] = struct{}{}
		if len(raft.yeaVotes) >= raft.Quorum() {
			raft.currentNonce += 1
			raft.time0 = 50 + uint32(rand.Intn(100))
		}
	}
}

func (raft *Raft) recvNominateRequest(from packet.PeerId, pkt packet.NominateRequest) {
	switch {
	case pkt.Term > raft.currentTerm:
		raft.becomeFollower()
		raft.currentTerm = pkt.Term
		raft.becomeCandidate()

	case pkt.Term == raft.currentTerm && raft.state == follower:
		raft.becomeCandidate()
	}
}

func (raft *Raft) recvInformRequest(from packet.PeerId, pkt packet.InformRequest) {
	switch {
	case pkt.Term > raft.currentTerm:
		raft.becomeFollower()
		raft.currentTerm = pkt.Term
		raft.currentLeader = pkt.Leader
		raft.lastLeaderTerm = raft.currentTerm
		raft.send(from, packet.NominateRequest{raft.currentTerm})

	case pkt.Term == raft.currentTerm && raft.currentLeader != pkt.Leader:
		raft.becomeFollower()
		raft.currentLeader = pkt.Leader
		raft.lastLeaderTerm = raft.currentTerm
		raft.send(from, packet.NominateRequest{raft.currentTerm})
	}
}

func (raft *Raft) Tick() {
	raft.mutex.Lock()
	origState := raft.state
	if raft.time0 > 0 {
		raft.time0 -= 1
	}
	if raft.time1 > 0 {
		raft.time1 -= 1
	}
	switch {
	case raft.time0 == 0:
		raft.becomeCandidate()

	case raft.state == candidate && raft.time1 == 0:
		raft.time1 = 5 + uint32(rand.Intn(10))
		raft.sendNotVoted(packet.VoteRequest{raft.currentTerm})

	case raft.state == leader && raft.time1 == 0:
		raft.currentNonce += 1
		raft.time1 = 5 + uint32(rand.Intn(10))
		raft.yeaVotes = make(map[packet.PeerId]struct{}, len(raft.peers))
		raft.yeaVotes[raft.self] = struct{}{}
		raft.sendNotVoted(packet.HeartbeatRequest{raft.currentTerm, raft.currentNonce})
	}
	var fn func(*Raft)
	switch {
	case raft.state == leader && origState != leader:
		fn = raft.onGainLeadership
	case raft.state != leader && origState == leader:
		fn = raft.onLoseLeadership
	case raft.currentTerm >= raft.lastLeaderTerm+2 && !raft.inLonelyState:
		fn = raft.onLonely
		raft.inLonelyState = true
	}
	raft.mutex.Unlock()
	if fn != nil {
		fn(raft)
	}
}

func (raft *Raft) becomeFollower() {
	raft.state = follower
	raft.votedFor = 0
	raft.currentLeader = 0
	raft.time0 = 50 + uint32(rand.Intn(100))
	raft.time1 = 0
	raft.currentNonce = 0
	raft.yeaVotes = nil
}

func (raft *Raft) becomeCandidate() {
	raft.state = candidate
	raft.currentLeader = 0
	raft.currentTerm += 1
	raft.votedFor = raft.self
	raft.time0 = 50 + uint32(rand.Intn(100))
	raft.time1 = 5 + uint32(rand.Intn(10))
	raft.currentNonce = 0
	raft.yeaVotes = make(map[packet.PeerId]struct{}, len(raft.peers))
	raft.yeaVotes[raft.self] = struct{}{}
	if len(raft.yeaVotes) >= raft.Quorum() {
		raft.state = leader
		raft.sendNotVoted(packet.HeartbeatRequest{raft.currentTerm, raft.currentNonce})
	} else {
		raft.sendNotVoted(packet.VoteRequest{raft.currentTerm})
	}
}

func equalUDPAddr(a, b *net.UDPAddr) bool {
	return a.IP.Equal(b.IP) && a.Port == b.Port && a.Zone == b.Zone
}

type byId []packet.PeerId

func (x byId) Len() int           { return len(x) }
func (x byId) Less(i, j int) bool { return x[i] < x[j] }
func (x byId) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

func (raft *Raft) yeaVotesToString() string {
	idList := make([]packet.PeerId, 0, len(raft.peers))
	for id := range raft.peers {
		idList = append(idList, id)
	}
	sort.Sort(byId(idList))
	var buf bytes.Buffer
	for _, id := range idList {
		if _, found := raft.yeaVotes[id]; found {
			fmt.Fprintf(&buf, "%d", id)
		} else {
			buf.Write([]byte("."))
		}
	}
	return buf.String()
}

func processConfig(cfg *Config) (map[packet.PeerId]*net.UDPAddr, error) {
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("must configure at least one Raft node")
	}
	result := make(map[packet.PeerId]*net.UDPAddr, len(cfg.Peers))
	for _, item := range cfg.Peers {
		id := packet.PeerId(item.Id)
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
		result[id] = addr
	}
	return result, nil
}

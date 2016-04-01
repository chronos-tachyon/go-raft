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

	"github.com/golang/protobuf/proto"

	pb "github.com/chronos-tachyon/go-raft/proto"
)

type state uint8

const (
	follower state = iota
	candidate
	leader
)

type peerData struct {
	addr       *net.UDPAddr
	yeaVote    bool
	nextIndex  uint64
	matchIndex uint64
}

// Raft represents a single participant in a Raft cluster.
type Raft struct {
	selfId           uint32
	rand             *rand.Rand
	storage          Storage
	conn             *net.UDPConn
	wg               sync.WaitGroup
	mutex            sync.Mutex
	peers            map[uint32]*peerData
	sm               StateMachine
	log              *raftLog
	state            state
	isSolitary       bool
	inLonelyState    bool
	votedForId       uint32
	currentLeaderId  uint32
	electionTimer    uint32
	heartbeatTimer   uint32
	currentTerm      uint64
	lastLeaderTerm   uint64
	onGainLeadership func(*Raft)
	onLoseLeadership func(*Raft)
	onLonely         func(*Raft)
	onNotLonely      func(*Raft)
}

/*
 * follower:
 *	electionTimer	ticks until become candidate
 *	heartbeatTimer	unused (0)
 *	yeaVotes	unused (nil)
 *
 * candidate:
 *	electionTimer	ticks until bump currentTerm and try again
 *	heartbeatTimer	ticks until retransmit VoteRequests
 *	yeaVotes	set of yea votes to become leader
 *
 * leader:
 *	electionTimer	unused (temporarily; will be: ticks until drop leadership)
 *	heartbeatTimer	ticks until heartbeat sent
 *	yeaVotes	set of yea votes to retain leadership
 */

// New constructs a new Raft.
func New(storage Storage, sm StateMachine, id uint32, addr string) (*Raft, error) {
	if id == 0 {
		return nil, fmt.Errorf("invalid id: 0")
	}
	udpaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return &Raft{
		selfId:  id,
		rand:    rand.New(rand.NewSource(0xdeadbeef ^ int64(id))),
		storage: storage,
		peers:   map[uint32]*peerData{id: &peerData{addr: udpaddr}},
		sm:      sm,
		log:     &raftLog{},
	}, nil
}

// Solitary marks this Raft as willing to operate without any peers.
// Rafts normally operate in clusters of 3 or 5.
func (raft *Raft) Solitary(b bool) *Raft {
	raft.isSolitary = b
	return raft
}

// Start launches this Raft.
func (raft *Raft) Start() error {
	if err := raft.restoreState(); err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", raft.peers[raft.selfId].addr)
	if err != nil {
		return err
	}
	raft.conn = conn
	raft.state = follower
	raft.electionTimer = 2 * raft.newHeartbeatTimer()
	raft.wg.Add(1)
	go raft.loop()
	return nil
}

// Stop halts this Raft and releases resources.
func (raft *Raft) Stop() error {
	err := raft.conn.Close()
	raft.wg.Wait()
	raft.conn = nil
	return err
}

// Id returns the identifier for this Raft.
func (raft *Raft) Id() uint32 {
	return raft.selfId
}

// Addr returns this Raft's UDP address.
func (raft *Raft) Addr() *net.UDPAddr {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	return raft.peers[raft.selfId].addr
}

// Peer returns the UDP address of the Raft with the given ID.
func (raft *Raft) Peer(id uint32) *net.UDPAddr {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	peer, found := raft.peers[id]
	if !found {
		return nil
	}
	return peer.addr
}

// PeerIds returns the IDs of all current peers.
func (raft *Raft) PeerIds() []uint32 {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	list := make([]uint32, 0, len(raft.peers)-1)
	for id := range raft.peers {
		if id != raft.selfId {
			list = append(list, id)
		}
	}
	sort.Sort(byId(list))
	return list
}

// Quorum returns the number of Rafts required to form a quorum.
func (raft *Raft) Quorum() int {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	return raft.quorumLocked()
}

func (raft *Raft) quorumLocked() int {
	assert(len(raft.peers) > 0, "raft.peers is empty")
	if len(raft.peers) == 1 {
		if raft.isSolitary {
			return 1
		}
		return 2
	}
	return len(raft.peers)/2 + 1
}

// IsLeader returns true if this Raft is *probably* the leader.
// (The information may be stale.)
func (raft *Raft) IsLeader() bool {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	return raft.state == leader
}

func (raft *Raft) AddPeer(id uint32, addr string) error {
	if id == 0 {
		return fmt.Errorf("invalid id: 0")
	}
	if id == raft.selfId {
		return fmt.Errorf("duplicate id: %d", id)
	}
	udpaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	if _, found := raft.peers[id]; found {
		return fmt.Errorf("duplicate id: %d", id)
	}
	peer := &peerData{addr: udpaddr}
	if raft.state == leader {
		peer.nextIndex = raft.log.nextIndex()
	}
	raft.peers[id] = peer
	return nil
}

func (raft *Raft) RemovePeer(id uint32) error {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	if _, found := raft.peers[id]; !found {
		return fmt.Errorf("no such id: %d", id)
	}
	delete(raft.peers, id)
	return nil
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
func (raft *Raft) Nominate(id uint32) {
	raft.mutex.Lock()
	if id == raft.selfId {
		if raft.state != leader {
			raft.becomeCandidate()
		}
	} else {
		raft.send(id, &pb.NominateRequest{
			Term: raft.currentTerm,
		})
	}
	raft.mutex.Unlock()
}

func (raft *Raft) Append(cmd []byte, cb func(ok bool)) {
	assert(len(cmd) < 256, "command too long")
	raft.mutex.Lock()
	if raft.state == leader {
		raft.log.appendEntry(raft.currentTerm, cmd, cb)
	} else {
		if cb != nil {
			cb(false)
		}
	}
	raft.mutex.Unlock()
}

func (raft *Raft) yeaVotes() int {
	result := 0
	for _, peer := range raft.peers {
		if peer.yeaVote {
			result++
		}
	}
	return result
}

// String returns a condensed but human-readable summary of this node's state.
func (raft *Raft) String() string {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	return raft.stringLocked()
}

func (raft *Raft) stringLocked() string {
	var ch byte
	switch raft.state {
	case follower:
		ch = 'F'

	case candidate:
		ch = 'C'

	case leader:
		ch = 'L'

	default:
		assert(false, "unknown state")
	}
	var pd string
	for _, id := range raft.idList() {
		if id != raft.selfId {
			peer := raft.peers[id]
			pd += fmt.Sprintf("%d:%d,%d ", id, peer.nextIndex, peer.matchIndex)
		}
	}
	if len(pd) > 0 {
		pd = pd[0 : len(pd)-1]
	}
	return fmt.Sprintf("%d[%c %d %03d/%02d %s %d %d] (%s) {%v} <%v>", raft.selfId, ch,
		raft.currentTerm, raft.electionTimer, raft.heartbeatTimer,
		raft.yeaVotesToString(), raft.votedForId, raft.currentLeaderId,
		pd, raft.sm, raft.log)
}

func (raft *Raft) yeaVotesToString() string {
	var buf bytes.Buffer
	for _, id := range raft.idList() {
		if raft.peers[id].yeaVote {
			fmt.Fprintf(&buf, "%d", id)
		} else {
			buf.Write([]byte("."))
		}
	}
	return buf.String()
}

func (raft *Raft) idList() []uint32 {
	list := make([]uint32, 0, len(raft.peers))
	for id := range raft.peers {
		list = append(list, id)
	}
	sort.Sort(byId(list))
	return list
}

func (raft *Raft) loop() {
	var buf [1024]byte
	for {
		size, fromAddr, err := raft.conn.ReadFromUDP(buf[:])
		if err != nil {
			operr, ok := err.(*net.OpError)
			if !ok || operr.Op != "read" || operr.Err.Error() != "use of closed network connection" {
				log.Printf("go-raft: %v", err)
			}
			break
		}
		var packet pb.Packet
		err = unpackData(buf[:size], &packet)
		if err != nil {
			log.Printf("go-raft: %v", err)
			continue
		}
		var from uint32
		raft.mutex.Lock()
		for id, peer := range raft.peers {
			if equalUDPAddr(fromAddr, peer.addr) {
				from = id
				break
			}
		}
		raft.mutex.Unlock()
		if from != 0 {
			raft.recv(from, packet)
		}
	}
	raft.conn.Close()
	raft.wg.Done()
}

var faultInjectorMutex sync.Mutex
var faultInjectorFunction func(from, to uint32) bool

// SetFaultInjectorFunction assigns a fault injector function.  If the fault
// injector function is non-nil, then it is called with the source and
// destination peer IDs.  If the function then returns true, a fault is
// injected and the packet is dropped.
func SetFaultInjectorFunction(fn func(from, to uint32) bool) {
	faultInjectorMutex.Lock()
	faultInjectorFunction = fn
	faultInjectorMutex.Unlock()
}

func (raft *Raft) send(to uint32, msg proto.Message) {
	faultInjectorMutex.Lock()
	fn := faultInjectorFunction
	faultInjectorMutex.Unlock()
	if fn != nil && fn(raft.selfId, to) {
		return
	}

	var packet pb.Packet
	switch msg.(type) {
	case *pb.VoteRequest:
		packet.Type = pb.Packet_VOTE_REQUEST
	case *pb.VoteResponse:
		packet.Type = pb.Packet_VOTE_RESPONSE
	case *pb.AppendEntriesRequest:
		packet.Type = pb.Packet_APPEND_ENTRIES_REQUEST
	case *pb.AppendEntriesResponse:
		packet.Type = pb.Packet_APPEND_ENTRIES_RESPONSE
	case *pb.NominateRequest:
		packet.Type = pb.Packet_NOMINATE_REQUEST
	case *pb.InformRequest:
		packet.Type = pb.Packet_INFORM_REQUEST
	default:
		panic(fmt.Errorf("unsupported message type %T", msg))
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	packet.Payload = data

	data, err = packData(&packet)
	if err != nil {
		panic(err)
	}
	peer := raft.peers[to]
	_, err = raft.conn.WriteToUDP(data, peer.addr)
	if err != nil {
		log.Printf("go-raft: %v", err)
	}
}

func (raft *Raft) sendVoteRequests() {
	latestIndex := raft.log.latestIndex()
	latestTerm := raft.log.term(latestIndex)
	for id, peer := range raft.peers {
		if !peer.yeaVote {
			raft.send(id, &pb.VoteRequest{
				Term:        raft.currentTerm,
				LatestTerm:  latestTerm,
				LatestIndex: latestIndex,
			})
		}
	}
}

func (raft *Raft) sendAppendEntries() {
	for _, id := range raft.idList() {
		if id == raft.selfId {
			continue
		}
		peer := raft.peers[id]
		latestIndex := raft.log.latestIndex()
		peerLatestIndex := peer.nextIndex - 1
		peerLatestTerm := raft.log.term(peerLatestIndex)
		assert(peerLatestIndex <= latestIndex, "peerLatestIndex=%d > latestIndex=%d", peerLatestIndex, latestIndex)
		n := min(latestIndex, peerLatestIndex+8)
		numEntries := n - peerLatestIndex
		entries := make([]*pb.AppendEntry, 0, numEntries)
		for index := peerLatestIndex + 1; index <= n; index++ {
			entry := raft.log.at(index)
			entries = append(entries, &pb.AppendEntry{
				Term:    entry.term,
				Command: entry.command,
			})
		}
		commitIndex := min(raft.log.commitIndex, peerLatestIndex+uint64(len(entries)))
		raft.send(id, &pb.AppendEntriesRequest{
			Term:              raft.currentTerm,
			PrevLogTerm:       peerLatestTerm,
			PrevLogIndex:      peerLatestIndex,
			LeaderCommitIndex: commitIndex,
			Entries:           entries,
		})
	}
}

func (raft *Raft) recv(from uint32, pkt pb.Packet) {
	raft.mutex.Lock()
	origState := raft.state
	switch pkt.Type {
	case pb.Packet_VOTE_REQUEST:
		var v pb.VoteRequest
		mustUnmarshalProto(pkt.Payload, &v)
		raft.recvVoteRequest(from, &v)

	case pb.Packet_VOTE_RESPONSE:
		var v pb.VoteResponse
		mustUnmarshalProto(pkt.Payload, &v)
		raft.recvVoteResponse(from, &v)

	case pb.Packet_APPEND_ENTRIES_REQUEST:
		var v pb.AppendEntriesRequest
		mustUnmarshalProto(pkt.Payload, &v)
		raft.recvAppendEntriesRequest(from, &v)

	case pb.Packet_APPEND_ENTRIES_RESPONSE:
		var v pb.AppendEntriesResponse
		mustUnmarshalProto(pkt.Payload, &v)
		raft.recvAppendEntriesResponse(from, &v)

	case pb.Packet_NOMINATE_REQUEST:
		var v pb.NominateRequest
		mustUnmarshalProto(pkt.Payload, &v)
		raft.recvNominateRequest(from, &v)

	case pb.Packet_INFORM_REQUEST:
		var v pb.InformRequest
		mustUnmarshalProto(pkt.Payload, &v)
		raft.recvInformRequest(from, &v)

	default:
		assert(false, "unknown packet type %#02x", pkt.Type)
	}
	var fns []func(*Raft)
	if raft.inLonelyState && raft.currentLeaderId != 0 {
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

func (raft *Raft) recvVoteRequest(from uint32, pkt *pb.VoteRequest) {
	latestIndex := raft.log.latestIndex()
	latestTerm := raft.log.term(latestIndex)
	logIsOK := (pkt.LatestTerm > latestTerm || (pkt.LatestTerm == latestTerm && pkt.LatestIndex >= latestIndex))
	alreadyVoted := (pkt.Term == raft.currentTerm && raft.votedForId != 0 && raft.votedForId != from)
	if pkt.Term > raft.currentTerm {
		raft.becomeFollower(pkt.Term, from, 0)
	}
	granted := (pkt.Term == raft.currentTerm && logIsOK && !alreadyVoted)
	raft.send(from, &pb.VoteResponse{
		Term:    raft.currentTerm,
		Granted: granted,
		LogIsOk: logIsOK,
	})
}

func (raft *Raft) recvVoteResponse(from uint32, pkt *pb.VoteResponse) {
	if pkt.Term < raft.currentTerm {
		return
	}
	if pkt.Term > raft.currentTerm {
		raft.becomeFollower(pkt.Term, 0, 0)
		return
	}
	if raft.state != candidate {
		return
	}
	if pkt.Granted {
		raft.peers[from].yeaVote = true
		if raft.yeaVotes() >= raft.quorumLocked() {
			raft.becomeLeader()
		}
	} else {
		if !pkt.LogIsOk {
			raft.becomeFollower(pkt.Term, 0, 0)
			raft.send(from, &pb.NominateRequest{
				Term: raft.currentTerm,
			})
		}
	}
}

func (raft *Raft) recvAppendEntriesRequest(from uint32, pkt *pb.AppendEntriesRequest) {
	entries := pkt.Entries
	firstIndex := pkt.PrevLogIndex + 1
	lastCommittedIndex := min(firstIndex+uint64(len(entries)), raft.log.commitIndex)

	if pkt.Term < raft.currentTerm {
		goto Reject
	}
	if raft.state == leader && pkt.Term == raft.currentTerm {
		log.Printf("go-raft: AppendEntriesRequest sent to leader")
		goto Reject
	}

	raft.becomeFollower(pkt.Term, from, from)
	raft.log.checkInvariants()
	if pkt.PrevLogIndex >= raft.log.nextIndex() {
		goto Reject
	}
	if pkt.PrevLogTerm != raft.log.term(pkt.PrevLogIndex) {
		goto Reject
	}
	for firstIndex < lastCommittedIndex {
		if firstIndex >= raft.log.startIndex && firstIndex < raft.log.nextIndex() {
			if raft.log.term(pkt.PrevLogIndex) != entries[0].Term {
				goto Reject
			}
			assert(bytes.Equal(raft.log.at(pkt.PrevLogIndex).command, entries[0].Command), "")
		}
		firstIndex++
		entries = entries[1:]
	}
	for i, entry := range entries {
		index := firstIndex + uint64(i)
		if index >= raft.log.startIndex {
			if index < raft.log.nextIndex() && raft.log.term(index) != entry.Term {
				raft.log.deleteEntriesAfter(index - 1)
				assert(raft.log.nextIndex() == index, "new item index mismatch: expected %d got %d", raft.log.nextIndex(), index)
			}
			raft.log.appendEntry(entry.Term, entry.Command, nil)
		}
	}
	raft.commitTo(pkt.LeaderCommitIndex)
	raft.send(from, &pb.AppendEntriesResponse{
		Success:     true,
		Term:        raft.currentTerm,
		LatestIndex: raft.log.latestIndex(),
		CommitIndex: raft.log.commitIndex,
	})
	for _, id := range raft.idList() {
		if id != raft.selfId && id != from {
			raft.send(id, &pb.InformRequest{
				Term:     raft.currentTerm,
				LeaderId: uint32(raft.currentLeaderId),
			})
		}
	}
	return

Reject:
	raft.send(from, &pb.AppendEntriesResponse{
		Success:     false,
		Term:        raft.currentTerm,
		LatestIndex: raft.log.latestIndex(),
		CommitIndex: raft.log.commitIndex,
	})
}

func (raft *Raft) recvAppendEntriesResponse(from uint32, pkt *pb.AppendEntriesResponse) {
	if pkt.Term > raft.currentTerm {
		raft.becomeFollower(pkt.Term, 0, 0)
		return
	}
	if pkt.Term < raft.currentTerm {
		return
	}
	if raft.state != leader {
		log.Print("go-raft: AppendEntriesResponse sent to non-leader")
		return
	}
	peer := raft.peers[from]
	if pkt.Success {
		prevLogIndex := peer.nextIndex - 1
		numEntries := pkt.LatestIndex - prevLogIndex
		assert(peer.matchIndex <= prevLogIndex+numEntries, "%#v matchIndex went backward: old=%d new=%d+%d", from, peer.matchIndex, prevLogIndex, numEntries)
		peer.matchIndex = prevLogIndex + numEntries
		newCommitIndex := raft.matchConsensus()
		if raft.log.term(newCommitIndex) == raft.currentTerm {
			raft.commitTo(newCommitIndex)
		}
		peer.nextIndex = peer.matchIndex + 1
	} else {
		if peer.nextIndex > 1 {
			peer.nextIndex--
		}
		if peer.nextIndex > pkt.LatestIndex+1 {
			peer.nextIndex = pkt.LatestIndex + 1
		}
	}
	peer.yeaVote = true
	if raft.yeaVotes() >= raft.quorumLocked() {
		raft.electionTimer = raft.newElectionTimer()
		for id, peer := range raft.peers {
			peer.yeaVote = (id == raft.selfId)
		}
	}
}

func (raft *Raft) recvNominateRequest(from uint32, pkt *pb.NominateRequest) {
	if pkt.Term > raft.currentTerm {
		raft.becomeFollower(pkt.Term, 0, 0)
	}
	if pkt.Term < raft.currentTerm {
		return
	}
	if raft.state == leader {
		return
	}
	raft.becomeCandidate()
}

func (raft *Raft) recvInformRequest(from uint32, pkt *pb.InformRequest) {
	if pkt.Term < raft.currentTerm {
		return
	}
	if pkt.Term == raft.currentTerm && (raft.currentLeaderId == pkt.LeaderId || raft.currentLeaderId == 0) {
		return
	}

	raft.becomeFollower(pkt.Term, pkt.LeaderId, pkt.LeaderId)
	raft.send(from, &pb.NominateRequest{
		Term: raft.currentTerm,
	})
}

func (raft *Raft) Tick() {
	raft.mutex.Lock()
	origState := raft.state
	if raft.electionTimer > 0 {
		raft.electionTimer--
	}
	if raft.heartbeatTimer > 0 {
		raft.heartbeatTimer--
	}
	switch {
	case raft.electionTimer == 0:
		raft.becomeCandidate()

	case raft.state == candidate && raft.heartbeatTimer == 0:
		raft.heartbeatTimer = raft.newHeartbeatTimer()
		raft.sendVoteRequests()

	case raft.state == leader && raft.heartbeatTimer == 0:
		raft.heartbeatTimer = raft.newHeartbeatTimer()
		for id, peer := range raft.peers {
			peer.yeaVote = (id == raft.selfId)
		}
		raft.sendAppendEntries()
	}
	var fn func(*Raft)
	switch {
	case raft.state == leader && origState != leader:
		fn = raft.onGainLeadership
	case raft.state != leader && origState == leader:
		fn = raft.onLoseLeadership
	case raft.state != leader && raft.currentTerm >= raft.lastLeaderTerm+2 && !raft.inLonelyState:
		fn = raft.onLonely
		raft.inLonelyState = true
	}
	raft.mutex.Unlock()
	if fn != nil {
		fn(raft)
	}
}

func (raft *Raft) becomeFollower(term uint64, voteFor uint32, leader uint32) {
	raft.state = follower
	raft.currentTerm = term
	raft.votedForId = voteFor
	raft.currentLeaderId = leader
	if leader != 0 {
		raft.lastLeaderTerm = term
	}
	raft.electionTimer = raft.newElectionTimer()
	raft.heartbeatTimer = 0 // unused
	for _, peer := range raft.peers {
		peer.yeaVote = false // unused
		peer.nextIndex = 0   // unused
		peer.matchIndex = 0  // unused
	}
}

func (raft *Raft) becomeCandidate() {
	raft.state = candidate
	raft.votedForId = raft.selfId
	raft.currentLeaderId = 0
	raft.currentTerm += 1
	raft.electionTimer = raft.newElectionTimer()
	raft.heartbeatTimer = raft.newHeartbeatTimer()
	for id, peer := range raft.peers {
		peer.yeaVote = (id == raft.selfId)
		peer.nextIndex = 0  // unused
		peer.matchIndex = 0 // unused
	}
	if raft.yeaVotes() >= raft.quorumLocked() {
		raft.becomeLeader()
	} else {
		raft.sendVoteRequests()
	}
}

func (raft *Raft) becomeLeader() {
	raft.state = leader
	raft.currentLeaderId = raft.selfId
	raft.lastLeaderTerm = raft.currentTerm
	raft.electionTimer = raft.newElectionTimer()
	raft.heartbeatTimer = raft.newHeartbeatTimer()
	for id, peer := range raft.peers {
		peer.yeaVote = (id == raft.selfId)
		peer.nextIndex = raft.log.nextIndex()
		peer.matchIndex = 0
	}
	raft.log.appendEntry(raft.currentTerm, nil, nil)
	raft.sendAppendEntries()
}

func (raft *Raft) newElectionTimer() uint32 {
	return 100 + uint32(raft.rand.Intn(50))
}

func (raft *Raft) newHeartbeatTimer() uint32 {
	return 10 + uint32(raft.rand.Intn(5))
}

func (raft *Raft) matchConsensus() uint64 {
	values := make([]uint64, 0, len(raft.peers))
	values = append(values, raft.log.latestIndex())
	for id, peer := range raft.peers {
		if id != raft.selfId {
			values = append(values, peer.matchIndex)
		}
	}
	sort.Sort(byIndex(values))
	i := (len(values) - 1) / 2
	return values[i]
}

func (raft *Raft) commitTo(newCommitIndex uint64) {
	for raft.log.commitIndex < newCommitIndex {
		raft.log.commitIndex++
		entry := raft.log.at(raft.log.commitIndex)
		if len(entry.command) > 0 {
			raft.sm.Commit(entry.command)
		}
		if entry.callback != nil {
			entry.callback(true)
		}
	}
}

func (raft *Raft) saveState() error {
	var state pb.State
	for _, id := range raft.idList() {
		peer := raft.peers[id]
		state.Peers = append(state.Peers, &pb.Peer{
			Id:   uint32(id),
			Ip:   []byte(peer.addr.IP),
			Port: uint32(peer.addr.Port),
			Zone: peer.addr.Zone,
		})
	}
	for _, entry := range raft.log.entries {
		state.Logs = append(state.Logs, &pb.LogEntry{
			Term:    entry.term,
			Command: entry.command,
		})
	}
	state.VotedForId = uint32(raft.votedForId)
	state.CurrentTerm = raft.currentTerm
	state.StartTerm = raft.log.startTerm
	state.StartIndex = raft.log.startIndex
	state.Snapshot = raft.sm.Snapshot()

	stateBytes, err := packData(&state)
	if err != nil {
		return err
	}
	return raft.storage.Store("state", stateBytes)
}

func (raft *Raft) restoreState() error {
	stateBytes, err := raft.storage.Retrieve("state")
	if err != nil {
		if !raft.storage.IsNotFound(err) {
			return err
		}
	}

	var state pb.State
	if err := unpackData(stateBytes, &state); err != nil {
		return err
	}
	if state.StartIndex == 0 {
		state.StartIndex = 1
	}
	newPeers := make(map[uint32]*peerData)
	newPeers[raft.selfId] = raft.peers[raft.selfId]
	for _, peer := range state.Peers {
		id := peer.Id
		if _, found := newPeers[id]; found {
			return fmt.Errorf("duplicate id: %d", id)
		}
		var addr net.UDPAddr
		addr.IP = net.IP(peer.Ip)
		addr.Port = int(peer.Port)
		addr.Zone = peer.Zone
		raft.peers[id] = &peerData{addr: &addr}
	}
	newLog := &raftLog{
		startTerm:  state.StartTerm,
		startIndex: state.StartIndex,
		entries:    make([]raftEntry, len(state.Logs)),
	}
	for i, entry := range state.Logs {
		newLog.entries[i] = raftEntry{
			index:   newLog.startIndex + uint64(i),
			term:    entry.Term,
			command: entry.Command,
		}
	}
	newLog.checkInvariants()

	raft.peers = newPeers
	raft.sm.Restore(state.Snapshot)
	raft.log = newLog
	raft.votedForId = state.VotedForId
	raft.currentTerm = state.CurrentTerm
	return nil
}

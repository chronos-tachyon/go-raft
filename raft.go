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
	nextIndex  Index
	matchIndex Index
}

// Raft represents a single participant in a Raft cluster.
type Raft struct {
	self             PeerId
	conn             *net.UDPConn
	rand             *rand.Rand
	sm               StateMachine
	wg               sync.WaitGroup
	mutex            sync.Mutex
	peers            map[PeerId]*peerData
	log              *raftLog
	state            state
	inLonelyState    bool
	votedFor         PeerId
	currentLeader    PeerId
	currentTerm      Term
	lastLeaderTerm   Term
	electionTimer    uint32
	heartbeatTimer   uint32
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
func New(sm StateMachine, cfg *Config, self PeerId) (*Raft, error) {
	peers, err := processConfig(cfg)
	if err != nil {
		return nil, err
	}
	if _, found := peers[self]; !found {
		return nil, fmt.Errorf("missing self id %d", self)
	}
	return &Raft{
		self:  self,
		rand:  rand.New(rand.NewSource(0xdeadbeef ^ int64(self))),
		sm:    sm,
		peers: peers,
		log:   newLog(),
	}, nil
}

// Start acquires the resources needed for this Raft.
func (raft *Raft) Start() error {
	conn, err := net.ListenUDP("udp", raft.peers[raft.self].addr)
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

// Stop releases the resources associated with this Raft.
func (raft *Raft) Stop() error {
	err := raft.conn.Close()
	raft.wg.Wait()
	return err
}

// Id returns the identifier for this node.
func (raft *Raft) Id() PeerId {
	return raft.self
}

// Quorum returns the number of peers required to form a quorum.
func (raft *Raft) Quorum() int {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	return raft.quorumLocked()
}

func (raft *Raft) quorumLocked() int {
	return len(raft.peers)/2 + 1
}

func (raft *Raft) IsLeader() bool {
	raft.mutex.Lock()
	defer raft.mutex.Unlock()
	return raft.state == leader
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
func (raft *Raft) Nominate(id PeerId) {
	raft.mutex.Lock()
	if id == raft.self {
		if raft.state != leader {
			raft.becomeCandidate()
		}
	} else {
		raft.send(id, nominateRequest{
			term: raft.currentTerm,
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
		if id != raft.self {
			peer := raft.peers[id]
			pd += fmt.Sprintf("%d:%d,%d ", id, peer.nextIndex, peer.matchIndex)
		}
	}
	if len(pd) > 0 {
		pd = pd[0 : len(pd)-1]
	}
	return fmt.Sprintf("%d[%c %d %03d/%02d %s %d %d] (%s) {%v} <%v>", raft.self, ch,
		raft.currentTerm, raft.electionTimer, raft.heartbeatTimer,
		raft.yeaVotesToString(), raft.votedFor, raft.currentLeader,
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

func (raft *Raft) idList() []PeerId {
	idList := make([]PeerId, 0, len(raft.peers))
	for id := range raft.peers {
		idList = append(idList, id)
	}
	sort.Sort(byId(idList))
	return idList
}

func (raft *Raft) loop() {
	var buf [1024]byte
	for {
		size, pktAddr, err := raft.conn.ReadFromUDP(buf[:])
		if err != nil {
			operr, ok := err.(*net.OpError)
			if !ok || operr.Op != "read" || operr.Err.Error() != "use of closed network connection" {
				log.Printf("go-raft: %#v", err)
			}
			break
		}
		pkt := unpack(buf[:size])
		if pkt != nil {
			var pktId PeerId
			raft.mutex.Lock()
			for id, peer := range raft.peers {
				if equalUDPAddr(pktAddr, peer.addr) {
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
var faultInjectorFunction func(from, to PeerId) bool

// SetFaultInjectorFunction assigns a fault injector function.  If the fault
// injector function is non-nil, then it is called with the source and
// destination PeerId values.  If the function then returns true, a fault is
// injected and the packet is dropped.
func SetFaultInjectorFunction(fn func(from, to PeerId) bool) {
	faultInjectorMutex.Lock()
	faultInjectorFunction = fn
	faultInjectorMutex.Unlock()
}

func (raft *Raft) send(to PeerId, p packet) {
	faultInjectorMutex.Lock()
	fn := faultInjectorFunction
	faultInjectorMutex.Unlock()
	if fn != nil && fn(raft.self, to) {
		return
	}
	buf := p.pack()
	peer := raft.peers[to]
	_, err := raft.conn.WriteToUDP(buf, peer.addr)
	if err != nil {
		log.Printf("go-raft: %v", err)
	}
}

func (raft *Raft) sendVoteRequests() {
	latestIndex := raft.log.latestIndex()
	latestTerm := raft.log.term(latestIndex)
	for id, peer := range raft.peers {
		if !peer.yeaVote {
			raft.send(id, voteRequest{
				term:        raft.currentTerm,
				latestTerm:  latestTerm,
				latestIndex: latestIndex,
			})
		}
	}
}

func (raft *Raft) sendAppendEntries() {
	for _, id := range raft.idList() {
		if id == raft.self {
			continue
		}
		peer := raft.peers[id]
		latestIndex := raft.log.latestIndex()
		peerLatestIndex := peer.nextIndex - 1
		peerLatestTerm := raft.log.term(peerLatestIndex)
		assert(peerLatestIndex <= latestIndex, "peerLatestIndex=%d > latestIndex=%d", peerLatestIndex, latestIndex)
		n := minIndex(latestIndex, peerLatestIndex+8)
		numEntries := n - peerLatestIndex
		entries := make([]appendEntry, 0, numEntries)
		for index := peerLatestIndex + 1; index <= n; index++ {
			entry := raft.log.at(index)
			entries = append(entries, appendEntry{
				term:    entry.term,
				command: entry.command,
			})
		}
		commitIndex := minIndex(raft.log.commitIndex, peerLatestIndex+Index(len(entries)))
		raft.send(id, appendEntriesRequest{
			term:         raft.currentTerm,
			prevLogTerm:  peerLatestTerm,
			prevLogIndex: peerLatestIndex,
			leaderCommit: commitIndex,
			entries:      entries,
		})
	}
}

func (raft *Raft) recv(from PeerId, pkt packet) {
	raft.mutex.Lock()
	origState := raft.state
	switch v := pkt.(type) {
	case voteRequest:
		raft.recvVoteRequest(from, v)

	case voteResponse:
		raft.recvVoteResponse(from, v)

	case appendEntriesRequest:
		raft.recvAppendEntriesRequest(from, v)

	case appendEntriesResponse:
		raft.recvAppendEntriesResponse(from, v)

	case nominateRequest:
		raft.recvNominateRequest(from, v)

	case informRequest:
		raft.recvInformRequest(from, v)

	default:
		assert(false, "unknown type %T", pkt)
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

func (raft *Raft) recvVoteRequest(from PeerId, pkt voteRequest) {
	latestIndex := raft.log.latestIndex()
	latestTerm := raft.log.term(latestIndex)
	logIsOK := (pkt.latestTerm > latestTerm || (pkt.latestTerm == latestTerm && pkt.latestIndex >= latestIndex))
	alreadyVoted := (pkt.term == raft.currentTerm && raft.votedFor != 0 && raft.votedFor != from)
	if pkt.term > raft.currentTerm {
		raft.becomeFollower(pkt.term, from, 0)
	}
	granted := (pkt.term == raft.currentTerm && logIsOK && !alreadyVoted)
	raft.send(from, voteResponse{
		term:    raft.currentTerm,
		granted: granted,
		logIsOK: logIsOK,
	})
}

func (raft *Raft) recvVoteResponse(from PeerId, pkt voteResponse) {
	if pkt.term < raft.currentTerm {
		return
	}
	if pkt.term > raft.currentTerm {
		raft.becomeFollower(pkt.term, 0, 0)
		return
	}
	if raft.state != candidate {
		return
	}
	if pkt.granted {
		raft.peers[from].yeaVote = true
		if raft.yeaVotes() >= raft.quorumLocked() {
			raft.becomeLeader()
		}
	} else {
		if !pkt.logIsOK {
			raft.becomeFollower(pkt.term, 0, 0)
			raft.send(from, nominateRequest{
				term: raft.currentTerm,
			})
		}
	}
}

func (raft *Raft) recvAppendEntriesRequest(from PeerId, pkt appendEntriesRequest) {
	entries := pkt.entries
	firstIndex := pkt.prevLogIndex + 1
	lastCommittedIndex := minIndex(firstIndex+Index(len(entries)), raft.log.commitIndex)

	if pkt.term < raft.currentTerm {
		goto Reject
	}
	if raft.state == leader && pkt.term == raft.currentTerm {
		log.Printf("go-raft: AppendEntriesRequest sent to leader")
		goto Reject
	}

	raft.becomeFollower(pkt.term, from, from)
	raft.log.checkInvariants()
	if pkt.prevLogIndex >= raft.log.nextIndex() {
		goto Reject
	}
	if raft.log.term(pkt.prevLogIndex) != pkt.prevLogTerm {
		goto Reject
	}
	for firstIndex < lastCommittedIndex {
		if firstIndex >= raft.log.startIndex && firstIndex < raft.log.nextIndex() {
			if raft.log.term(pkt.prevLogIndex) != entries[0].term {
				goto Reject
			}
			assert(bytes.Equal(raft.log.at(pkt.prevLogIndex).command, entries[0].command), "")
		}
		firstIndex++
		entries = entries[1:]
	}
	for i, entry := range entries {
		index := firstIndex + Index(i)
		if index >= raft.log.startIndex {
			if index < raft.log.nextIndex() && raft.log.term(index) != entry.term {
				raft.log.deleteEntriesAfter(index - 1)
				assert(raft.log.nextIndex() == index, "new item index mismatch: expected %d got %d", raft.log.nextIndex(), index)
			}
			raft.log.appendEntry(entry.term, entry.command, nil)
		}
	}
	raft.commitTo(pkt.leaderCommit)
	raft.send(from, appendEntriesResponse{
		success:     true,
		term:        raft.currentTerm,
		latestIndex: raft.log.latestIndex(),
		commitIndex: raft.log.commitIndex,
	})
	for _, id := range raft.idList() {
		if id != raft.self && id != from {
			raft.send(id, informRequest{
				term:   raft.currentTerm,
				leader: raft.currentLeader,
			})
		}
	}
	return

Reject:
	raft.send(from, appendEntriesResponse{
		success:     false,
		term:        raft.currentTerm,
		latestIndex: raft.log.latestIndex(),
		commitIndex: raft.log.commitIndex,
	})
}

func (raft *Raft) recvAppendEntriesResponse(from PeerId, pkt appendEntriesResponse) {
	if pkt.term > raft.currentTerm {
		raft.becomeFollower(pkt.term, 0, 0)
		return
	}
	if pkt.term < raft.currentTerm {
		return
	}
	if raft.state != leader {
		log.Print("go-raft: AppendEntriesResponse sent to non-leader")
		return
	}
	peer := raft.peers[from]
	if pkt.success {
		prevLogIndex := peer.nextIndex - 1
		numEntries := pkt.latestIndex - prevLogIndex
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
		if peer.nextIndex > pkt.latestIndex+1 {
			peer.nextIndex = pkt.latestIndex + 1
		}
	}
	peer.yeaVote = true
	if raft.yeaVotes() >= raft.quorumLocked() {
		raft.electionTimer = raft.newElectionTimer()
		for id, peer := range raft.peers {
			peer.yeaVote = (id == raft.self)
		}
	}
}

func (raft *Raft) recvNominateRequest(from PeerId, pkt nominateRequest) {
	if pkt.term > raft.currentTerm {
		raft.becomeFollower(pkt.term, 0, 0)
	}
	if pkt.term < raft.currentTerm {
		return
	}
	if raft.state == leader {
		return
	}
	raft.becomeCandidate()
}

func (raft *Raft) recvInformRequest(from PeerId, pkt informRequest) {
	if pkt.term < raft.currentTerm {
		return
	}
	if pkt.term == raft.currentTerm && (raft.currentLeader == pkt.leader || raft.currentLeader == 0) {
		return
	}

	raft.becomeFollower(pkt.term, pkt.leader, pkt.leader)
	raft.send(from, nominateRequest{
		term: raft.currentTerm,
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
			peer.yeaVote = (id == raft.self)
		}
		raft.sendAppendEntries()
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

func (raft *Raft) becomeFollower(term Term, voteFor PeerId, leader PeerId) {
	raft.state = follower
	raft.currentTerm = term
	raft.votedFor = voteFor
	raft.currentLeader = leader
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
	raft.votedFor = raft.self
	raft.currentLeader = 0
	raft.currentTerm += 1
	raft.electionTimer = raft.newElectionTimer()
	raft.heartbeatTimer = raft.newHeartbeatTimer()
	for id, peer := range raft.peers {
		peer.yeaVote = (id == raft.self)
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
	raft.currentLeader = raft.self
	raft.lastLeaderTerm = raft.currentTerm
	raft.electionTimer = raft.newElectionTimer()
	raft.heartbeatTimer = raft.newHeartbeatTimer()
	for id, peer := range raft.peers {
		peer.yeaVote = (id == raft.self)
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

func (raft *Raft) matchConsensus() Index {
	values := make([]Index, 0, len(raft.peers))
	values = append(values, raft.log.latestIndex())
	for id, peer := range raft.peers {
		if id != raft.self {
			values = append(values, peer.matchIndex)
		}
	}
	sort.Sort(byIndex(values))
	i := (len(values) - 1) / 2
	return values[i]
}

func (raft *Raft) commitTo(newCommitIndex Index) {
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

func processConfig(cfg *Config) (map[PeerId]*peerData, error) {
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("must configure at least one Raft node")
	}
	result := make(map[PeerId]*peerData, len(cfg.Peers))
	for _, item := range cfg.Peers {
		id := PeerId(item.Id)
		if id == 0 {
			return nil, fmt.Errorf("invalid id: 0")
		}
		if _, found := result[id]; found {
			return nil, fmt.Errorf("duplicate id: %d", id)
		}
		addr, err := net.ResolveUDPAddr("udp", item.Addr)
		if err != nil {
			return nil, err
		}
		result[id] = &peerData{addr: addr}
	}
	return result, nil
}

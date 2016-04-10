package server

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/satori/go.uuid"

	"github.com/chronos-tachyon/go-raft/internal/multierror"
	"github.com/chronos-tachyon/go-raft/internal/util"
	pb "github.com/chronos-tachyon/go-raft/proto"
	"github.com/chronos-tachyon/go-raft/rpc"
	"github.com/chronos-tachyon/go-raft/storage"
)

type sidrid struct {
	sid uint64
	rid uint64
}

type sessionEntry struct {
	expire time.Time
	cache  map[uint32]cacheEntry
}

type cacheEntry struct {
	packet pb.Packet
	expire time.Time
}

type state uint8

const (
	blank state = iota
	watcher
	follower
	candidate
	leader
)

var stateChars = []byte{'x', 'w', 'F', 'C', 'L'}

func (s state) Char() byte {
	if int(s) >= len(stateChars) {
		return '?'
	}
	return stateChars[s]
}

/*
 * follower:
 *  electionTimer       ticks until become candidate
 *  heartbeatTimer      unused (0)
 *  peer.vote           unused (unknownVote)
 *  peer.nextIndex      unused (0)
 *  peer.matchIndex     unused (0)
 *
 * candidate:
 *  electionTimer       ticks until bump CurrentTerm and try again
 *  heartbeatTimer      ticks until retransmit VoteRequests
 *  peer.vote           the vote which this peer has cast in the leader election
 *  peer.nextIndex      unused (0)
 *  peer.matchIndex     unused (0)
 *
 * leader:
 *  electionTimer       unused (temporarily; will be: ticks until drop leadership)
 *  heartbeatTimer      ticks until heartbeat sent
 *  peer.vote           yeaVote iff this peer has responded to a heartbeat recently
 *  peer.nextIndex      index of the next entry to send
 *  peer.matchIndex     index of the most recent entry known to match what we have
 */

// Raft represents a single participant in a Raft cluster.
type Server struct {
	mutex            sync.Mutex
	wg               sync.WaitGroup
	selfId           uint32
	selfAddr         string
	rng              *rand.Rand
	manager          *storage.Manager
	secrets          rpc.SecretsManager
	peerSecretNum    uint32
	conn             *net.UDPConn
	c                *configuration
	state            state
	inLonelyState    bool
	cluster          uuid.UUID
	sessions         map[uint32]sessionEntry
	peerMap          map[uint32]*net.UDPAddr
	electionTimer    uint32
	heartbeatTimer   uint32
	currentLeaderId  uint32
	lastLeaderTerm   uint64
	onGainLeadership func(*Server)
	onLoseLeadership func(*Server)
	onLonely         func(*Server)
	onNotLonely      func(*Server)
}

type Params struct {
	Storage        storage.Storage
	StateMachine   storage.StateMachine
	SecretsManager rpc.SecretsManager
	PeerSecretNum  uint32
	ServerId       uint32
	ServerAddr     string
}

// NewServer constructs a new Server.
func New(params Params) (*Server, error) {
	if params.Storage == nil {
		return nil, fmt.Errorf("must specify Params.Storage")
	}
	if params.StateMachine == nil {
		return nil, fmt.Errorf("must specify Params.StateMachine")
	}
	if params.SecretsManager == nil {
		return nil, fmt.Errorf("must specify Params.SecretsManager")
	}
	if params.ServerId == 0 {
		return nil, fmt.Errorf("must specify Params.ServerId")
	}
	if params.ServerAddr == "" {
		return nil, fmt.Errorf("must specify Params.ServerAddr")
	}
	svr := &Server{
		selfId:        params.ServerId,
		selfAddr:      params.ServerAddr,
		rng:           rand.New(rand.NewSource(0xdeadbeef ^ int64(params.ServerId))),
		secrets:       params.SecretsManager,
		peerSecretNum: params.PeerSecretNum,
		peerMap:       make(map[uint32]*net.UDPAddr),
	}
	svr.manager = storage.NewManager(params.Storage, params.StateMachine, svr.applyConfig)
	return svr, nil
}

// Id returns the identifier for this Server.
func (svr *Server) Id() uint32 {
	return svr.selfId
}

// Addr returns this Server's UDP address.
func (svr *Server) Addr() string {
	return svr.selfAddr
}

// Configuration returns the Server's active Configuration.
func (svr *Server) Configuration() *pb.Configuration {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()
	return svr.manager.ActiveConfiguration
}

// IsLeader returns true if this Server is *probably* the leader.
// (The information may be stale.)
func (svr *Server) IsLeader() bool {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()
	return svr.state == leader
}

// OnGainLeadership sets the callback which, if non-nil, will be executed when
// this Server becomes the leader.
//
// The previous leader may believe it is still the leader for up to 150 ticks.
func (svr *Server) OnGainLeadership(fn func(*Server)) {
	svr.mutex.Lock()
	svr.onGainLeadership = fn
	svr.mutex.Unlock()
}

// OnLoseLeadership sets the callback which, if non-nil, will be executed when
// this Server is no longer the leader.
func (svr *Server) OnLoseLeadership(fn func(*Server)) {
	svr.mutex.Lock()
	svr.onLoseLeadership = fn
	svr.mutex.Unlock()
}

func (svr *Server) OnLonely(fn func(*Server)) {
	svr.mutex.Lock()
	svr.onLonely = fn
	svr.mutex.Unlock()
}

func (svr *Server) OnNotLonely(fn func(*Server)) {
	svr.mutex.Lock()
	svr.onNotLonely = fn
	svr.mutex.Unlock()
}

// ForceElection forces a new leadership election, nominating this Server as
// the proposed leader.
func (svr *Server) ForceElection() {
	svr.mutex.Lock()
	svr.becomeCandidate()
	svr.mutex.Unlock()
}

// String returns a dense but human-readable summary of this Server's state.
func (svr *Server) String() string {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()
	return svr.stringLocked()
}

func (svr *Server) stringLocked() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%d[%c %d %03d/%02d %d %d] ", svr.selfId,
		svr.state.Char(), svr.manager.CurrentTerm, svr.electionTimer,
		svr.heartbeatTimer, svr.manager.VotedFor, svr.currentLeaderId)
	for _, p := range svr.c.peers {
		fmt.Fprintf(&buf, "(%v) ", p)
	}
	buf.WriteString(svr.manager.String())
	return buf.String()
}

// Start launches this Server.
func (svr *Server) Start() error {
	udpaddr, err := net.ResolveUDPAddr("udp", svr.selfAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", udpaddr)
	if err != nil {
		return err
	}

	err = svr.manager.Open()
	if err != nil {
		conn.Close()
		return err
	}

	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	c := importConfiguration(svr.manager.ActiveConfiguration, svr.c, svr.selfId, svr.selfAddr)

	svr.conn = conn
	svr.c = c
	switch svr.c.byId[svr.selfId].state {
	case pb.Peer_BLANK:
		svr.becomeBlank()
	case pb.Peer_LEARNING:
		svr.becomeFollower()
		svr.state = watcher
	default:
		svr.becomeFollower()
	}
	svr.wg.Add(1)
	go svr.loop()
	return nil
}

// Stop halts this Server and releases resources.
func (svr *Server) Stop() error {
	err1 := svr.conn.Close()
	svr.wg.Wait()
	err2 := svr.manager.Close()
	svr.mutex.Lock()
	svr.conn = nil
	svr.mutex.Unlock()
	return multierror.Of(err1, err2)
}

func (svr *Server) Bootstrap(clusterUUID string) {
	cluster, err := uuid.FromString(clusterUUID)
	util.Assert(err == nil, "%v", err)
	util.Assert(!uuid.Equal(cluster, uuid.Nil), "must specify a valid cluster UUID")

	cfg := &pb.Configuration{
		Id: 1,
		Peers: []*pb.Peer{
			&pb.Peer{
				Id:    svr.selfId,
				Addr:  svr.selfAddr,
				State: pb.Peer_STABLE,
			},
		},
	}
	c := importConfiguration(cfg, nil, svr.selfId, svr.selfAddr)

	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	util.Assert(svr.conn != nil, "Bootstrap can only be called on a running server")
	util.Assert(svr.manager.ActiveConfiguration == nil, "Bootstrap can only be called on a newly installed server")
	svr.manager.ActiveConfiguration = cfg
	svr.c = c
	svr.cluster = cluster
	svr.becomeFollower()
	svr.becomeCandidate()

	cv := sync.NewCond(&svr.mutex)

	entries := []*pb.LogEntry{
		&pb.LogEntry{
			Term:    svr.manager.CurrentTerm,
			Type:    pb.LogEntry_CONFIGURATION,
			Payload: util.MustMarshalProto(cfg),
		},
	}
	svr.manager.Append(entries, func(_, end uint64, err error) {
		svr.mutex.Lock()
		defer svr.mutex.Unlock()

		svr.manager.OnCommit(end - 1, func(ok bool) {
			cv.Signal()
		})
	})
	cv.Wait()
}

func (svr *Server) Tick() {
	svr.mutex.Lock()
	defer svr.mutex.Unlock()

	if svr.state == blank {
		return
	}

	if svr.electionTimer > 0 {
		svr.electionTimer--
	}
	if svr.heartbeatTimer > 0 {
		svr.heartbeatTimer--
	}

	origState := svr.state

	switch {
	case svr.state >= follower && svr.electionTimer == 0:
		svr.becomeCandidate()

	case svr.state == candidate && svr.heartbeatTimer == 0:
		svr.heartbeatTimer = svr.newHeartbeatTimer()
		svr.sendVoteRequests()

	case svr.state == leader && svr.heartbeatTimer == 0:
		svr.heartbeatTimer = svr.newHeartbeatTimer()
		svr.sendAppendEntries()
		if svr.c.yeaQuorum() {
			svr.electionTimer = svr.newElectionTimer()
			svr.advanceCommitIndex()
		}
	}

	switch {
	case svr.state == leader && origState != leader:
		fn := svr.onGainLeadership
		go fn(svr)
	case svr.state != leader && origState == leader:
		fn := svr.onLoseLeadership
		go fn(svr)
	case svr.state != leader && svr.manager.CurrentTerm >= svr.lastLeaderTerm+2 && !svr.inLonelyState:
		svr.inLonelyState = true
		fn := svr.onLonely
		go fn(svr)
	}
}

func (svr *Server) loop() {
	svr.mutex.Lock()
	needUnlock := true
	defer (func() {
		if needUnlock {
			svr.mutex.Unlock()
		}
	})()

	for {
		svr.mutex.Unlock()
		needUnlock = false

		packet, fromAddr, secretNum, err := rpc.RecvUDP(svr.secrets, svr.conn)

		needUnlock = true
		svr.mutex.Lock()

		if err != nil {
			operr, ok := err.(*net.OpError)
			if !ok || operr.Op != "read" || operr.Err.Error() != "use of closed network connection" {
				log.Printf("go-raft/server: %v", err)
			}
			break
		}

		var packetCluster uuid.UUID
		if len(packet.Cluster) > 0 {
			packetCluster, err = uuid.FromBytes(packet.Cluster)
			if err != nil {
				log.Printf("go-raft/server: packet with invalid UUID: %v", err)
				continue
			}
		}
		if !uuid.Equal(packetCluster, svr.cluster) && !uuid.Equal(packetCluster, uuid.Nil) && !uuid.Equal(svr.cluster, uuid.Nil) {
			log.Printf("go-raft/server: received packet intended for cluster %s", packetCluster.String())
			continue
		}

		switch rpc.Category(packet.Type) {
		case rpc.PeerCategory:
			if _, found := svr.peerMap[packet.SourceId]; !found {
				svr.peerMap[packet.SourceId] = fromAddr
			}
			svr.recvFromPeer(packet)

		case rpc.AdminCategory, rpc.WriteCategory, rpc.ReadCategory:
			svr.recvFromClient(fromAddr, secretNum, packet)

		default:
			// ignore packet
		}
	}
	svr.conn.Close()
	svr.wg.Done()
}

func (svr *Server) send(to *net.UDPAddr, secretNum uint32, packet *pb.Packet) {
	if err := rpc.SendUDP(svr.secrets, secretNum, svr.conn, to, packet); err != nil {
		log.Printf("go-raft/server: %v", err)
	}
}

func (svr *Server) sendToClient(to *net.UDPAddr, secretNum, sid, rid uint32, ptype pb.Packet_Type, msg *pb.ClientResponse) {
	packet := &pb.Packet{
		Cluster:   svr.cluster.Bytes(),
		SourceId:  svr.selfId,
		SessionId: sid,
		RequestId: rid,
		Type:      ptype,
		Payload:   util.MustMarshalProto(msg),
	}
	if svr.sessions != nil {
		if session, found := svr.sessions[sid]; found {
			session.cache[rid] = cacheEntry{
				packet: *packet,
				expire: time.Now().Add(5 * time.Minute),
			}
			// FIXME: instead of +5min, make rid sequential and
			// have each request include an ACK of all rid
			// responses received by client
		}
	}
	svr.send(to, secretNum, packet)
}

func (svr *Server) sendToPeer(to uint32, msg proto.Message) {
	var ptype pb.Packet_Type
	switch msg.(type) {
	case *pb.VoteRequest:
		ptype = pb.Packet_VOTE_REQUEST
	case *pb.VoteResponse:
		ptype = pb.Packet_VOTE_RESPONSE
	case *pb.AppendEntriesRequest:
		ptype = pb.Packet_APPEND_ENTRIES_REQUEST
	case *pb.AppendEntriesResponse:
		ptype = pb.Packet_APPEND_ENTRIES_RESPONSE
	case *pb.NominateRequest:
		ptype = pb.Packet_NOMINATE_REQUEST
	case *pb.InformRequest:
		ptype = pb.Packet_INFORM_REQUEST
	default:
		panic(fmt.Errorf("sendToPeer not implemented for %T", msg))
	}

	udpaddr, found := svr.peerMap[to]
	if !found {
		p, found2 := svr.c.byId[to]
		if !found2 {
			return
		}
		var err error
		udpaddr, err = net.ResolveUDPAddr("udp", p.addr)
		if err != nil {
			log.Printf("go-raft/server: %v", err)
			return
		}
		svr.peerMap[to] = udpaddr
	}

	svr.send(udpaddr, svr.peerSecretNum, &pb.Packet{
		Cluster:  svr.cluster.Bytes(),
		SourceId: svr.selfId,
		Type:     ptype,
		Payload:  util.MustMarshalProto(msg),
	})
}

func (svr *Server) sendVoteRequests() {
	latestIndex := svr.manager.LatestIndex()
	latestTerm := svr.manager.Term(latestIndex)
	req := pb.VoteRequest{
		Term:        svr.manager.CurrentTerm,
		LatestTerm:  latestTerm,
		LatestIndex: latestIndex,
	}
	for _, p := range svr.c.peers {
		if p.vote == unknownVote {
			svr.sendToPeer(p.id, &req)
		}
	}
}

func (svr *Server) sendAppendEntries() {
	nextIndex := svr.manager.NextIndex()
	latestIndex := nextIndex - 1

	self := svr.c.byId[svr.selfId]
	self.nextIndex = nextIndex
	self.matchIndex = latestIndex

	for _, p := range svr.c.peers {
		p.vote = unknownVote
		if p.id == svr.selfId {
			p.vote = yeaVote
			continue
		}
		peerLatestIndex := p.nextIndex - 1
		peerLatestTerm := svr.manager.Term(peerLatestIndex)
		util.Assert(peerLatestIndex <= latestIndex, "peerLatestIndex=%d > latestIndex=%d", peerLatestIndex, latestIndex)
		n := util.Min(latestIndex, peerLatestIndex+8)
		entries := svr.manager.Slice(peerLatestIndex+1, n+1)
		commitIndex := util.Min(svr.manager.CommitIndex, peerLatestIndex+uint64(len(entries)))
		svr.sendToPeer(p.id, &pb.AppendEntriesRequest{
			Term:              svr.manager.CurrentTerm,
			PrevLogTerm:       peerLatestTerm,
			PrevLogIndex:      peerLatestIndex,
			LeaderCommitIndex: commitIndex,
			Entries:           entries,
		})
	}
}

func (svr *Server) recvFromPeer(packet *pb.Packet) {
	origState := svr.state

	switch packet.Type {
	case pb.Packet_VOTE_REQUEST:
		var v pb.VoteRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvVoteRequest(packet, &v)

	case pb.Packet_VOTE_RESPONSE:
		var v pb.VoteResponse
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvVoteResponse(packet, &v)

	case pb.Packet_APPEND_ENTRIES_REQUEST:
		var v pb.AppendEntriesRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvAppendEntriesRequest(packet, &v)

	case pb.Packet_APPEND_ENTRIES_RESPONSE:
		var v pb.AppendEntriesResponse
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvAppendEntriesResponse(packet, &v)

	case pb.Packet_NOMINATE_REQUEST:
		var v pb.NominateRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvNominateRequest(packet, &v)

	case pb.Packet_INFORM_REQUEST:
		var v pb.InformRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvInformRequest(packet, &v)

	default:
		log.Printf("go-raft/server: recvFromPeer not implemented for Packet.Type %[1]s(%[1]d)", packet.Type)
	}

	if svr.inLonelyState && svr.currentLeaderId != 0 {
		svr.inLonelyState = false
		fn := svr.onNotLonely
		go fn(svr)
	}

	switch {
	case svr.state == leader && origState != leader:
		fn := svr.onGainLeadership
		go fn(svr)
	case svr.state != leader && origState == leader:
		fn := svr.onLoseLeadership
		go fn(svr)
	}
}

func (svr *Server) recvFromClient(from *net.UDPAddr, secretNum uint32, packet *pb.Packet) {
	if svr.state != leader {
		leaderAddr := ""
		if svr.currentLeaderId != 0 {
			leaderAddr = svr.c.byId[svr.currentLeaderId].addr
		}
		resp := rpc.NotLeaderError(svr.selfId, svr.currentLeaderId, leaderAddr).AsClientResponse()
		resp.Configuration = svr.manager.ActiveConfiguration
		svr.populateClientResponse(resp)
		svr.sendToClient(from, secretNum, packet.SessionId, packet.RequestId, packet.Type+1, resp)
		return
	}

	util.Assert(svr.sessions != nil, "leader must have non-nil svr.sessions")
	if session, found := svr.sessions[packet.SessionId]; found {
		if entry, found2 := session.cache[packet.RequestId]; found2 {
			svr.send(from, secretNum, &entry.packet)
			return
		}
	} else {
		if packet.Type != pb.Packet_CREATE_SESSION_REQUEST {
			resp := rpc.InvalidSessionError(packet.SessionId).AsClientResponse()
			svr.populateClientResponse(resp)
			svr.sendToClient(from, secretNum, packet.SessionId, packet.RequestId, packet.Type+1, resp)
			return
		}
	}

	switch packet.Type {
	case pb.Packet_CREATE_SESSION_REQUEST:
		var v pb.CreateSessionRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvCreateSessionRequest(from, secretNum, packet, &v)

	case pb.Packet_DESTROY_SESSION_REQUEST:
		var v pb.DestroySessionRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvDestroySessionRequest(from, secretNum, packet, &v)

	case pb.Packet_QUERY_REQUEST:
		var v pb.QueryRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvQueryRequest(from, secretNum, packet, &v)

	case pb.Packet_WRITE_REQUEST:
		var v pb.WriteRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvWriteRequest(from, secretNum, packet, &v)

	case pb.Packet_GET_CONFIGURATION_REQUEST:
		var v pb.GetConfigurationRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvGetConfigurationRequest(from, secretNum, packet, &v)

	case pb.Packet_SET_CONFIGURATION_REQUEST:
		var v pb.SetConfigurationRequest
		util.MustUnmarshalProto(packet.Payload, &v)
		svr.recvSetConfigurationRequest(from, secretNum, packet, &v)

	default:
		log.Printf("go-raft/server: recvFromClient not implemented for Packet.Type %[1]s(%[1]d)", packet.Type)
	}
}

func (svr *Server) recvVoteRequest(packet *pb.Packet, msg *pb.VoteRequest) {
	if svr.state == blank {
		cluster, err := uuid.FromBytes(packet.Cluster)
		if err != nil {
			return
		}
		svr.becomeFollower()
		svr.state = watcher
		svr.cluster = cluster
	}

	valid := (svr.state >= follower)
	currentTerm := svr.manager.CurrentTerm
	votedFor := svr.manager.VotedFor

	if msg.Term > currentTerm {
		svr.becomeFollower()
		currentTerm = msg.Term
		votedFor = 0
	}

	latestIndex := svr.manager.LatestIndex()
	latestTerm := svr.manager.Term(latestIndex)
	logIsOK := (msg.LatestTerm > latestTerm || (msg.LatestTerm == latestTerm && msg.LatestIndex >= latestIndex))
	alreadyVoted := (msg.Term == currentTerm && votedFor != 0 && votedFor != packet.SourceId)
	granted := (msg.Term == currentTerm && logIsOK && !alreadyVoted)
	if granted {
		votedFor = packet.SourceId
	}

	svr.manager.UpdateTerm(currentTerm, votedFor, func(err error) {
		svr.mutex.Lock()
		defer svr.mutex.Unlock()

		if !valid || err != nil {
			return
		}
		svr.sendToPeer(packet.SourceId, &pb.VoteResponse{
			Term:    currentTerm,
			Granted: granted,
			LogIsOk: logIsOK,
		})
	})
}

func (svr *Server) recvVoteResponse(packet *pb.Packet, msg *pb.VoteResponse) {
	if svr.state == blank {
		return
	}
	if msg.Term < svr.manager.CurrentTerm {
		return
	}
	if msg.Term > svr.manager.CurrentTerm {
		svr.becomeFollower()
		svr.manager.UpdateTerm(msg.Term, 0, nop)
		return
	}
	if svr.state != candidate {
		return
	}
	from, found := svr.c.byId[packet.SourceId]
	if !found {
		log.Printf("go-raft/server: VoteResponse sent from unknown server <%d>", packet.SourceId)
		return
	}
	if msg.Granted {
		from.vote = yeaVote
		if svr.c.yeaQuorum() {
			svr.becomeLeader()
		}
	} else {
		from.vote = nayVote
		if !msg.LogIsOk {
			svr.becomeFollower()
			svr.sendToPeer(packet.SourceId, &pb.NominateRequest{
				Term: svr.manager.CurrentTerm,
			})
		}
	}
}

func (svr *Server) recvAppendEntriesRequest(packet *pb.Packet, msg *pb.AppendEntriesRequest) {
	if svr.state == blank {
		cluster, err := uuid.FromBytes(packet.Cluster)
		if err != nil {
			return
		}
		svr.becomeFollower()
		svr.state = watcher
		svr.cluster = cluster
	}

	currentTerm := svr.manager.CurrentTerm
	votedFor := svr.manager.VotedFor

	success := false
	respond := func() {
		svr.sendToPeer(packet.SourceId, &pb.AppendEntriesResponse{
			Success:     success,
			Term:        currentTerm,
			LatestIndex: svr.manager.LatestIndex(),
			CommitIndex: svr.manager.CommitIndex,
		})
	}

	if msg.Term < currentTerm || (msg.Term == currentTerm && svr.state == leader) {
		respond()
		return
	}

	if msg.Term > currentTerm {
		currentTerm = msg.Term
		votedFor = packet.SourceId
	}

	svr.becomeFollower()
	svr.currentLeaderId = packet.SourceId
	svr.lastLeaderTerm = currentTerm
	svr.manager.UpdateTerm(currentTerm, votedFor, func(err error) {
		svr.mutex.Lock()
		defer svr.mutex.Unlock()

		if err != nil {
			respond()
			return
		}
		svr.manager.Match(msg.PrevLogIndex, msg.PrevLogTerm, msg.Entries, func(err error) {
			svr.mutex.Lock()
			defer svr.mutex.Unlock()

			if err != nil {
				respond()
				return
			}
			svr.manager.CommitTo(msg.LeaderCommitIndex)
			success = true
			respond()

			for _, e := range msg.Entries {
				if e.Type == pb.LogEntry_CONFIGURATION {
					var cfg pb.Configuration
					util.MustUnmarshalProto(e.Payload, &cfg)
					c := importConfiguration(&cfg, svr.c, svr.selfId, svr.selfAddr)
					svr.manager.ActiveConfiguration = &cfg
					svr.c = c
				}
			}

			switch svr.c.byId[svr.selfId].state {
			case pb.Peer_BLANK:
				svr.becomeBlank()
			case pb.Peer_LEARNING:
				svr.state = watcher
			default:
				svr.state = follower
			}

			for _, p := range svr.c.peers {
				if p.id != svr.selfId && p.id != packet.SourceId {
					svr.sendToPeer(p.id, &pb.InformRequest{
						Term:     svr.manager.CurrentTerm,
						LeaderId: svr.currentLeaderId,
					})
				}
			}
		})
	})
}

func (svr *Server) recvAppendEntriesResponse(packet *pb.Packet, msg *pb.AppendEntriesResponse) {
	if svr.state == blank {
		return
	}
	if msg.Term < svr.manager.CurrentTerm {
		return
	}
	if msg.Term > svr.manager.CurrentTerm {
		svr.becomeFollower()
		svr.manager.UpdateTerm(msg.Term, 0, nop)
		return
	}
	if svr.state != leader {
		log.Printf("go-raft/server: AppendEntriesResponse sent to non-leader")
		return
	}
	from, found := svr.c.byId[packet.SourceId]
	if !found {
		log.Printf("go-raft/server: AppendEntriesResponse sent from unknown server <%d>", packet.SourceId)
		return
	}
	if msg.Success {
		prevLogIndex := from.nextIndex - 1
		numEntries := msg.LatestIndex - prevLogIndex
		util.Assert(from.matchIndex <= prevLogIndex+numEntries, "<%d:%d> matchIndex went backward: old=%d new=%d+%d", svr.selfId, packet.SourceId, from.matchIndex, prevLogIndex, numEntries)
		from.matchIndex = prevLogIndex + numEntries
		from.nextIndex = from.matchIndex + 1
		svr.advanceCommitIndex()
	} else {
		if from.nextIndex > 1 {
			from.nextIndex--
		}
		if from.nextIndex > msg.LatestIndex+1 {
			from.nextIndex = msg.LatestIndex + 1
		}
	}
	from.vote = yeaVote
	if svr.c.yeaQuorum() {
		svr.electionTimer = svr.newElectionTimer()
	}
}

func (svr *Server) recvNominateRequest(packet *pb.Packet, msg *pb.NominateRequest) {
	if svr.state == blank {
		return
	}

	valid := (svr.state >= follower)

	if msg.Term < svr.manager.CurrentTerm {
		return
	}
	if msg.Term > svr.manager.CurrentTerm {
		svr.becomeFollower()
		svr.manager.UpdateTerm(msg.Term, 0, func(err error) {
			svr.mutex.Lock()
			defer svr.mutex.Unlock()

			if valid && err == nil {
				svr.becomeCandidate()
			}
		})
		return
	}
	if valid && svr.state != leader {
		svr.becomeCandidate()
	}
}

func (svr *Server) recvInformRequest(packet *pb.Packet, msg *pb.InformRequest) {
	if svr.state == blank {
		return
	}

	valid := (svr.state >= follower)
	currentTerm := svr.manager.CurrentTerm
	votedFor := svr.manager.VotedFor

	if msg.Term < currentTerm {
		return
	}
	if msg.Term == currentTerm && (svr.currentLeaderId == msg.LeaderId || svr.currentLeaderId == 0) {
		return
	}
	if msg.Term > currentTerm {
		currentTerm = msg.Term
		votedFor = 0
	}

	svr.becomeFollower()
	svr.currentLeaderId = msg.LeaderId
	svr.lastLeaderTerm = currentTerm
	svr.manager.UpdateTerm(currentTerm, votedFor, nop)
	if valid {
		svr.sendToPeer(packet.SourceId, &pb.NominateRequest{
			Term: currentTerm,
		})
	}
}

func (svr *Server) recvCreateSessionRequest(from *net.UDPAddr, secretNum uint32, raw *pb.Packet, msg *pb.CreateSessionRequest) {
	svr.sessions[raw.SessionId] = sessionEntry{
		expire: time.Now().Add(1 * time.Hour),
		cache:  make(map[uint32]cacheEntry),
	}
	response := &pb.ClientResponse{Status: pb.ClientResponse_OK}
	svr.populateClientResponse(response)
	svr.sendToClient(from, secretNum, raw.SessionId, raw.RequestId, pb.Packet_CREATE_SESSION_RESPONSE, response)
}

func (svr *Server) recvDestroySessionRequest(from *net.UDPAddr, secretNum uint32, raw *pb.Packet, msg *pb.DestroySessionRequest) {
	delete(svr.sessions, raw.SessionId)
	response := &pb.ClientResponse{Status: pb.ClientResponse_OK}
	svr.populateClientResponse(response)
	svr.sendToClient(from, secretNum, raw.SessionId, raw.RequestId, pb.Packet_DESTROY_SESSION_RESPONSE, response)
}

func (svr *Server) recvQueryRequest(from *net.UDPAddr, secretNum uint32, raw *pb.Packet, msg *pb.QueryRequest) {
	err := svr.manager.Machine.ValidateQuery(msg.Command)
	response := rpc.ClientResponseFromError(err)
	svr.populateClientResponse(response)
	if err == nil {
		response.Payload = svr.manager.Machine.Query(msg.Command)
	}
	svr.sendToClient(from, secretNum, raw.SessionId, raw.RequestId, pb.Packet_QUERY_RESPONSE, response)
}

func (svr *Server) recvWriteRequest(from *net.UDPAddr, secretNum uint32, raw *pb.Packet, msg *pb.WriteRequest) {
	if err := svr.manager.Machine.ValidateApply(msg.Command); err != nil {
		response := rpc.ClientResponseFromError(err)
		svr.populateClientResponse(response)
		svr.sendToClient(from, secretNum, raw.SessionId, raw.RequestId, pb.Packet_WRITE_RESPONSE, response)
		return
	}
	entries := []*pb.LogEntry{
		&pb.LogEntry{
			Term:    svr.manager.CurrentTerm,
			Type:    pb.LogEntry_COMMAND,
			Payload: msg.Command,
		},
	}
	svr.manager.Append(entries, func(start, end uint64, err error) {
		svr.mutex.Lock()
		defer svr.mutex.Unlock()

		if err != nil {
			log.Printf("go-raft/server: %v", err)
		}

		svr.manager.OnCommit(end - 1, func(ok bool) {
			svr.mutex.Lock()
			defer svr.mutex.Unlock()

			var response pb.ClientResponse
			if ok {
				response.Status = pb.ClientResponse_OK
			} else {
				response.Status = pb.ClientResponse_CONFLICT
			}
			svr.populateClientResponse(&response)
			svr.sendToClient(from, secretNum, raw.SessionId, raw.RequestId, pb.Packet_WRITE_RESPONSE, &response)
		})
	})
}

func (svr *Server) recvGetConfigurationRequest(from *net.UDPAddr, secretNum uint32, raw *pb.Packet, msg *pb.GetConfigurationRequest) {
	response := &pb.ClientResponse{Status: pb.ClientResponse_OK}
	response.Configuration = svr.manager.ActiveConfiguration
	svr.populateClientResponse(response)
	svr.sendToClient(from, secretNum, raw.SessionId, raw.RequestId, pb.Packet_GET_CONFIGURATION_RESPONSE, response)
}

func (svr *Server) recvSetConfigurationRequest(from *net.UDPAddr, secretNum uint32, raw *pb.Packet, msg *pb.SetConfigurationRequest) {
	response := &pb.ClientResponse{}
	respond := func() {
		svr.populateClientResponse(response)
		svr.sendToClient(from, secretNum, raw.SessionId, raw.RequestId, pb.Packet_SET_CONFIGURATION_RESPONSE, response)
	}

	byId, err := vetConfig(msg.Configuration, svr.manager.ActiveConfiguration)
	if err != nil {
		response.Status = pb.ClientResponse_INVALID_ARGUMENT
		response.ErrorMessage = err.Error()
		respond()
		return
	}
	self := byId[svr.selfId]

	if svr.manager.ActiveConfiguration.Id != svr.manager.CommittedConfigurationId {
		response.Status = pb.ClientResponse_CONFLICT
		response.ErrorMessage = "a configuration change is already pending"
		respond()
		return
	}

	entries := []*pb.LogEntry{
		&pb.LogEntry{
			Term:    svr.manager.CurrentTerm,
			Type:    pb.LogEntry_CONFIGURATION,
			Payload: util.MustMarshalProto(msg.Configuration),
		},
	}
	svr.manager.Append(entries, func(_, end uint64, err error) {
		svr.mutex.Lock()
		defer svr.mutex.Unlock()

		c := importConfiguration(msg.Configuration, svr.c, svr.selfId, svr.selfAddr)
		for _, p := range c.peers {
			if p.nextIndex == 0 {
				p.nextIndex = svr.manager.NextIndex()
			}
		}

		svr.manager.ActiveConfiguration = msg.Configuration
		svr.c = c
		svr.sendAppendEntries()
		if self.State != pb.Peer_STABLE {
			svr.becomeFollower()
		}

		svr.manager.OnCommit(end - 1, func(ok bool) {
			svr.mutex.Lock()
			defer svr.mutex.Unlock()

			if ok {
				response.Status = pb.ClientResponse_OK
			} else {
				response.Status = pb.ClientResponse_CONFLICT
			}
			respond()
		})
	})
}

func (svr *Server) populateClientResponse(response *pb.ClientResponse) {
	response.LeaderId = svr.currentLeaderId
	response.CurrentTerm = svr.manager.CurrentTerm
	response.CommitIndex = svr.manager.CommitIndex
	response.NextIndex = svr.manager.NextIndex()
}

func (svr *Server) becomeBlank() {
	svr.state = blank
	svr.cluster = uuid.Nil
	svr.sessions = nil
	svr.manager.CurrentTerm = 0
	svr.manager.VotedFor = 0
	svr.currentLeaderId = 0
	svr.lastLeaderTerm = 0
	svr.electionTimer = 999
	svr.heartbeatTimer = 99
}

func (svr *Server) becomeFollower() {
	if svr.state != watcher {
		svr.state = follower
	}
	svr.sessions = nil
	svr.currentLeaderId = 0
	svr.electionTimer = svr.newElectionTimer()
	svr.heartbeatTimer = 0 // unused
	for _, p := range svr.c.peers {
		p.vote = unknownVote // unused
		p.nextIndex = 0  // unused
		p.matchIndex = 0  // unused
	}
}

func (svr *Server) becomeCandidate() {
	util.Assert(svr.state >= follower, "cannot jump from blank/watcher to candidate")
	svr.manager.UpdateTerm(svr.manager.CurrentTerm+1, svr.selfId, func(err error) {
		svr.mutex.Lock()
		defer svr.mutex.Unlock()

		if err != nil {
			svr.becomeFollower()
			return
		}

		svr.state = candidate
		svr.sessions = nil
		svr.currentLeaderId = 0
		svr.electionTimer = svr.newElectionTimer()
		svr.heartbeatTimer = svr.newHeartbeatTimer()
		for _, p := range svr.c.peers {
			p.vote = unknownVote
			if p.id == svr.selfId {
				p.vote = yeaVote
			}
			p.nextIndex = 0  // unused
			p.matchIndex = 0  // unused
		}
		svr.sendVoteRequests()
		if svr.c.yeaQuorum() {
			svr.becomeLeader()
		}
	})
}

func (svr *Server) becomeLeader() {
	util.Assert(svr.state >= candidate, "cannot jump from blank/watcher/follower to candidate")
	svr.state = leader
	svr.sessions = make(map[uint32]sessionEntry)
	svr.currentLeaderId = svr.selfId
	svr.lastLeaderTerm = svr.manager.CurrentTerm
	svr.electionTimer = svr.newElectionTimer()
	svr.heartbeatTimer = svr.newHeartbeatTimer()
	for _, p := range svr.c.peers {
		p.vote = unknownVote
		if p.id == svr.selfId {
			p.vote = yeaVote
		}
		p.nextIndex = svr.manager.NextIndex()
		p.matchIndex = 0
	}
	entries := []*pb.LogEntry{
		&pb.LogEntry{
			Term:    svr.manager.CurrentTerm,
			Type:    pb.LogEntry_NOP,
			Payload: nil,
		},
	}
	svr.sendAppendEntries()
	svr.manager.Append(entries, func(_, _ uint64, _ error) {
		svr.mutex.Lock()
		defer svr.mutex.Unlock()

		self := svr.c.byId[svr.selfId]
		self.vote = yeaVote
		self.nextIndex = svr.manager.NextIndex()
		self.matchIndex = self.nextIndex - 1
		svr.advanceCommitIndex()
	})
}

func (svr *Server) advanceCommitIndex() {
	newCommitIndex := svr.c.matchQuorum()
	if svr.manager.Term(newCommitIndex) == svr.manager.CurrentTerm {
		svr.manager.CommitTo(newCommitIndex)
	}
	if svr.c.yeaQuorum() {
		svr.electionTimer = svr.newElectionTimer()
		for _, p := range svr.c.peers {
			p.vote = unknownVote
			if p.id == svr.selfId {
				p.vote = yeaVote
			}
		}
	}
}

type stateTransition struct {
	oldState pb.Peer_State
	newState pb.Peer_State
}

var legalStateTransitions = map[stateTransition]struct{}{
	{pb.Peer_BLANK, pb.Peer_BLANK}:    struct{}{},
	{pb.Peer_BLANK, pb.Peer_LEARNING}: struct{}{},
	{pb.Peer_BLANK, pb.Peer_JOINING}:  struct{}{},

	{pb.Peer_LEARNING, pb.Peer_BLANK}:    struct{}{},
	{pb.Peer_LEARNING, pb.Peer_LEARNING}: struct{}{},
	{pb.Peer_LEARNING, pb.Peer_JOINING}:  struct{}{},

	{pb.Peer_JOINING, pb.Peer_JOINING}: struct{}{},
	{pb.Peer_JOINING, pb.Peer_STABLE}:  struct{}{},

	{pb.Peer_STABLE, pb.Peer_STABLE}:  struct{}{},
	{pb.Peer_STABLE, pb.Peer_LEAVING}: struct{}{},

	{pb.Peer_LEAVING, pb.Peer_BLANK}:   struct{}{},
	{pb.Peer_LEAVING, pb.Peer_LEAVING}: struct{}{},
}

func (svr *Server) applyConfig(cfg *pb.Configuration) {}

func (svr *Server) newElectionTimer() uint32 {
	return 100 + uint32(svr.rng.Intn(50))
}

func (svr *Server) newHeartbeatTimer() uint32 {
	return 10 + uint32(svr.rng.Intn(5))
}

func nop(_ error) {}

func vetConfig(cfg *pb.Configuration, old *pb.Configuration) (map[uint32]*pb.Peer, error) {
	oldById := make(map[uint32]*pb.Peer)
	for _, pbp := range old.Peers {
		oldById[pbp.Id] = pbp
	}

	newById := make(map[uint32]*pb.Peer)
	for _, pbp := range cfg.Peers {
		newById[pbp.Id] = pbp
	}

	var errs []error

	var expectId uint64 = 1
	if old != nil {
		expectId = old.Id + 1
	}
	if cfg.Id < expectId {
		errs = append(errs, fmt.Errorf("configuration has id %d, which is older than %d", cfg.Id, expectId))
	}

	var foundStable bool
	for _, pbp := range cfg.Peers {
		oldpbp, _ := oldById[pbp.Id]

		if pbp.State == pb.Peer_STABLE {
			foundStable = true
		}

		if pbp.Addr == "" {
			errs = append(errs, fmt.Errorf("server %d has empty Addr", pbp.Id))
		}
		if oldpbp != nil && pbp.Addr != oldpbp.Addr {
			errs = append(errs, fmt.Errorf("server %d changes Addr from %q to %q", pbp.Id, oldpbp.Addr, pbp.Addr))
		}

		var t stateTransition
		if oldpbp == nil {
			t = stateTransition{0, pbp.State}
		} else {
			t = stateTransition{oldpbp.State, pbp.State}
		}
		_, legal := legalStateTransitions[t]
		if !legal {
			errs = append(errs, fmt.Errorf("server %d has forbidden transition of %v to %v", pbp.Id, t.oldState, t.newState))
		}
	}
	for _, oldpbp := range old.Peers {
		pbp, _ := newById[oldpbp.Id]
		var t stateTransition
		if pbp == nil {
			t = stateTransition{oldpbp.State, 0}
		} else {
			t = stateTransition{oldpbp.State, pbp.State}
		}
		_, legal := legalStateTransitions[t]
		if !legal {
			errs = append(errs, fmt.Errorf("server %d has forbidden transition of %v to %v", pbp.Id, t.oldState, t.newState))
		}
	}
	if !foundStable {
		errs = append(errs, fmt.Errorf("at least one STABLE server is required"))
	}
	return newById, multierror.New(errs)
}

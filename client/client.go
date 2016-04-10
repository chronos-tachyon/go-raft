package client

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/satori/go.uuid"

	"github.com/chronos-tachyon/go-raft/internal/multierror"
	"github.com/chronos-tachyon/go-raft/internal/util"
	pb "github.com/chronos-tachyon/go-raft/proto"
	"github.com/chronos-tachyon/go-raft/rpc"
)

type have struct {
	packet   *pb.Packet
	deadline time.Time
}

type want struct {
	ptype    pb.Packet_Type
	sid      uint32
	rid      uint32
	deadline time.Time
	done     func(*pb.Packet)
}

type sidrid struct {
	sid uint32
	rid uint32
}

type Client struct {
	conn      *net.UDPConn
	rng       *rand.Rand
	secrets   rpc.SecretsManager
	secretNum uint32
	policy    RetryPolicy
	wantch    chan want
	gotch     chan *pb.Packet
	wg        sync.WaitGroup
	mutex     sync.Mutex
	uuid      uuid.UUID
	addr      string
	sid       uint32
}

type Params struct {
	ClusterUUID    string
	ClusterAddr    string
	Bind           string
	RandomSource   rand.Source
	SecretsManager rpc.SecretsManager
	SecretNum      uint32
	RetryPolicy    RetryPolicy
}

func New(params Params) (*Client, error) {
	var cluster uuid.UUID
	if params.ClusterUUID != "" {
		var err error
		cluster, err = uuid.FromString(params.ClusterUUID)
		if err != nil {
			return nil, fmt.Errorf("failed to parse Params.ClusterUUID: %v", err)
		}
	}
	if params.ClusterAddr == "" {
		return nil, fmt.Errorf("must specify Params.ClusterAddr")
	}
	bind := params.Bind
	if bind == "" {
		bind = "[::]:0"
	}
	rngsrc := params.RandomSource
	if rngsrc == nil {
		rngsrc = rand.NewSource(time.Now().UnixNano())
	}
	rng := rand.New(rngsrc)
	if params.SecretsManager == nil {
		return nil, fmt.Errorf("must specify Params.SecretsManager")
	}
	policy := params.RetryPolicy
	if policy == nil {
		policy = DefaultRetryPolicy{}
	}

	udpbind, err := net.ResolveUDPAddr("udp", bind)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpbind)
	if err != nil {
		return nil, err
	}

	cli := &Client{
		conn:      conn,
		rng:       rng,
		secrets:   params.SecretsManager,
		secretNum: params.SecretNum,
		policy:    policy,
		sid:       rng.Uint32(),
		wantch:    make(chan want),
		gotch:     make(chan *pb.Packet),
		uuid:      cluster,
		addr:      params.ClusterAddr,
	}
	cli.wg.Add(2)
	go cli.recvLoop()
	go cli.wantLoop()
	var wg sync.WaitGroup
	wg.Add(1)
	cli.createSession(func(cberr error) {
		err = cberr
		wg.Done()
	})
	wg.Wait()
	if err != nil {
		if rpcerr, ok := err.(rpc.Error); !ok || rpcerr.Status != pb.ClientResponse_NOT_LEADER {
			return nil, err
		}
	}
	return cli, nil
}

func (cli *Client) Query(command []byte) (*pb.ClientResponse, error) {
	var resp *pb.ClientResponse
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	cli.QueryAsync(command, func(cbresp *pb.ClientResponse, cberr error) {
		resp, err = cbresp, cberr
		wg.Done()
	})
	wg.Wait()
	return resp, err
}

func (cli *Client) QueryAsync(command []byte, callback func(*pb.ClientResponse, error)) {
	var req pb.QueryRequest
	req.Command = command
	go cli.retryLoop(pb.Packet_QUERY_REQUEST, &req, callback)
}

func (cli *Client) Write(command []byte) (*pb.ClientResponse, error) {
	var resp *pb.ClientResponse
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	cli.WriteAsync(command, func(cbresp *pb.ClientResponse, cberr error) {
		resp, err = cbresp, cberr
		wg.Done()
	})
	wg.Wait()
	return resp, err
}

func (cli *Client) WriteAsync(command []byte, callback func(*pb.ClientResponse, error)) {
	var req pb.WriteRequest
	req.Command = command
	go cli.retryLoop(pb.Packet_WRITE_REQUEST, &req, callback)
}

func (cli *Client) GetConfiguration() (*pb.ClientResponse, error) {
	var resp *pb.ClientResponse
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	cli.GetConfigurationAsync(func(cbresp *pb.ClientResponse, cberr error) {
		resp, err = cbresp, cberr
		wg.Done()
	})
	wg.Wait()
	return resp, err
}

func (cli *Client) GetConfigurationAsync(callback func(*pb.ClientResponse, error)) {
	var req pb.GetConfigurationRequest
	go cli.retryLoop(pb.Packet_GET_CONFIGURATION_REQUEST, &req, callback)
}

func (cli *Client) SetConfiguration(cfg *pb.Configuration) (*pb.ClientResponse, error) {
	var resp *pb.ClientResponse
	var err error
	var wg sync.WaitGroup
	wg.Add(1)
	cli.SetConfigurationAsync(cfg, func(cbresp *pb.ClientResponse, cberr error) {
		resp, err = cbresp, cberr
		wg.Done()
	})
	wg.Wait()
	return resp, err
}

func (cli *Client) SetConfigurationAsync(cfg *pb.Configuration, callback func(*pb.ClientResponse, error)) {
	var req pb.SetConfigurationRequest
	req.Configuration = cfg
	go cli.retryLoop(pb.Packet_SET_CONFIGURATION_REQUEST, &req, callback)
}

func (cli *Client) Close() error {
	var req pb.DestroySessionRequest
	resp, err := cli.syncRetryLoop(pb.Packet_DESTROY_SESSION_REQUEST, &req)
	if err == nil && resp.Status != pb.ClientResponse_OK {
		log.Printf("go-raft: got ClientRequest.Status %d during shutdown", resp.Status)
	}

	close(cli.wantch)
	err = multierror.New([]error{err, cli.conn.Close()})
	cli.wg.Wait()
	return err
}

func (cli *Client) recvLoop() {
	for {
		packet, _, _, err := rpc.RecvUDP(cli.secrets, cli.conn)
		if err != nil {
			log.Printf("go-raft: %v", err)
			break
		}
		cli.gotch <- packet
	}
	close(cli.gotch)
	cli.wg.Done()
}

func (cli *Client) wantLoop() {
	haves := make(map[sidrid]have)
	wants := make(map[sidrid]want)
	timer := time.NewTimer(24 * time.Hour)

Outer:
	for {
		var now time.Time
		select {
		case w := <-cli.wantch:
			if w.ptype == 0 && w.sid == 0 && w.rid == 0 {
				break Outer
			}
			key := sidrid{w.sid, w.rid}
			if h, found := haves[key]; found {
				delete(haves, key)
				w.done(h.packet)
			} else {
				wants[key] = w
			}
			now = time.Now()

		case packet := <-cli.gotch:
			if packet == nil {
				break Outer
			}
			key := sidrid{packet.SessionId, packet.RequestId}
			if w, found := wants[key]; found {
				delete(wants, key)
				w.done(packet)
			} else {
				haves[key] = have{
					packet:   packet,
					deadline: time.Now().Add(1 * time.Hour),
				}
			}
			now = time.Now()

		case t := <-timer.C:
			now = t
		}

		var rmlist []sidrid
		next := now.Add(24 * time.Hour)
		for key, w := range wants {
			switch {
			case now.After(w.deadline):
				w.done(nil)
				rmlist = append(rmlist, key)
			case next.After(w.deadline):
				next = w.deadline
			}
		}
		for _, key := range rmlist {
			delete(wants, key)
		}
		rmlist = nil
		for key, h := range haves {
			switch {
			case now.After(h.deadline):
				rmlist = append(rmlist, key)
			case next.After(h.deadline):
				next = h.deadline
			}
		}
		for _, key := range rmlist {
			delete(haves, key)
		}
		timer.Stop()
		timer = time.NewTimer(next.Sub(now))
	}
	timer.Stop()
	for _, w := range wants {
		w.done(nil)
	}
	cli.wg.Done()
}

func (cli *Client) await(ptype pb.Packet_Type, sid, rid uint32, deadline time.Time) *pb.Packet {
	var result *pb.Packet
	var wg sync.WaitGroup
	wg.Add(1)
	donefn := func(packet *pb.Packet) {
		result = packet
		wg.Done()
	}
	cli.wantch <- want{
		ptype:    ptype,
		sid:      sid,
		rid:      rid,
		deadline: deadline,
		done:     donefn,
	}
	wg.Wait()
	return result
}

func (cli *Client) retryLoop(ptype pb.Packet_Type, msg proto.Message, callback func(*pb.ClientResponse, error)) {
	cli.mutex.Lock()
	cluster := cli.uuid
	leaderAddr := cli.addr
	cli.mutex.Unlock()

	rid := cli.rng.Uint32()

	var reqpkt pb.Packet
	if !uuid.Equal(cluster, uuid.Nil) {
		reqpkt.Cluster = cluster.Bytes()
	}
	reqpkt.SessionId = cli.sid
	reqpkt.RequestId = rid
	reqpkt.Type = ptype
	reqpkt.Payload = mustMarshalProto(msg)

Redirect:
	udpaddrs, err := resolve(leaderAddr)
	if err != nil {
		callback(nil, err)
		return
	}

	Outer:
	for retrynum := uint(0); true; retrynum++ {
		for addrnum, udpaddr := range udpaddrs {
		Redo:
			log.Printf("debug: sending %v, attempt %d/3, address %d/%d (%s)", ptype, retrynum+1, addrnum+1, len(udpaddrs), udpaddr)
			err = rpc.SendUDP(cli.secrets, cli.secretNum, cli.conn, udpaddr, &reqpkt)
			if err != nil {
				callback(nil, err)
				return
			}

			deadline := time.Now().Add(cli.policy.GetTimeout(retrynum))
			rsppkt := cli.await(ptype+1, cli.sid, rid, deadline)
			log.Printf("debug: returned")
			if rsppkt == nil {
				err := rpc.TimedOutError()
				retry := cli.policy.ShouldRetry(retrynum, err)
				if retry {
					continue Outer
				}
				callback(nil, err)
				return
			}

			var rsp pb.ClientResponse
			if err := proto.Unmarshal(rsppkt.Payload, &rsp); err != nil {
				callback(nil, err)
				return
			}
			if uuid.Equal(cluster, uuid.Nil) && len(rsppkt.Cluster) > 0 {
				cluster, err = uuid.FromBytes(rsppkt.Cluster)
				if err == nil {
					cli.mutex.Lock()
					cli.uuid = cluster
					cli.mutex.Unlock()
				}
			}
			if rsp.Status != pb.ClientResponse_OK {
				err = rpc.ErrorFromClientResponse(&rsp)
				if rsp.Status == pb.ClientResponse_NOT_LEADER && rsp.Redirect != "" {
					leaderAddr = rsp.Redirect
					log.Printf("go-raft: redirect to %q", leaderAddr)
					cli.mutex.Lock()
					cli.addr = leaderAddr
					cli.mutex.Unlock()
					goto Redirect
				}
				if rsp.Status == pb.ClientResponse_INVALID_SESSION {
					util.Assert(ptype != pb.Packet_CREATE_SESSION_REQUEST, "CREATE_SESSION_REQUEST call returned INVALID_SESSION!")
					var err error
					var wg sync.WaitGroup
					wg.Add(1)
					cli.createSession(func(cberr error) {
						err = cberr
						wg.Done()
					})
					wg.Wait()
					if err == nil {
						log.Printf("debug: got session, trying that again")
						goto Redo
					}
				}
				retry := cli.policy.ShouldRetry(retrynum, err)
				if retry {
					continue Outer
				}
				callback(nil, err)
				return
			}
			callback(&rsp, nil)
			return
		}
	}
	callback(nil, rpc.TimedOutError())
}

func (cli *Client) syncRetryLoop(ptype pb.Packet_Type, msg proto.Message) (*pb.ClientResponse, error) {
	var resp *pb.ClientResponse
	var err error
	cli.retryLoop(ptype, msg, func(cbresp *pb.ClientResponse, cberr error) {
		resp = cbresp
		err = cberr
	})
	return resp, err
}

func (cli *Client) createSession(cb func(error)) {
	var req pb.CreateSessionRequest
	go cli.retryLoop(pb.Packet_CREATE_SESSION_REQUEST, &req, func(_ *pb.ClientResponse, err error) {
		cb(err)
	})
}

func resolve(clusterList string) ([]*net.UDPAddr, error) {
	var addrs []*net.UDPAddr
	var errs []error
	hostports := strings.Split(clusterList, ",")
	for _, hostport := range hostports {
		host, port, err := net.SplitHostPort(hostport)
		if err != nil {
			return nil, err
		}
		ips, err := net.LookupIP(host)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		portnum, err := net.LookupPort("udp", port)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, ip := range ips {
			addrs = append(addrs, &net.UDPAddr{IP: ip, Port: portnum})
		}
	}
	return addrs, multierror.New(errs)
}

func mustMarshalProto(msg proto.Message) []byte {
	if data, err := proto.Marshal(msg); err != nil {
		panic(err)
	} else {
		return data
	}
}

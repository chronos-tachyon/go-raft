package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chronos-tachyon/go-raft/client"
	pb "github.com/chronos-tachyon/go-raft/proto"
	"github.com/chronos-tachyon/go-raft/rpc"
	"github.com/chronos-tachyon/go-raft/server"
	"github.com/chronos-tachyon/go-raft/storage"
)

func GainLeadership(server *server.Server) {
	fmt.Printf("NEW LEADER <%d>\n", server.Id())
}

func LoseLeadership(server *server.Server) {
	fmt.Printf("OLD LEADER <%d>\n", server.Id())
}

func Lonely(server *server.Server) {
	fmt.Printf("SO LONELY! <%d>\n", server.Id())
}

func NotLonely(server *server.Server) {
	fmt.Printf("WE'RE BACK! <%d>\n", server.Id())
}

type stateMachine struct {
	x, y uint8
}

func (sm *stateMachine) Snapshot() []byte {
	return []byte{'x', sm.x, 'y', sm.y}
}

func (sm *stateMachine) Restore(snapshot []byte) {
	if len(snapshot) == 0 {
		*sm = stateMachine{}
		return
	}
	if len(snapshot) != 4 || snapshot[0] != 'x' || snapshot[2] != 'y' {
		panic("invalid snapshot")
	}
	sm.x = snapshot[1]
	sm.y = snapshot[3]
}

func (sm *stateMachine) ValidateQuery(command []byte) error {
	if len(command) != 1 {
		return fmt.Errorf("invalid command")
	}
	switch command[0] {
	case 'x', 'y':
		return nil
	default:
		return fmt.Errorf("unknown command: %#02x", command[0])
	}
}

func (sm *stateMachine) Query(command []byte) []byte {
	if len(command) == 1 {
		switch command[0] {
		case 'x':
			return []byte{sm.x}
		case 'y':
			return []byte{sm.y}
		}
	}
	return nil
}

func (sm *stateMachine) ValidateApply(command []byte) error {
	if len(command) != 0 && len(command) != 2 {
		return fmt.Errorf("invalid command")
	}
	switch command[0] {
	case 'x', 'y':
		return nil
	default:
		return fmt.Errorf("unknown command: %#02x", command[0])
	}
}

func (sm *stateMachine) Apply(command []byte) {
	if len(command) == 0 {
		*sm = stateMachine{}
	}
	if len(command) == 2 {
		switch command[0] {
		case 'x':
			sm.x += command[1]
		case 'y':
			sm.y += command[1]
		}
	}
}

func (sm *stateMachine) String() string {
	return fmt.Sprintf("x:%d y:%d", sm.x, sm.y)
}

func showRafts(servers []*server.Server) {
	for _, svr := range servers {
		fmt.Println(svr)
	}
	fmt.Println()
}

type configItem struct {
	id   uint32
	addr string
}

var configuration = []configItem{
	{1, "127.0.0.1:9001"},
	{2, "127.0.0.1:9002"},
	{3, "127.0.0.1:9003"},
	{4, "127.0.0.1:9004"},
	{5, "127.0.0.1:9005"},
}

func main() {
	secrets := make(rpc.SimpleSecretsManager)
	secrets[1] = rpc.Secret{Key: nil, Access: rpc.ReadAccess}
	secrets[2] = rpc.Secret{Key: []byte{'f', 'o', 'o'}, Access: rpc.WriteAccess}
	secrets[3] = rpc.Secret{Key: []byte{'b', 'a', 'r'}, Access: rpc.AdminAccess}
	secrets[4] = rpc.Secret{Key: []byte{'b', 'a', 'z'}, Access: rpc.PeerAccess}

	uuid := "5636988b-8487-42f0-a13e-2962a1e1d278"

	servers := make([]*server.Server, 0, len(configuration))
	for i, item := range configuration {
		server, err := server.New(server.Params{
			ServerId:       item.id,
			ServerAddr:     item.addr,
			Storage:        storage.NewMemStorage(),
			StateMachine:   new(stateMachine),
			SecretsManager: secrets,
			PeerSecretNum:  4,
		})
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
		server.OnGainLeadership(GainLeadership)
		server.OnLoseLeadership(LoseLeadership)
		server.OnLonely(Lonely)
		server.OnNotLonely(NotLonely)
		if err := server.Start(); err != nil {
			log.Fatalf("fatal: %v", err)
		}
		if i == 0 {
			server.Bootstrap(uuid)
		}
		servers = append(servers, server)
	}

	var tickCount uint32
	tick := func() {
		tickCount++
		for _, server := range servers {
			server.Tick()
		}
		fmt.Println("T", tickCount)
		showRafts(servers)
	}

	fmt.Println("T", 0)
	showRafts(servers)

	var clusterAddrs []string
	for _, server := range servers {
		clusterAddrs = append(clusterAddrs, server.Addr())
	}
	clusterList := strings.Join(clusterAddrs, ",")

	cli, err := client.New(client.Params{
		ClusterUUID:    uuid,
		ClusterAddr:    clusterList,
		Bind:           "127.0.0.1:9006",
		SecretsManager: secrets,
		SecretNum:      3,
	})
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}

	resp, err := cli.GetConfiguration()
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}

	cfg := resp.Configuration
	cfg.Id++
	for i, server := range servers {
		if i > 0 {
			cfg.Peers = append(cfg.Peers, &pb.Peer{
				Id: server.Id(),
				Addr: server.Addr(),
				State: pb.Peer_JOINING,
			})
		}
	}

	var x uint32
	cli.SetConfigurationAsync(cfg, func(resp *pb.ClientResponse, err error) {
		atomic.AddUint32(&x, 1)
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
	})
	for atomic.LoadUint32(&x) == 0 {
		tick()
	}

	cfg.Id++
	for _, pbp := range cfg.Peers {
		pbp.State = pb.Peer_STABLE
	}

	x = 0
	cli.SetConfigurationAsync(cfg, func(resp *pb.ClientResponse, err error) {
		atomic.AddUint32(&x, 1)
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
	})
	for atomic.LoadUint32(&x) == 0 {
		tick()
	}

	var signaled uint32
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigch)
	go (func() {
		sig := <-sigch
		log.Printf("got signal %v", sig)
		atomic.AddUint32(&signaled, 1)
	})()

	for atomic.LoadUint32(&signaled) == 0 {
		time.Sleep(5 * time.Millisecond)
		switch tickCount {
		case 40:
			fmt.Println("--- BANG! 1 ---")
			rpc.SetFaultInjectorFunction(func(from, to *net.UDPAddr) bool {
				i := idFromAddr(servers, from)
				j := idFromAddr(servers, to)
				return (i == 1 && j == 2) || (i == 2 && j == 1)
			})

		case 45:
			cli.WriteAsync([]byte{'x', 1}, func(resp *pb.ClientResponse, err error) {
				if err != nil {
					log.Fatalf("fatal: %v", err)
				}
				if resp.Status == pb.ClientResponse_OK {
					fmt.Println("--- COMMITTED x+=1 ---")
				} else {
					fmt.Println("--- REJECTED  x+=1 ---")
				}
			})

		case 75:
			cli.WriteAsync([]byte{'y', 2}, func(resp *pb.ClientResponse, err error) {
				if err != nil {
					log.Fatalf("fatal: %v", err)
				}
				if resp.Status == pb.ClientResponse_OK {
					fmt.Println("--- COMMITTED y+=2 ---")
				} else {
					fmt.Println("--- REJECTED  y+=2 ---")
				}
			})

		case 175:
			fmt.Println("--- HEAL! 1 ---")
			rpc.SetFaultInjectorFunction(nil)

		case 180:
			cli.QueryAsync([]byte{'x'}, func(resp *pb.ClientResponse, err error) {
				if err == nil {
					fmt.Printf("--- QUERY: x=%d ---\n", resp.Payload[0])
				} else {
					fmt.Printf("--- QUERY: %v ---\n", err)
				}
			})

		case 200:
			fmt.Println("--- BANG! 2 ---")
			rpc.SetFaultInjectorFunction(func(from, to *net.UDPAddr) bool {
				i := idFromAddr(servers, from)
				j := idFromAddr(servers, to)
				group := map[uint32]uint8{0: 1, 1: 1, 2: 1, 3: 1, 4: 2, 5: 2}
				return group[i] != group[j]
			})

		case 315:
			cli.WriteAsync([]byte{'x', 2}, func(resp *pb.ClientResponse, err error) {
				if err != nil {
					log.Fatalf("fatal: %v", err)
				}
				if resp.Status == pb.ClientResponse_OK {
					fmt.Println("--- COMMITTED x+=2 ---")
				} else {
					fmt.Println("--- REJECTED  x+=2 ---")
				}
			})

		case 425:
			fmt.Println("--- HEAL! 2 ---")
			rpc.SetFaultInjectorFunction(nil)

		case 460:
			atomic.AddUint32(&signaled, 1)
		}
		tick()
	}
	for _, server := range servers {
		server.Stop()
	}
}

func idFromAddr(servers []*server.Server, addr *net.UDPAddr) uint32 {
	for _, server := range servers {
		serverAddr, err := net.ResolveUDPAddr("udp", server.Addr())
		if err == nil && equalUDPAddr(addr, serverAddr) {
			return server.Id()
		}
	}
	return 0
}

func equalUDPAddr(a, b *net.UDPAddr) bool {
	return a.IP.Equal(b.IP) && a.Port == b.Port && a.Zone == b.Zone
}

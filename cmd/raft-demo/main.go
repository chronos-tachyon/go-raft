package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/chronos-tachyon/go-raft"
	"github.com/chronos-tachyon/go-raft/packet"
)

func mustResolveUDPAddr(n, a string) *net.UDPAddr {
	addr, err := net.ResolveUDPAddr(n, a)
	if err != nil {
		panic(err)
	}
	return addr
}

var counter uint32

func MyFaultInjector(from, to packet.NodeId) bool {
	n := atomic.AddUint32(&counter, 1)
	if n < 100 {
		return false
	}
	if n == 100 {
		fmt.Println("BANG!")
		return (from == 3 && to == 5) || (from == 5 && to == 3)
	}
	if n < 1000 {
		return (from == 3 && to == 5) || (from == 5 && to == 3)
	}
	if n == 1000 {
		fmt.Println("FIXED!")
		return false
	}
	return false
}

func GainLeadership(node *raft.Node) {
	fmt.Printf("NEW LEADER: %v\n", node)
}

func LoseLeadership(node *raft.Node) {
	fmt.Printf("OLD LEADER: %v\n", node)
}

func main() {
	rand.Seed(0x56f2fb72)
	raft.SetFaultInjectorFunction(MyFaultInjector)

	peerlist := []raft.Peer{
		raft.Peer{Id: 1, Addr: mustResolveUDPAddr("udp", "localhost:9001")},
		raft.Peer{Id: 2, Addr: mustResolveUDPAddr("udp", "localhost:9002")},
		raft.Peer{Id: 3, Addr: mustResolveUDPAddr("udp", "localhost:9003")},
		raft.Peer{Id: 4, Addr: mustResolveUDPAddr("udp", "localhost:9004")},
		raft.Peer{Id: 5, Addr: mustResolveUDPAddr("udp", "localhost:9005")},
	}

	nodes := make([]*raft.Node, 0, len(peerlist))
	for _, peer := range peerlist {
		node, err := raft.New(peer.Id, peerlist)
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
		node.OnGainLeadership(GainLeadership)
		node.OnLoseLeadership(LoseLeadership)
		nodes = append(nodes, node)
	}
	fmt.Printf("%v\n", nodes)
	for {
		time.Sleep(5 * time.Millisecond)
		for _, node := range nodes {
			node.Tick()
		}
		fmt.Printf("%v\n", nodes)
	}
}

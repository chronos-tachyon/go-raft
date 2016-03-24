package main

import (
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/chronos-tachyon/go-raft"
	"github.com/chronos-tachyon/go-raft/packet"
)

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

const configuration = `
---
nodes:
  - id: 1
    addr: localhost:9001
  - id: 2
    addr: localhost:9002
  - id: 3
    addr: localhost:9003
  - id: 4
    addr: localhost:9004
  - id: 5
    addr: localhost:9005
`

func main() {
	rand.Seed(0x56f2fb72)
	raft.SetFaultInjectorFunction(MyFaultInjector)

	cfg, err := raft.ParseConfig([]byte(configuration))
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
	nodes := make([]*raft.Node, 0, len(cfg.Nodes))
	for _, item := range cfg.Nodes {
		node, err := raft.New(cfg, item.Id)
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

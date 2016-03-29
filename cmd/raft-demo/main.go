package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chronos-tachyon/go-raft"
	"github.com/chronos-tachyon/go-raft/packet"
)

var counter uint32

var faultMatrix = map[packet.PeerId]map[packet.PeerId]bool{
	1: map[packet.PeerId]bool{1: false, 2: false, 3: false, 4: false, 5: false},
	2: map[packet.PeerId]bool{1: false, 2: false, 3: false, 4: false, 5: false},
	3: map[packet.PeerId]bool{1: false, 2: false, 3: false, 4: false, 5: true},
	4: map[packet.PeerId]bool{1: false, 2: false, 3: false, 4: false, 5: false},
	5: map[packet.PeerId]bool{1: false, 2: false, 3: true, 4: false, 5: false},
}

func MyFaultInjector(from, to packet.PeerId) bool {
	n := atomic.AddUint32(&counter, 1)
	if n < 100 {
		return false
	}
	if n == 100 {
		fmt.Println("BANG!")
		return faultMatrix[from][to]
	}
	if n < 1000 {
		return faultMatrix[from][to]
	}
	if n == 1000 {
		fmt.Println("FIXED!")
		return false
	}
	return false
}

func GainLeadership(peer *raft.Raft) {
	fmt.Printf("NEW LEADER: %v\n", peer)
}

func LoseLeadership(peer *raft.Raft) {
	fmt.Printf("OLD LEADER: %v\n", peer)
}

func Lonely(peer *raft.Raft) {
	fmt.Printf("SO LONELY! %v\n", peer)
}

func NotLonely(peer *raft.Raft) {
	fmt.Printf("WE'RE BACK! %v\n", peer)
}

const configuration = `
---
peers:
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
	peers := make([]*raft.Raft, 0, len(cfg.Peers))
	for _, item := range cfg.Peers {
		peer, err := raft.New(cfg, item.Id)
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
		peer.OnGainLeadership(GainLeadership)
		peer.OnLoseLeadership(LoseLeadership)
		peer.OnLonely(Lonely)
		peer.OnNotLonely(NotLonely)
		err = peer.Start()
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
		peers = append(peers, peer)
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

	fmt.Printf("%v\n", peers)
	for atomic.LoadUint32(&signaled) == 0 {
		time.Sleep(5 * time.Millisecond)
		for _, peer := range peers {
			peer.Tick()
		}
		fmt.Printf("%v\n", peers)
	}
	for _, peer := range peers {
		peer.Stop()
	}
}

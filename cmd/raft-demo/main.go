package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/chronos-tachyon/go-raft"
)

func GainLeadership(peer *raft.Raft) {
	fmt.Printf("NEW LEADER <%d>\n", peer.Id())
}

func LoseLeadership(peer *raft.Raft) {
	fmt.Printf("OLD LEADER <%d>\n", peer.Id())
}

func Lonely(peer *raft.Raft) {
	fmt.Printf("SO LONELY! <%d>\n", peer.Id())
}

func NotLonely(peer *raft.Raft) {
	fmt.Printf("WE'RE BACK! <%d>\n", peer.Id())
}

type stateMachine struct {
	x, y uint8
}

func (sm *stateMachine) Snapshot() []byte {
	return []byte{'x', sm.x, 'y', sm.y}
}

func (sm *stateMachine) Restore(snapshot []byte) {
	if len(snapshot) != 4 || snapshot[0] != 'x' || snapshot[2] != 'y' {
		panic("invalid snapshot")
	}
	sm.x = snapshot[1]
	sm.y = snapshot[3]
}

func (sm *stateMachine) Commit(command []byte) {
	if len(command) != 2 {
		panic("invalid command")
	}
	switch command[0] {
	case 'x':
		sm.x += command[1]
	case 'y':
		sm.y += command[1]
	}
}

func (sm *stateMachine) String() string {
	return fmt.Sprintf("x:%d y:%d", sm.x, sm.y)
}

func leader(rafts []*raft.Raft) *raft.Raft {
	for _, raft := range rafts {
		if raft.IsLeader() {
			return raft
		}
	}
	return rafts[0]
}

func showRafts(rafts []*raft.Raft) {
	for _, raft := range rafts {
		fmt.Println(raft)
	}
	fmt.Println()
}

type configItem struct {
	id raft.PeerId
	addr string
}

var configuration = []configItem{
	{1, "localhost:9001"},
	{2, "localhost:9002"},
	{3, "localhost:9003"},
	{4, "localhost:9004"},
	{5, "localhost:9005"},
}

func main() {
	peers := make([]*raft.Raft, 0, len(configuration))
	for _, item := range configuration {
		peer, err := raft.New(&stateMachine{}, item.id, item.addr, false)
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
		peers = append(peers, peer)
	}
	for _, p := range peers {
		for _, q := range peers {
			if p != q {
				p.AddPeer(q.Id(), q.Addr().String())
			}
		}
	}
	for _, peer := range peers {
		peer.OnGainLeadership(GainLeadership)
		peer.OnLoseLeadership(LoseLeadership)
		peer.OnLonely(Lonely)
		peer.OnNotLonely(NotLonely)
		if err := peer.Start(); err != nil {
			log.Fatalf("fatal: %v", err)
		}
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

	var tickCount uint32
	fmt.Println("T", 0)
	showRafts(peers)
	for atomic.LoadUint32(&signaled) == 0 {
		tickCount++
		time.Sleep(5 * time.Millisecond)
		switch tickCount {
		case 40:
			fmt.Println("--- BANG! 1 ---")
			raft.SetFaultInjectorFunction(func(i, j raft.PeerId) bool {
				return (i == 1 && j == 2) || (i == 2 && j == 1)
			})
		case 45:
			leader(peers).Append([]byte{'x', 1}, func(ok bool) {
				if ok {
					fmt.Println("--- COMMITTED x+=1 ---")
				} else {
					fmt.Println("--- REJECTED x+=1 ---")
				}
			})
		case 142:
			leader(peers).Append([]byte{'y', 2}, func(ok bool) {
				if ok {
					fmt.Println("--- COMMITTED y+=2 ---")
				} else {
					fmt.Println("--- REJECTED y+=2 ---")
				}
			})
		case 165:
			fmt.Println("--- HEAL! 1 ---")
			raft.SetFaultInjectorFunction(nil)
		case 200:
			fmt.Println("--- BANG! 2 ---")
			raft.SetFaultInjectorFunction(func(i, j raft.PeerId) bool {
				group := map[raft.PeerId]uint8{
					1: 1,
					2: 1,
					3: 1,
					4: 2,
					5: 2,
				}
				return group[i] != group[j]
			})
		case 315:
			leader(peers).Append([]byte{'x', 2}, func(ok bool) {
				if ok {
					fmt.Println("--- COMMITTED x+=2 ---")
				} else {
					fmt.Println("--- REJECTED x+=2 ---")
				}
			})
		case 500:
			fmt.Println("--- HEAL! 2 ---")
			raft.SetFaultInjectorFunction(nil)
		case 600:
			atomic.AddUint32(&signaled, 1)
		}
		for _, peer := range peers {
			peer.Tick()
		}
		fmt.Println("T", tickCount)
		showRafts(peers)
	}
	for _, peer := range peers {
		peer.Stop()
	}
}

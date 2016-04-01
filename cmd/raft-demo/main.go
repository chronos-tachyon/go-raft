package main

import (
	"errors"
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

var errNotFound = errors.New("not found")

type memoryStorage map[string][]byte

func (ms memoryStorage) Store(filename string, data []byte) error {
	ms[filename] = data
	return nil
}

func (ms memoryStorage) Retrieve(filename string) ([]byte, error) {
	data, found := ms[filename]
	if !found {
		return nil, errNotFound
	}
	return data, nil
}

func (ms memoryStorage) Delete(filename string) error {
	_, found := ms[filename]
	if !found {
		return errNotFound
	}
	delete(ms, filename)
	return nil
}

func (ms memoryStorage) IsNotFound(err error) bool {
	return err == errNotFound
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
	id   uint32
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
		storage := make(memoryStorage)
		sm := &stateMachine{}
		peer, err := raft.New(storage, sm, item.id, item.addr)
		if err != nil {
			log.Fatalf("fatal: %v", err)
		}
		peer.OnGainLeadership(GainLeadership)
		peer.OnLoseLeadership(LoseLeadership)
		peer.OnLonely(Lonely)
		peer.OnNotLonely(NotLonely)
		if err := peer.Start(); err != nil {
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
			raft.SetFaultInjectorFunction(func(i, j uint32) bool {
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
			raft.SetFaultInjectorFunction(func(i, j uint32) bool {
				group := map[uint32]uint8{1: 1, 2: 1, 3: 1, 4: 2, 5: 2}
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

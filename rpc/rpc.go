package rpc

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/golang/protobuf/proto"

	pb "github.com/chronos-tachyon/go-raft/proto"
)

var faultInjectorMutex sync.Mutex
var faultInjectorFunction func(from, to *net.UDPAddr) bool

// SetFaultInjectorFunction assigns a fault injector function.  If the fault
// injector function is non-nil, then it is called with the source and
// destination UDP/IP addresses.  If the function then returns true, a fault is
// injected and the packet is dropped.
func SetFaultInjectorFunction(fn func(from, to *net.UDPAddr) bool) {
	faultInjectorMutex.Lock()
	faultInjectorFunction = fn
	faultInjectorMutex.Unlock()
}

// InjectFault returns true iff a fault should be injected.
func InjectFault(from, to *net.UDPAddr) bool {
	faultInjectorMutex.Lock()
	fn := faultInjectorFunction
	faultInjectorMutex.Unlock()
	if fn != nil && fn(from, to) {
		log.Printf("--- FAULT INJECTED ---")
		return true
	}
	return false
}

const MaxPacketSize = 1200

func SendUDP(sec SecretsManager, secretNum uint32, conn *net.UDPConn, to *net.UDPAddr, packet *pb.Packet) error {
	data, err := proto.Marshal(packet)
	if err != nil {
		return err
	}
	if len(data) > MaxPacketSize {
		return fmt.Errorf("cannot send packet of %d bytes, max is %d", len(data), MaxPacketSize)
	}

	secret := sec.LookupSecret(secretNum)
	if secret.IsZero() {
		return fmt.Errorf("invalid secretNum: %#08x", secretNum)
	}
	n := len(data)
	data = append(data, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(data[n:], secretNum)
	mac := hmac.New(sha256.New, secret.Key)
	mac.Write(data)
	data = mac.Sum(data)

	if InjectFault(conn.LocalAddr().(*net.UDPAddr), to) {
		return nil
	}

	_, err = conn.WriteToUDP(data, to)
	return err
}

func RecvUDP(sec SecretsManager, conn *net.UDPConn) (*pb.Packet, *net.UDPAddr, uint32, error) {
	var buf [MaxPacketSize + sha256.Size + 4]byte
	for {
		size, from, err := conn.ReadFromUDP(buf[:])
		if err != nil {
			return nil, nil, 0, err
		}

		data := buf[:size]
		n := size - sha256.Size - 4
		expectedMAC := data[n+4:]
		secretNum := binary.BigEndian.Uint32(data[n : n+4])
		secret := sec.LookupSecret(secretNum)
		if secret.IsZero() {
			log.Printf("go-raft/rpc: received packet with invalid secretNum %#08x", secretNum)
			continue
		}
		mac := hmac.New(sha256.New, secret.Key)
		mac.Write(data[:n+4])
		actualMAC := mac.Sum(nil)
		if !hmac.Equal(expectedMAC, actualMAC) {
			log.Printf("go-raft/rpc: received packet with bad HMAC")
			continue
		}

		var packet pb.Packet
		err = proto.Unmarshal(data[:n], &packet)
		if err != nil {
			log.Printf("go-raft/rpc: received correctly-signed packet with error: %v", err)
			continue
		}
		if !secret.Access.Check(Category(packet.Type)) {
			log.Printf("go-raft/rpc: access denied: packet type %d (%v) but secretNum %#08x (%v)", packet.Type, Category(packet.Type), secretNum, secret.Access)
			continue
		}

		if InjectFault(from, conn.LocalAddr().(*net.UDPAddr)) {
			continue
		}

		return &packet, from, secretNum, nil
	}
}

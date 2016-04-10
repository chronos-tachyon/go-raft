package rpc

import (
	"fmt"
	"log"
	"strings"
)

type AccessLevel uint8

const (
	NoAccess AccessLevel = iota
	ReadAccess
	WriteAccess
	AdminAccess
	PeerAccess
)

var accessLevelNames = []string{
	"NoAccess",
	"ReadAccess",
	"WriteAccess",
	"AdminAccess",
	"PeerAccess",
}

func (access AccessLevel) String() string {
	if int(access) >= len(accessLevelNames) {
		return fmt.Sprintf("AccessLevel(%d)", access)
	}
	return accessLevelNames[access]
}

func (access AccessLevel) Check(category PacketCategory) bool {
	switch category {
	case ReadCategory:
		return access >= ReadAccess
	case WriteCategory:
		return access >= WriteAccess
	case AdminCategory:
		return access >= AdminAccess
	case PeerCategory:
		return access >= PeerAccess
	default:
		log.Printf("go-raft/raftrpc: not implemented: %[1]T %[1]v", category)
		return false
	}
}

var storedAccessLevels = []string{
	"none",
	"read",
	"write",
	"admin",
	"peer",
}

func parseStoredAccessLevel(str string) AccessLevel {
	switch {
	case strings.EqualFold(str, "admin"):
		return AdminAccess
	case strings.EqualFold(str, "none"):
		return NoAccess
	case strings.EqualFold(str, "peer"):
		return PeerAccess
	case strings.EqualFold(str, "read"):
		return ReadAccess
	case strings.EqualFold(str, "write"):
		return WriteAccess
	}
	log.Printf("go-raft/raftrpc: failed to parse access level: %q", str)
	return NoAccess
}

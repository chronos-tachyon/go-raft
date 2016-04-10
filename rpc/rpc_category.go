package rpc

import (
	"fmt"

	pb "github.com/chronos-tachyon/go-raft/proto"
)

type PacketCategory uint8

const (
	UnknownCategory PacketCategory = iota
	ReadCategory
	WriteCategory
	AdminCategory
	PeerCategory
)

var categoryNames = []string{
	"",
	"ReadCategory",
	"WriteCategory",
	"AdminCategory",
	"PeerCategory",
}

func (category PacketCategory) String() string {
	if category == 0 || int(category) >= len(categoryNames) {
		return fmt.Sprintf("PacketCategory(%d)", category)
	}
	return categoryNames[category]
}

var categoryByPacketType = map[pb.Packet_Type]PacketCategory{
	pb.Packet_VOTE_REQUEST:               PeerCategory,
	pb.Packet_VOTE_RESPONSE:              PeerCategory,
	pb.Packet_APPEND_ENTRIES_REQUEST:     PeerCategory,
	pb.Packet_APPEND_ENTRIES_RESPONSE:    PeerCategory,
	pb.Packet_NOMINATE_REQUEST:           PeerCategory,
	pb.Packet_INFORM_REQUEST:             PeerCategory,
	pb.Packet_DISCOVER_REQUEST:           PeerCategory,
	pb.Packet_DISCOVER_RESPONSE:          PeerCategory,
	pb.Packet_CREATE_SESSION_REQUEST:     ReadCategory,
	pb.Packet_CREATE_SESSION_RESPONSE:    ReadCategory,
	pb.Packet_DESTROY_SESSION_REQUEST:    ReadCategory,
	pb.Packet_DESTROY_SESSION_RESPONSE:   ReadCategory,
	pb.Packet_QUERY_REQUEST:              ReadCategory,
	pb.Packet_QUERY_RESPONSE:             ReadCategory,
	pb.Packet_WRITE_REQUEST:              WriteCategory,
	pb.Packet_WRITE_RESPONSE:             WriteCategory,
	pb.Packet_GET_CONFIGURATION_REQUEST:  ReadCategory,
	pb.Packet_GET_CONFIGURATION_RESPONSE: ReadCategory,
	pb.Packet_SET_CONFIGURATION_REQUEST:  AdminCategory,
	pb.Packet_SET_CONFIGURATION_RESPONSE: AdminCategory,
}

func Category(ptype pb.Packet_Type) PacketCategory {
	category, _ := categoryByPacketType[ptype]
	return category
}

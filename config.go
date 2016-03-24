package raft

import (
	"fmt"
	"net"

	"gopkg.in/yaml.v2"

	"github.com/chronos-tachyon/go-raft/packet"
)

type Config struct {
	Nodes []NodeConfig
}

type NodeConfig struct {
	Id   uint8
	Addr string
	udp  *net.UDPAddr `yaml:"-"`
}

func ParseConfig(in []byte) (*Config, error) {
	var cfg Config
	err := yaml.Unmarshal(in, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (cfg *Config) verify() (map[packet.NodeId]peer, error) {
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("must configure at least one Raft node")
	}
	result := make(map[packet.NodeId]peer, len(cfg.Nodes))
	for _, item := range cfg.Nodes {
		id := packet.NodeId(item.Id)
		if id == 0 {
			return nil, fmt.Errorf("invalid id: 0")
		}
		_, found := result[id]
		if found {
			return nil, fmt.Errorf("duplicate id: %d", id)
		}
		addr, err := net.ResolveUDPAddr("udp", item.Addr)
		if err != nil {
			return nil, err
		}
		result[id] = peer{id, addr}
	}
	return result, nil
}

func (cfg *Config) Save() []byte {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return out
}

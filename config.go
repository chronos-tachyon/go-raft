package raft

import (
	"gopkg.in/yaml.v2"
)

type Config struct {
	Peers []PeerConfig
}

type PeerConfig struct {
	Id   PeerId
	Addr string
}

func ParseConfig(in []byte) (*Config, error) {
	var cfg Config
	err := yaml.Unmarshal(in, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (cfg *Config) Save() []byte {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return out
}

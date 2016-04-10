package rpc

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

type Secret struct {
	Key    []byte
	Access AccessLevel
}

func (s Secret) IsZero() bool {
	return s.Key == nil && s.Access == 0
}

type StoredSecret struct {
	Key    []byte `json:"key"`
	Access string `json:"access"`
}

func (ss *StoredSecret) Set(s Secret) {
	ss.Key = s.Key
	if int(s.Access) >= len(storedAccessLevels) {
		ss.Access = ""
	} else {
		ss.Access = storedAccessLevels[s.Access]
	}
}

func (ss *StoredSecret) Get() Secret {
	return Secret{
		Key:    ss.Key,
		Access: parseStoredAccessLevel(ss.Access),
	}
}

type SecretsManager interface {
	LookupSecret(secretNum uint32) Secret
}

type SimpleSecretsManager map[uint32]Secret

func (secrets SimpleSecretsManager) LookupSecret(secretNum uint32) Secret {
	result, _ := secrets[secretNum]
	return result
}

type FileSecretsManager string

func (path FileSecretsManager) LookupSecret(secretNum uint32) Secret {
	f, err := os.Open(string(path))
	if err != nil {
		log.Printf("go-raft/raftrpc: %v", err)
		return Secret{}
	}
	defer f.Close()

	raw, err := ioutil.ReadAll(f)
	if err != nil {
		log.Printf("go-raft/raftrpc: %v", err)
		return Secret{}
	}

	out := make(map[uint32]StoredSecret)
	err = json.Unmarshal(raw, out)
	if err != nil {
		log.Printf("go-raft/raftrpc: %s: %v", string(path), err)
		return Secret{}
	}

	ss, found := out[secretNum]
	if !found {
		return Secret{}
	}

	return ss.Get()
}

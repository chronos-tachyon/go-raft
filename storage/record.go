package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"

	"github.com/chronos-tachyon/go-raft/internal/util"
)

var errFailedChecksum = errors.New("record failed checksum")

type CorruptionError struct {
	Err error
}

func (err CorruptionError) Error() string {
	return fmt.Sprintf("go-raft/storage: corruption error: %s", err.Err.Error())
}

func ReadRecord(f File, msg proto.Message) error {
	var buf [4]byte
	_, err := io.ReadFull(f, buf[:])
	if err != nil {
		return err
	}
	n := int(binary.BigEndian.Uint32(buf[:]))
	data := make([]byte, n+8)
	copy(data[:4], buf[:])
	_, err = io.ReadFull(f, data[4:])
	if err != nil {
		return err
	}
	n += 4
	if binary.BigEndian.Uint32(data[n:]) != util.CRC(data[:n]) {
		return CorruptionError{Err: errFailedChecksum}
	}
	err = proto.Unmarshal(data[4:n], msg)
	if err != nil {
		return CorruptionError{Err: err}
	}
	return nil
}

func WriteRecord(f File, msg proto.Message) (int, error) {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return 0, err
	}
	n := len(raw)
	data := make([]byte, n+8)
	binary.BigEndian.PutUint32(data[:4], uint32(n))
	n += 4
	copy(data[4:n], raw)
	binary.BigEndian.PutUint32(data[n:], util.CRC(data[:n]))
	return f.Write(data)
}

func readRecordFromFile(storage Storage, name string, msg proto.Message) error {
	f, err := storage.Open(name, ReadOnly)
	if err != nil {
		return err
	}
	err = ReadRecord(f, msg)
	if err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func writeRecordToFile(storage Storage, name string, msg proto.Message) error {
	f, err := storage.Open(name, WriteOnly)
	if err != nil {
		return err
	}
	_, err = WriteRecord(f, msg)
	if err != nil {
		f.Close()
		storage.Delete(name)
		return err
	}
	err = f.Sync()
	if err != nil {
		f.Close()
		storage.Delete(name)
		return err
	}
	err = f.Close()
	if err != nil {
		storage.Delete(name)
		return err
	}
	return nil
}

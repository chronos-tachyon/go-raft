package storage

import (
	"bytes"
	"errors"
	"path"
	"sync"
)

var (
	errNotFound  = errors.New("not found")
	errExist     = errors.New("exists")
	errClosed    = errors.New("closed")
	errReadOnly  = errors.New("read-only file")
	errWriteOnly = errors.New("write-only file")
)

type MemStorage struct {
	sync.Mutex
	Files map[string][]byte
}

func NewMemStorage() *MemStorage {
	return &MemStorage{Files: make(map[string][]byte)}
}

func (ms *MemStorage) List() ([]string, error) {
	ms.Lock()
	defer ms.Unlock()

	var names []string
	for name := range ms.Files {
		names = append(names, name)
	}
	return names, nil
}

func (ms *MemStorage) Open(name string, mode Mode) (File, error) {
	ms.Lock()
	defer ms.Unlock()

	name = path.Clean(name)
	switch mode {
	case ReadOnly:
		data, found := ms.Files[name]
		if !found {
			return nil, errNotFound
		}
		return &memReadFile{r: bytes.NewReader(data)}, nil

	case WriteOnly:
		_, found := ms.Files[name]
		if found {
			return nil, errExist
		}
		ms.Files[name] = nil
		return &memWriteFile{ms: ms, name: name}, nil
	}
	panic("unknown open mode")
}

func (ms *MemStorage) Rename(oldname, newname string) error {
	ms.Lock()
	defer ms.Unlock()

	oldname = path.Clean(oldname)
	newname = path.Clean(newname)
	data, found := ms.Files[oldname]
	if !found {
		return errNotFound
	}
	_, found2 := ms.Files[newname]
	if found2 {
		return errExist
	}
	ms.Files[newname] = data
	delete(ms.Files, oldname)
	return nil
}

func (ms *MemStorage) Delete(name string) error {
	ms.Lock()
	defer ms.Unlock()

	name = path.Clean(name)
	_, found := ms.Files[name]
	if !found {
		return errNotFound
	}
	delete(ms.Files, name)
	return nil
}

func (_ *MemStorage) IsNotExist(err error) bool {
	return err == errNotFound
}

func (_ *MemStorage) IsExist(err error) bool {
	return err == errExist
}

var _ Storage = &MemStorage{}

type memReadFile struct {
	sync.Mutex
	r *bytes.Reader
}

func (mrf *memReadFile) Read(b []byte) (int, error) {
	mrf.Lock()
	defer mrf.Unlock()

	if mrf.r == nil {
		return 0, errClosed
	}
	return mrf.r.Read(b)
}

func (mrf *memReadFile) Write(p []byte) (int, error) {
	mrf.Lock()
	defer mrf.Unlock()

	if mrf.r == nil {
		return 0, errClosed
	}
	return 0, errReadOnly
}

func (mrf *memReadFile) Sync() error {
	mrf.Lock()
	defer mrf.Unlock()

	if mrf.r == nil {
		return errClosed
	}
	return nil
}

func (mrf *memReadFile) Close() error {
	mrf.Lock()
	defer mrf.Unlock()

	if mrf.r == nil {
		return errClosed
	}
	mrf.r = nil
	return nil
}

type memWriteFile struct {
	sync.Mutex
	ms   *MemStorage
	name string
	buf  bytes.Buffer
}

func (mwf *memWriteFile) Read(b []byte) (int, error) {
	mwf.Lock()
	defer mwf.Unlock()

	if mwf.ms == nil {
		return 0, errClosed
	}
	return 0, errWriteOnly
}

func (mwf *memWriteFile) Write(p []byte) (int, error) {
	mwf.Lock()
	defer mwf.Unlock()

	if mwf.ms == nil {
		return 0, errClosed
	}
	return mwf.buf.Write(p)
}

func (mwf *memWriteFile) Sync() error {
	mwf.Lock()
	defer mwf.Unlock()
	return mwf.syncLocked()
}

func (mwf *memWriteFile) syncLocked() error {
	if mwf.ms == nil {
		return errClosed
	}
	mwf.ms.Lock()
	defer mwf.ms.Unlock()

	mwf.ms.Files[mwf.name] = mwf.buf.Bytes()
	return nil
}

func (mwf *memWriteFile) Close() error {
	mwf.Lock()
	defer mwf.Unlock()
	if err := mwf.syncLocked(); err != nil {
		return err
	}
	mwf.ms = nil
	return nil
}

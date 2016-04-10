package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type DiskStorage struct {
	Root string
}

func (ds *DiskStorage) List() ([]string, error) {
	var names []string
	err := filepath.Walk(ds.Root, func(path string, _ os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		name, err := filepath.Rel(ds.Root, path)
		if err != nil {
			return err
		}
		names = append(names, name)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return names, nil
}

func (ds *DiskStorage) Open(name string, mode Mode) (File, error) {
	path := filepath.Join(ds.Root, name)
	var flags, how int
	switch mode {
	case ReadOnly:
		flags = os.O_RDONLY
		how = syscall.LOCK_SH
	case WriteOnly:
		flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
		how = syscall.LOCK_EX
	default:
		panic("unknown open mode")
	}
	f, err := os.OpenFile(path, flags, 0666)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), how); err != nil {
		panic(fmt.Errorf("go-raft/storage: %s: flock failed: %v", path, err))
	}
	return f, nil
}

func (ds *DiskStorage) Rename(oldname, newname string) error {
	oldpath := filepath.Join(ds.Root, oldname)
	newpath := filepath.Join(ds.Root, newname)
	return os.Rename(oldpath, newpath)
}

func (ds *DiskStorage) Delete(name string) error {
	path := filepath.Join(ds.Root, name)
	return os.Remove(path)
}

func (ds *DiskStorage) IsNotExist(err error) bool {
	return false // FIXME
}

func (ds *DiskStorage) IsExist(err error) bool {
	return false // FIXME
}

var _ Storage = &DiskStorage{}

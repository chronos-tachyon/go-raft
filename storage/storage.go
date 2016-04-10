package storage

import (
	"io"
)

type File interface {
	io.Reader
	io.Writer
	io.Closer
	Sync() error
}

type Mode uint8

const (
	// Equivalent to os.O_RDONLY
	ReadOnly Mode = iota

	// Equivalent to os.O_WRONLY|os.O_CREATE|os.O_EXCL
	WriteOnly
)

type Storage interface {
	// List returns the names of all files in the given Storage area.
	List() ([]string, error)

	// Open opens the named file in the given mode.
	Open(name string, mode Mode) (File, error)

	// Rename renames the named file from oldname to newname.  If no file
	// named oldname exists, then the error returned matches the IsNotExist
	// predicate.  If a file named newname already exists, then the error
	// returned matches the IsExist predicate.
	Rename(oldname, newname string) error

	// Delete removes the named file.  If the file does not exist, then the
	// error returned matches the IsNotExist predicate.
	Delete(name string) error

	// IsNotExist takes an error returned by Open, Retrieve, or Delete, and
	// returns true iff the error was caused by the file not existing.
	IsNotExist(err error) bool

	// IsExist takes an error returned by Open or Rename, and returns true
	// iff the error was caused by the file already existing.
	IsExist(err error) bool
}

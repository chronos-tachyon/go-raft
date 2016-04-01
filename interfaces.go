package raft

type StateMachine interface {
	// Commit advances the state machine by one command.
	Commit(command []byte)

	// Snapshot marshals the machine's state into a stable binary format.
	Snapshot() (snapshot []byte)

	// Restore takes a blob of data, the contents of which must have been
	// returned by Snapshot, and replaces the machine's state with the
	// state that existed when Snapshot was called.
	Restore(snapshot []byte)

	// String returns a human-friendly string representation of the state
	// machine.
	String() string
}

type Storage interface {
	// Store writes the given contents to the named file.  The file will be
	// overwritten if it exists, or created if it does not.
	Store(filename string, data []byte) error

	// Retrieve reads the contents of the named file.  If the file does not
	// exist, then the error returned matches the IsNotFound predicate.
	Retrieve(filename string) ([]byte, error)

	// Delete removes the named file.  If the file does not exist, then the
	// error returned matches the IsNotFound predicate.
	Delete(filename string) error

	// IsNotFound takes an error returned by Retrieve or Delete, and
	// returns true iff the error was caused by the file not existing.
	IsNotFound(err error) bool
}

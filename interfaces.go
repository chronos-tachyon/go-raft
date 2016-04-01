package raft

type StateMachine interface {
	// Snapshot marshals the state machine into a stable binary format.
	Snapshot() (snapshot []byte)

	// Restore unmarshals its argument and replaces the state machine with it.
	Restore(snapshot []byte)

	// Commit advances the state machine by one command.
	Commit(command []byte)

	String() string
}

type Storage interface {
	Store(filename string, data []byte) error
	Retrieve(filename string) ([]byte, error)
	Delete(filename string) error
	IsNotFound(err error) bool
}

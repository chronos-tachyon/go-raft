package storage

type StateMachine interface {
	// ValidateQuery verifies that the given Query command is legal.
	ValidateQuery(command []byte) error

	// Query asks the state machine for a piece of state.
	Query(command []byte) []byte

	// ValidateApply verifies that the given Apply command is legal.
	ValidateApply(command []byte) error

	// Apply advances the state machine by one command.
	Apply(command []byte)

	// Snapshot marshals the machine's state into a stable binary format.
	Snapshot() (snapshot []byte)

	// Restore takes a blob of data representing a machine state, and
	// replaces the machine's state with the state thus represented.  The
	// blob is either zero-length (meaning: return to the initial state) or
	// else a value returned from Snapshot.
	Restore(snapshot []byte)

	// String returns a human-friendly string representation of the state
	// machine.
	String() string
}

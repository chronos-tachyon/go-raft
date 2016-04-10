package proto

var peerStateChars = []byte{'b', 'l', 'J', 'S', 'L'}

func (x Peer_State) Char() byte {
	if int(x) >= len(peerStateChars) {
		return '?'
	}
	return peerStateChars[x]
}

var logEntryTypeChars = []byte{'n', 'c', 'x'}

func (x LogEntry_Type) Char() byte {
	if int(x) >= len(logEntryTypeChars) {
		return '?'
	}
	return logEntryTypeChars[x]
}

var logEntryTypeUpperChars = []byte{'N', 'C', 'X'}

func (x LogEntry_Type) UpperChar() byte {
	if int(x) >= len(logEntryTypeUpperChars) {
		return '!'
	}
	return logEntryTypeUpperChars[x]
}

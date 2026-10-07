package microvm

import "fmt"

func errorf(format string, args ...any) error { return fmt.Errorf(format, args...) }

// legalTransitions is the authoritative lifecycle state machine. A transition
// must appear in this table to be permitted; transient states intentionally
// serialize concurrent operations on the same VM.
var legalTransitions = map[VMState]map[OperationType]VMState{
	StatePending: {
		OpDefine:  StateStopped, // Define completes placement and reservation.
		OpDestroy: StateDestroying,
	},
	StateStopped: {
		OpStart:    StateStarting,
		OpDestroy:  StateDestroying,
		OpSnapshot: StateStopped, // snapshot keeps the VM stopped
		OpRollback: StateStopped,
		OpDeleteSS: StateStopped,
		OpWrite:    StateStopped,
	},
	StateStarting: {
		OpStart: StateRunning, // start workflow completes
	},
	StateRunning: {
		OpStop:     StateStopping,
		OpRestart:  StateRestarting,
		OpDestroy:  StateDestroying,
		OpSnapshot: StateRunning,
		OpRollback: StateRunning,
		OpDeleteSS: StateRunning,
		OpWrite:    StateRunning,
	},
	StateStopping: {
		OpStop: StateStopped,
	},
	StateRestarting: {
		OpRestart: StateRunning,
	},
	StateDestroying: {
		OpDestroy: StateDestroyed,
	},
	StateDestroyed: {},
}

// transition returns the next state for op from from, or an
// ErrInvalidTransition error explaining precisely what was rejected.
func transition(from VMState, op OperationType) (VMState, error) {
	if next, ok := legalTransitions[from][op]; ok {
		return next, nil
	}
	return from, errorf("%w: operation %s is not allowed in state %s", ErrInvalidTransition, op, from)
}

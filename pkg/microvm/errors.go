package microvm

import "errors"

// Distinguishable, sentinel error reasons. Callers can inspect them with
// errors.Is regardless of the operation that produced the error.
var (
	ErrVMNotFound        = errors.New("microvm: virtual machine not found")
	ErrAlreadyExists     = errors.New("microvm: virtual machine already exists")
	ErrInvalidArgument   = errors.New("microvm: invalid argument")
	ErrInvalidTransition = errors.New("microvm: illegal lifecycle transition")
	ErrDuplicateRequest  = errors.New("microvm: conflicting operation already in progress")
	ErrCapacityExceeded  = errors.New("microvm: host pool capacity exceeded")
	ErrSnapshotNotFound  = errors.New("microvm: snapshot not found")
	ErrOperationFailed   = errors.New("microvm: async operation failed")
	ErrBackendFailed     = errors.New("microvm: simulated backend failure")
	ErrClosed            = errors.New("microvm: control plane is closed")
)

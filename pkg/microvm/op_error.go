package microvm

// operationError keeps the distinguishable sentinel reason available through
// errors.Is even when wrapped inside a failed async operation.
type operationError struct {
	opType OperationType
	reason string
	err    error
}

func (e *operationError) Error() string {
	return "microvm: async operation failed: " + string(e.opType) + ": " + e.err.Error()
}

func (e *operationError) Unwrap() error { return e.err }

type plainError struct{ msg string }

func (e *plainError) Error() string { return e.msg }

func (e *plainError) Is(target error) bool {
	for _, s := range []error{
		ErrVMNotFound, ErrAlreadyExists, ErrInvalidArgument, ErrInvalidTransition,
		ErrDuplicateRequest, ErrCapacityExceeded, ErrSnapshotNotFound,
		ErrBackendFailed, ErrClosed,
	} {
		if s.Error() == e.msg || containsReason(e.msg, s.Error()) {
			if target == s {
				return true
			}
		}
	}
	return false
}

func containsReason(msg, reason string) bool {
	return len(msg) >= len(reason) && (indexOf(msg, reason) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

package microvm

import (
	"sync"
	"time"
)

// Backend is the simulated virtualization backend. It intentionally has no
// dependency on KVM/QEMU/libvirt: it only models latency, runtime power state
// and injectable failures. Durable VM state lives in the Store, so a backend
// "process" can be reconstructed after a control-plane restart.
type Backend struct {
	mu sync.Mutex

	delay time.Duration

	// failOnce, when non-nil, is consulted before each workflow step; if it
	// returns true the step fails and the hook is consumed.
	failHook func(op OperationType) bool

	running map[string]bool
}

// NewBackend creates a simulated backend. delay models the cost of a real
// hypervisor call so asynchronous workflows are observable in tests.
func NewBackend(delay time.Duration) *Backend {
	return &Backend{delay: delay, running: map[string]bool{}}
}

// SetFaultInjector installs a hook evaluated before every simulated backend
// action. Returning true makes exactly that action fail with ErrBackendFailed.
func (b *Backend) SetFaultInjector(hook func(OperationType) bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failHook = hook
}

func (b *Backend) step(op OperationType) error {
	time.Sleep(b.delay)
	b.mu.Lock()
	hook := b.failHook
	b.mu.Unlock()
	if hook != nil && hook(op) {
		return errorf("%w: injected failure during %s", ErrBackendFailed, op)
	}
	return nil
}

func (b *Backend) markRunning(vmID string, running bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running[vmID] = running
}

// IsRunning reports the simulated power state. Re-derived by the control
// plane on restart, it is only a runtime view.
func (b *Backend) IsRunning(vmID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.running[vmID]
}

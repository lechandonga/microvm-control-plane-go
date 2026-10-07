package microvm

import "time"

// VMState is the persisted lifecycle state of a micro-VM.
type VMState string

const (
	// StatePending: definition recorded, not yet placed on a host.
	StatePending VMState = "Pending"
	// StateStopped: placed, resources reserved, not running.
	StateStopped VMState = "Stopped"
	// StateStarting: transient state while a start operation is executing.
	StateStarting VMState = "Starting"
	// StateRunning: consuming CPU/memory as well as reserved disk.
	StateRunning VMState = "Running"
	// StateStopping: transient state while a stop operation is executing.
	StateStopping VMState = "Stopping"
	// StateRestarting: transient state while a restart operation is executing.
	StateRestarting VMState = "Restarting"
	// StateDestroying: transient state while a destroy operation is executing.
	StateDestroying VMState = "Destroying"
	// StateDestroyed: terminal state; no resources are held.
	StateDestroyed VMState = "Destroyed"
)

// IsTerminal reports whether the state can never change again.
func (s VMState) IsTerminal() bool { return s == StateDestroyed }

// Spec is the user-supplied definition of a micro-VM.
type Spec struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	VCPU   int    `json:"vcpu"`
	Memory int64  `json:"memory_bytes"`
	Disk   int64  `json:"disk_bytes"`
}

func (s Spec) validate() error {
	if s.ID == "" {
		return errorf("%w: id must not be empty", ErrInvalidArgument)
	}
	if s.VCPU <= 0 {
		return errorf("%w: vcpu must be positive, got %d", ErrInvalidArgument, s.VCPU)
	}
	if s.Memory <= 0 {
		return errorf("%w: memory_bytes must be positive, got %d", ErrInvalidArgument, s.Memory)
	}
	if s.Disk <= 0 {
		return errorf("%w: disk_bytes must be positive, got %d", ErrInvalidArgument, s.Disk)
	}
	return nil
}

// VM is the externally visible view of a micro-VM.
type VM struct {
	Spec      Spec       `json:"spec"`
	State     VMState    `json:"state"`
	HostID    string     `json:"host_id,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	Snapshots []Snapshot `json:"snapshots,omitempty"`
}

// Snapshot is a point-in-time, copy-on-write disk snapshot.
type Snapshot struct {
	ID         string    `json:"id"`
	VMID       string    `json:"vm_id"`
	TakenAt    time.Time `json:"taken_at"`
	SizeBytes  int64     `json:"size_bytes"`
	BaseDiskID string    `json:"base_disk_id"`
}

// HostConfig describes one simulated host in the resource pool.
type HostConfig struct {
	ID           string `json:"id"`
	CPUCapacity  int    `json:"cpu"`
	MemCapacity  int64  `json:"memory_bytes"`
	DiskCapacity int64  `json:"disk_bytes"`
}

func (h HostConfig) validate() error {
	if h.ID == "" || h.CPUCapacity <= 0 || h.MemCapacity <= 0 || h.DiskCapacity <= 0 {
		return errorf("%w: host %q fields must all be positive", ErrInvalidArgument, h.ID)
	}
	return nil
}

// OvercommitRatio configures how much CPU and memory may be overcommitted.
// Disk is never overcommitted (factor fixed at 1).
type OvercommitRatio struct {
	CPU float64 `json:"cpu"`
	Mem float64 `json:"memory"`
}

// HostUsage is the explainable capacity ledger of one host.
type HostUsage struct {
	ID            string  `json:"id"`
	CPUCapacity   int     `json:"cpu_capacity"`
	MemCapacity   int64   `json:"memory_capacity_bytes"`
	DiskCapacity  int64   `json:"disk_capacity_bytes"`
	CPUUsed       int     `json:"cpu_used"`
	MemUsed       int64   `json:"memory_used_bytes"`
	DiskUsed      int64   `json:"disk_used_bytes"`
	CPUOvercommit float64 `json:"cpu_overcommit"`
	MemOvercommit float64 `json:"memory_overcommit"`
}

// CPUFree is the schedulable CPU accounting for overcommit.
func (u HostUsage) CPUFree() int {
	return int(float64(u.CPUCapacity)*u.CPUOvercommit+0.5) - u.CPUUsed
}

// MemFree is the schedulable memory accounting for overcommit.
func (u HostUsage) MemFree() int64 {
	return int64(float64(u.MemCapacity)*u.MemOvercommit+0.5) - u.MemUsed
}

// DiskFree never exceeds physical disk: disk is not overcommitted.
func (u HostUsage) DiskFree() int64 { return u.DiskCapacity - u.DiskUsed }

// OperationType enumerates the asynchronous workflows.
type OperationType string

const (
	OpDefine   OperationType = "Define"
	OpStart    OperationType = "Start"
	OpStop     OperationType = "Stop"
	OpRestart  OperationType = "Restart"
	OpDestroy  OperationType = "Destroy"
	OpSnapshot OperationType = "Snapshot"
	OpRollback OperationType = "Rollback"
	OpDeleteSS OperationType = "DeleteSnapshot"
	OpWrite    OperationType = "WriteBlock"
)

// OperationState is the durable state of an asynchronous operation.
type OperationState string

const (
	OpPending   OperationState = "Pending"
	OpRunning   OperationState = "Running"
	OpSucceeded OperationState = "Succeeded"
	OpFailed    OperationState = "Failed"
)

// Operation is the externally visible, persisted status of a request.
type Operation struct {
	ID        string         `json:"id"`
	Type      OperationType  `json:"type"`
	VMID      string         `json:"vm_id"`
	State     OperationState `json:"state"`
	Error     string         `json:"error,omitempty"`
	Reason    string         `json:"reason,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

// Done reports whether the operation reached a terminal conclusion.
func (o Operation) Done() bool { return o.State == OpSucceeded || o.State == OpFailed }

// Succeeded reports durable success.
func (o Operation) Succeeded() bool { return o.State == OpSucceeded }

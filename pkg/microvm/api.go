package microvm

import "context"

// This file provides ergonomic typed wrappers over Submit/Wait.

// Define creates and places a micro-VM asynchronously.
func (cp *ControlPlane) Define(spec Spec, idemKey string) (string, error) {
	return cp.Submit(Request{Type: OpDefine, Spec: spec, IdempotencyKey: idemKey})
}

// Start powers a stopped VM on asynchronously.
func (cp *ControlPlane) Start(vmID, idemKey string) (string, error) {
	return cp.Submit(Request{Type: OpStart, VMID: vmID, IdempotencyKey: idemKey})
}

// Stop powers a running VM off asynchronously; CPU/memory are released.
func (cp *ControlPlane) Stop(vmID, idemKey string) (string, error) {
	return cp.Submit(Request{Type: OpStop, VMID: vmID, IdempotencyKey: idemKey})
}

// Restart power-cycles a running VM asynchronously.
func (cp *ControlPlane) Restart(vmID, idemKey string) (string, error) {
	return cp.Submit(Request{Type: OpRestart, VMID: vmID, IdempotencyKey: idemKey})
}

// Destroy tears a VM down and releases every byte it held.
func (cp *ControlPlane) Destroy(vmID, idemKey string) (string, error) {
	return cp.Submit(Request{Type: OpDestroy, VMID: vmID, IdempotencyKey: idemKey})
}

// TakeSnapshot captures a point-in-time COW snapshot; snapshotID may be empty
// for a generated ID.
func (cp *ControlPlane) TakeSnapshot(vmID, snapshotID, idemKey string) (string, error) {
	return cp.Submit(Request{Type: OpSnapshot, VMID: vmID, SnapshotID: snapshotID, IdempotencyKey: idemKey})
}

// Rollback restores a VM's disk to the exact contents of snapshotID.
func (cp *ControlPlane) Rollback(vmID, snapshotID, idemKey string) (string, error) {
	return cp.Submit(Request{Type: OpRollback, VMID: vmID, SnapshotID: snapshotID, IdempotencyKey: idemKey})
}

// DeleteSnapshot frees the bytes uniquely held by a snapshot.
func (cp *ControlPlane) DeleteSnapshot(vmID, snapshotID, idemKey string) (string, error) {
	return cp.Submit(Request{Type: OpDeleteSS, VMID: vmID, SnapshotID: snapshotID, IdempotencyKey: idemKey})
}

// WriteBlock writes a content-addressed disk block (simulated guest I/O).
func (cp *ControlPlane) WriteBlock(vmID string, index int, data []byte, idemKey string) (string, error) {
	return cp.Submit(Request{Type: OpWrite, VMID: vmID, BlockIndex: index, Data: data, IdempotencyKey: idemKey})
}

// GetVM returns the current external view of a VM.
func (cp *ControlPlane) GetVM(vmID string) (VM, error) {
	vm, ok := cp.store.getVM(vmID)
	if !ok {
		return VM{}, errorf("%w: %q", ErrVMNotFound, vmID)
	}
	return vm, nil
}

// ListVMs returns all non-destroyed VMs in creation order.
func (cp *ControlPlane) ListVMs() []VM {
	all, _, _ := cp.store.snapshotView()
	out := make([]VM, 0, len(all))
	for _, vm := range all {
		out = append(out, vm)
	}
	sortVMs(out)
	return out
}

// GetOperation returns durable status of an operation.
func (cp *ControlPlane) GetOperation(opID string) (Operation, error) {
	op, ok := cp.store.getOperation(opID)
	if !ok {
		return Operation{}, errorf("%w: operation %q", ErrOperationFailed, opID)
	}
	return op, nil
}

// HostUsages returns the recomputed per-host capacity ledger.
func (cp *ControlPlane) HostUsages() []HostUsage { return cp.store.HostUsages() }

// WaitVM is a convenience that waits for an operation id.
func (cp *ControlPlane) WaitVM(ctx context.Context, opID string) (Operation, error) {
	return cp.Wait(ctx, opID)
}

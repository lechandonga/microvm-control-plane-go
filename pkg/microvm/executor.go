package microvm

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"time"
)

// execute runs one operation to a durable conclusion. It is the *only* place
// that mutates VM/snapshot state and it is invoked while holding the per-VM
// serialization slot, so a VM never observes interleaved workflows. Every
// step is idempotent: replay after a crash re-derives the same result because
// durable events, not transient memory, drive the conclusions.
func (cp *ControlPlane) execute(op *opRec) error {
	if err := cp.markRunning(op); err != nil {
		return err
	}
	var execErr error
	switch op.Type {
	case OpDefine:
		execErr = cp.execDefine(op)
	case OpStart:
		execErr = cp.execStart(op)
	case OpStop:
		execErr = cp.execStop(op)
	case OpRestart:
		execErr = cp.execRestart(op)
	case OpDestroy:
		execErr = cp.execDestroy(op)
	case OpSnapshot:
		execErr = cp.execSnapshot(op)
	case OpRollback:
		execErr = cp.execRollback(op)
	case OpDeleteSS:
		execErr = cp.execDeleteSnapshot(op)
	case OpWrite:
		execErr = cp.execWrite(op)
	default:
		execErr = errorf("%w: unknown operation type %s", ErrInvalidArgument, op.Type)
	}
	if execErr != nil {
		return execErr
	}
	return nil
}

func (cp *ControlPlane) markRunning(op *opRec) error {
	return cp.finishOp(op.ID, OpRunning, nil)
}

func (cp *ControlPlane) setVMState(vmID string, state VMState) error {
	return cp.store.Apply(Event{Kind: evVMState, VMID: vmID, State: state})
}

// execDefine creates the definition, places it, reserves disk and lands in
// Stopped. Placement and disk reservation are one durable event, so a crash
// can never produce a placed-without-reserved or reserved-without-place VM.
func (cp *ControlPlane) execDefine(op *opRec) error {
	if err := cp.backend.step(OpDefine); err != nil {
		return err
	}
	if err := cp.store.Apply(Event{Kind: evVMDefined, Spec: &op.Spec}); err != nil {
		return err
	}
	hostID, err := cp.store.PlaceAtomically(op.Spec.ID)
	if err != nil {
		return err
	}
	if err := cp.backend.step(OpDefine); err != nil {
		return err
	}
	_ = hostID
	return nil
}

func (cp *ControlPlane) execStart(op *opRec) error {
	// Replay idempotence: an already-running VM completes immediately.
	cp.store.mu.Lock()
	rec := cp.store.state.VMs[op.VMID]
	cp.store.mu.Unlock()
	if rec == nil {
		return errorf("%w: %q", ErrVMNotFound, op.VMID)
	}
	if rec.State == StateRunning {
		return nil
	}
	if _, err := transition(rec.State, OpStart); err != nil {
		return err
	}
	if err := cp.admitStart(rec); err != nil {
		return err
	}
	if err := cp.setVMState(op.VMID, StateStarting); err != nil {
		return err
	}
	if err := cp.backend.step(OpStart); err != nil {
		return err
	}
	cp.backend.markRunning(op.VMID, true)
	if err := cp.backend.step(OpStart); err != nil {
		return err
	}
	return cp.setVMState(op.VMID, StateRunning)
}

// admitStart checks CPU/memory headroom on the placement host against the
// recomputed ledger. The stopped VM itself is not yet counted.
func (cp *ControlPlane) admitStart(rec *vmRec) error {
	cp.store.mu.Lock()
	defer cp.store.mu.Unlock()
	usages := cp.store.ledgerLocked()
	u, ok := usages[rec.HostID]
	if !ok {
		return errorf("%w: host %q vanished", ErrCapacityExceeded, rec.HostID)
	}
	if u.CPUFree() < rec.Spec.VCPU || u.MemFree() < rec.Spec.Memory {
		return errorf("%w: host %q cannot start vm %q (cpu free %d/%d mem free %d/%d)",
			ErrCapacityExceeded, rec.HostID, rec.Spec.ID,
			u.CPUFree(), rec.Spec.VCPU, u.MemFree(), rec.Spec.Memory)
	}
	return nil
}

func (cp *ControlPlane) execStop(op *opRec) error {
	cp.store.mu.Lock()
	rec := cp.store.state.VMs[op.VMID]
	cp.store.mu.Unlock()
	if rec == nil {
		return errorf("%w: %q", ErrVMNotFound, op.VMID)
	}
	if rec.State == StateStopped {
		return nil
	}
	if _, err := transition(rec.State, OpStop); err != nil {
		return err
	}
	if err := cp.setVMState(op.VMID, StateStopping); err != nil {
		return err
	}
	if err := cp.backend.step(OpStop); err != nil {
		return err
	}
	cp.backend.markRunning(op.VMID, false)
	return cp.setVMState(op.VMID, StateStopped)
}

func (cp *ControlPlane) execRestart(op *opRec) error {
	cp.store.mu.Lock()
	rec := cp.store.state.VMs[op.VMID]
	cp.store.mu.Unlock()
	if rec == nil {
		return errorf("%w: %q", ErrVMNotFound, op.VMID)
	}
	// Mid-crash recovery may resume from the transient Restarting state.
	if rec.State != StateRestarting {
		if _, err := transition(rec.State, OpRestart); err != nil {
			return err
		}
		if err := cp.setVMState(op.VMID, StateRestarting); err != nil {
			return err
		}
	}
	if err := cp.backend.step(OpRestart); err != nil {
		return err
	}
	cp.backend.markRunning(op.VMID, false)
	if err := cp.backend.step(OpRestart); err != nil {
		return err
	}
	cp.backend.markRunning(op.VMID, true)
	return cp.setVMState(op.VMID, StateRunning)
}

func (cp *ControlPlane) execDestroy(op *opRec) error {
	cp.store.mu.Lock()
	rec := cp.store.state.VMs[op.VMID]
	cp.store.mu.Unlock()
	if rec == nil {
		// Already destroyed: idempotent completion.
		return nil
	}
	if rec.State != StateDestroying {
		if _, err := transition(rec.State, OpDestroy); err != nil {
			return err
		}
		if err := cp.setVMState(op.VMID, StateDestroying); err != nil {
			return err
		}
	}
	if err := cp.backend.step(OpDestroy); err != nil {
		return err
	}
	cp.backend.markRunning(op.VMID, false)
	if err := cp.backend.step(OpDestroy); err != nil {
		return err
	}
	// Remove snapshots and the VM in one durable event; the recomputed ledger
	// immediately returns all CPU/memory/disk/snapshot bytes to the pool.
	if err := cp.store.Apply(Event{Kind: evVMDestroyed, VMID: op.VMID}); err != nil {
		return err
	}
	return nil
}

func (cp *ControlPlane) execSnapshot(op *opRec) error {
	cp.store.mu.Lock()
	rec := cp.store.state.VMs[op.VMID]
	snaps := cp.store.state.Snaps[op.VMID]
	cp.store.mu.Unlock()
	if rec == nil {
		return errorf("%w: %q", ErrVMNotFound, op.VMID)
	}
	if _, ok := snaps[op.SnapshotID]; ok {
		return nil // retried snapshot is a no-op
	}
	if !isStable(rec.State) {
		return errorf("%w: snapshot is not allowed in state %s", ErrInvalidTransition, rec.State)
	}
	if err := cp.backend.step(OpSnapshot); err != nil {
		return err
	}
	blocks, size := cp.captureDisk(op.VMID)
	snap := Snapshot{
		ID:         op.SnapshotID,
		VMID:       op.VMID,
		TakenAt:    time.Now().UTC(),
		SizeBytes:  size,
		BaseDiskID: cp.diskFingerprint(op.VMID),
	}
	return cp.store.Apply(Event{Kind: evSnapCreated, VMID: op.VMID, Snapshot: &snap, SnapBlocks: blocks})
}

// captureDisk returns the dense, point-in-time block-content vector and the
// number of bytes that this snapshot uniquely keeps alive (COW delta).
func (cp *ControlPlane) captureDisk(vmID string) ([]string, int64) {
	cp.store.mu.Lock()
	defer cp.store.mu.Unlock()
	disk := cp.store.state.Blocks[vmID]
	maxIdx := -1
	for idx := range disk {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	blocks := make([]string, maxIdx+1)
	live := map[string]bool{}
	for idx, blobID := range disk {
		blocks[idx] = blobID
		live[blobID] = true
	}
	// Unique bytes: blobs referenced by any snapshot of this VM but no longer
	// part of the live disk.
	kept := map[string]bool{}
	for _, sr := range cp.store.state.Snaps[vmID] {
		for _, blobID := range sr.Blocks {
			if blobID != "" && !live[blobID] {
				kept[blobID] = true
			}
		}
	}
	var size int64
	for blobID := range kept {
		size += int64(len(cp.store.state.Blobs[blobID]))
	}
	return blocks, size
}

func (cp *ControlPlane) diskFingerprint(vmID string) string {
	cp.store.mu.Lock()
	defer cp.store.mu.Unlock()
	disk := cp.store.state.Blocks[vmID]
	idxs := make([]int, 0, len(disk))
	for idx := range disk {
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	h := sha256.New()
	for _, idx := range idxs {
		h.Write([]byte(disk[idx]))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func (cp *ControlPlane) execRollback(op *opRec) error {
	cp.store.mu.Lock()
	rec := cp.store.state.VMs[op.VMID]
	sr := cp.store.state.Snaps[op.VMID][op.SnapshotID]
	cp.store.mu.Unlock()
	if rec == nil {
		return errorf("%w: %q", ErrVMNotFound, op.VMID)
	}
	if sr == nil {
		return errorf("%w: snapshot %q on vm %q", ErrSnapshotNotFound, op.SnapshotID, op.VMID)
	}
	if !isStable(rec.State) {
		return errorf("%w: rollback is not allowed in state %s", ErrInvalidTransition, rec.State)
	}
	running := rec.State == StateRunning
	if running {
		// Freeze point on a running VM: power cycles against the restored disk.
		cp.backend.markRunning(op.VMID, false)
	}
	if err := cp.backend.step(OpRollback); err != nil {
		if running {
			cp.backend.markRunning(op.VMID, true)
		}
		return err
	}
	snap := sr.Snapshot
	if err := cp.store.Apply(Event{Kind: evRollback, VMID: op.VMID, Snapshot: &snap}); err != nil {
		return err
	}
	if running {
		cp.backend.markRunning(op.VMID, true)
	}
	return nil
}

func (cp *ControlPlane) execDeleteSnapshot(op *opRec) error {
	cp.store.mu.Lock()
	rec := cp.store.state.VMs[op.VMID]
	sr := cp.store.state.Snaps[op.VMID][op.SnapshotID]
	cp.store.mu.Unlock()
	if rec == nil {
		return errorf("%w: %q", ErrVMNotFound, op.VMID)
	}
	if sr == nil {
		return errorf("%w: snapshot %q on vm %q", ErrSnapshotNotFound, op.SnapshotID, op.VMID)
	}
	if err := cp.backend.step(OpDeleteSS); err != nil {
		return err
	}
	snap := sr.Snapshot
	return cp.store.Apply(Event{Kind: evSnapDeleted, VMID: op.VMID, Snapshot: &snap})
}

func (cp *ControlPlane) execWrite(op *opRec) error {
	cp.store.mu.Lock()
	rec := cp.store.state.VMs[op.VMID]
	cp.store.mu.Unlock()
	if rec == nil {
		return errorf("%w: %q", ErrVMNotFound, op.VMID)
	}
	if !isStable(rec.State) {
		return errorf("%w: write is not allowed in state %s", ErrInvalidTransition, rec.State)
	}
	sum := sha256.Sum256(op.Data)
	blobID := "b-" + hex.EncodeToString(sum[:])
	ev := Event{
		Kind:       evBlockWrite,
		VMID:       op.VMID,
		BlockIndex: op.BlockIndex,
		BlobID:     blobID,
		Data:       op.Data,
	}
	return cp.store.Apply(ev)
}

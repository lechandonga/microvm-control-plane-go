package microvm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// Config configures a local, fully simulated control plane.
type Config struct {
	// Dir is the durable data directory (WAL + checkpoint).
	Dir string
	// Hosts is the simulated host pool; consumed only on first initialization.
	Hosts []HostConfig
	// Overcommit configures CPU/memory oversubscription. Defaults to 1.0.
	Overcommit OvercommitRatio
	// BackendDelay models hypervisor call latency.
	BackendDelay time.Duration
	// CrashHook optionally injects a simulated process crash before fsync.
	CrashHook CrashHook
}

// ControlPlane is the local micro-VM orchestrator. All state changes are
// durable and all per-VM workflows are serialized.
type ControlPlane struct {
	store   *Store
	backend *Backend

	mu   sync.Mutex
	busy map[string]bool

	closeOnce sync.Once
	closed    bool
	wg        sync.WaitGroup
}

// Open creates or recovers a control plane and resumes all operations that
// were in flight when the previous process ended.
func Open(cfg Config) (*ControlPlane, error) {
	if cfg.Overcommit.CPU <= 0 {
		cfg.Overcommit.CPU = 1
	}
	if cfg.Overcommit.Mem <= 0 {
		cfg.Overcommit.Mem = 1
	}
	st, err := OpenStore(cfg.Dir, cfg.Hosts, cfg.Overcommit)
	if err != nil {
		return nil, err
	}
	if cfg.CrashHook != nil {
		st.SetCrashHook(cfg.CrashHook)
	}
	cp := &ControlPlane{
		store:   st,
		backend: NewBackend(cfg.BackendDelay),
		busy:    map[string]bool{},
	}
	cp.recoverOperations()
	return cp, nil
}

// Store exposes the durable store (used by tests for crash seams/checkpoints).
func (cp *ControlPlane) Store() *Store { return cp.store }

// Backend exposes the simulated hypervisor for fault injection.
func (cp *ControlPlane) Backend() *Backend { return cp.backend }

// Close waits for in-process workers and closes durable storage.
func (cp *ControlPlane) Close() error {
	cp.closeOnce.Do(func() {
		cp.store.mu.Lock()
		storeDead := cp.store.closed
		cp.store.mu.Unlock()
		cp.mu.Lock()
		cp.closed = true
		cp.mu.Unlock()
		if !storeDead {
			cp.wg.Wait()
		}
	})
	if cp.store.closed {
		return nil
	}
	return cp.store.Close()
}

// Request is one asynchronous workflow submission.
type Request struct {
	Type           OperationType
	VMID           string
	IdempotencyKey string
	Spec           Spec
	SnapshotID     string
	BlockIndex     int
	Data           []byte
}

func newID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:])
}

// Submit validates and enqueues an asynchronous operation. It returns the
// durable operation ID. Validation errors (not found, illegal transition,
// bad arguments, capacity, conflicting in-flight op) are returned synchronously
// and are *not* persisted.
func (cp *ControlPlane) Submit(req Request) (string, error) {
	if req.Type == "" {
		return "", errorf("%w: operation type is required", ErrInvalidArgument)
	}
	if req.VMID == "" && req.Type != OpDefine {
		return "", errorf("%w: vm_id is required for %s", ErrInvalidArgument, req.Type)
	}
	if req.Type == OpDefine {
		if err := req.Spec.validate(); err != nil {
			return "", err
		}
		req.VMID = req.Spec.ID
	}
	if req.Type == OpRollback && req.SnapshotID == "" {
		return "", errorf("%w: snapshot_id is required for rollback", ErrInvalidArgument)
	}
	if req.Type == OpDeleteSS && req.SnapshotID == "" {
		return "", errorf("%w: snapshot_id is required for snapshot deletion", ErrInvalidArgument)
	}
	if req.Type == OpSnapshot && req.SnapshotID == "" {
		req.SnapshotID = newID("snap")
	}
	if req.Type == OpWrite && (len(req.Data) == 0 || req.BlockIndex < 0) {
		return "", errorf("%w: write requires a non-negative block index and data", ErrInvalidArgument)
	}
	if (req.Type == OpRollback || req.Type == OpDeleteSS) && req.VMID != "" {
		cp.store.mu.Lock()
		snaps := cp.store.state.Snaps[req.VMID]
		exists := snaps != nil
		if exists {
			_, exists = snaps[req.SnapshotID]
		}
		cp.store.mu.Unlock()
		if !exists {
			return "", errorf("%w: snapshot %q on vm %q", ErrSnapshotNotFound, req.SnapshotID, req.VMID)
		}
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()
	if cp.closed {
		return "", ErrClosed
	}

	// Idempotent retries always converge on the first operation, regardless
	// of whether it is still running or has already concluded.
	if req.IdempotencyKey != "" {
		cp.store.mu.Lock()
		if opID, ok := cp.store.state.Idem[req.IdempotencyKey]; ok {
			cp.store.mu.Unlock()
			return opID, nil
		}
		cp.store.mu.Unlock()
	}

	state := cp.vmStateLocked(req.VMID)
	if req.Type == OpDefine {
		if existing, ok := stateLookup(cp, req.VMID); ok {
			// A previous Define failed before placement completed: retrying
			// with the same spec and idempotency key resumes it.
			if existing == StatePending && cp.pendingSpecMatches(req.Spec, req.IdempotencyKey) {
				state = StatePending
			} else {
				return "", errorf("%w: %q", ErrAlreadyExists, req.VMID)
			}
		} else {
			state = StatePending
		}
	} else {
		if _, ok := stateLookup(cp, req.VMID); !ok {
			return "", errorf("%w: %q", ErrVMNotFound, req.VMID)
		}
	}
	if state != StatePending && isSnapshotWrite(req.Type) && !isStable(state) {
		return "", errorf("%w: %s is not allowed while state is %s", ErrInvalidTransition, req.Type, state)
	}
	if _, err := transition(state, req.Type); err != nil {
		return "", err
	}

	if cp.busy[req.VMID] {
		return "", errorf("%w: another operation on vm %q is in progress", ErrDuplicateRequest, req.VMID)
	}

	op := &opRec{
		Operation: Operation{
			ID:        newID("op"),
			Type:      req.Type,
			VMID:      req.VMID,
			State:     OpPending,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		},
		IdemKey: req.IdempotencyKey,
	}
	if req.Type == OpDefine {
		op.Spec = req.Spec
	}
	op.SnapshotID = req.SnapshotID
	if req.Type == OpWrite {
		op.BlockIndex = req.BlockIndex
		op.Data = append([]byte(nil), req.Data...)
	}

	// Durably record the intent before doing anything: a crash between
	// validation and execution still leaves a recoverable operation.
	if err := cp.upsertOp(op); err != nil {
		return "", err
	}
	if req.IdempotencyKey != "" {
		if err := cp.store.Apply(Event{Kind: evIdempotency, IdemKey: req.IdempotencyKey, Op: op}); err != nil {
			return "", err
		}
	}
	cp.busy[req.VMID] = true
	cp.wg.Add(1)
	go cp.runOperation(op.ID)
	return op.ID, nil
}

func stateLookup(cp *ControlPlane, vmID string) (VMState, bool) {
	cp.store.mu.Lock()
	defer cp.store.mu.Unlock()
	rec, ok := cp.store.state.VMs[vmID]
	if !ok {
		return "", false
	}
	return rec.State, true
}

func (cp *ControlPlane) vmStateLocked(vmID string) VMState {
	s, _ := stateLookup(cp, vmID)
	return s
}

func isStable(s VMState) bool { return s == StateRunning || s == StateStopped }

func isSnapshotWrite(t OperationType) bool {
	return t == OpSnapshot || t == OpRollback || t == OpDeleteSS || t == OpWrite
}

func (cp *ControlPlane) upsertOp(op *opRec) error {
	op.UpdatedAt = time.Now().UTC()
	cpCopy := *op
	return cp.store.Apply(Event{Kind: evOpUpsert, Op: &cpCopy})
}

func (cp *ControlPlane) release(vmID string) {
	cp.mu.Lock()
	delete(cp.busy, vmID)
	cp.mu.Unlock()
}

// Wait blocks until the operation reaches a terminal conclusion. A failed
// operation returns an error wrapping the distinguishable reason.
func (cp *ControlPlane) Wait(ctx context.Context, opID string) (Operation, error) {
	for {
		cp.store.mu.Lock()
		closed := cp.store.closed
		cp.store.mu.Unlock()
		if closed {
			return Operation{}, ErrClosed
		}
		op, ok := cp.store.getOperation(opID)
		if !ok {
			return Operation{}, errorf("%w: operation %q", ErrOperationFailed, opID)
		}
		if op.Done() {
			if op.Succeeded() {
				return op, nil
			}
			return op, &operationError{opType: op.Type, reason: op.Reason, err: &plainError{msg: op.Error}}
		}
		select {
		case <-ctx.Done():
			return op, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// MustWait is Wait with a background context.
func (cp *ControlPlane) MustWait(opID string) (Operation, error) {
	return cp.Wait(context.Background(), opID)
}

// submitAndWait is a convenience used by tests and synchronous callers.
func (cp *ControlPlane) submitAndWait(req Request) (Operation, error) {
	id, err := cp.Submit(req)
	if err != nil {
		return Operation{}, err
	}
	return cp.MustWait(id)
}

func (cp *ControlPlane) pendingSpecMatches(spec Spec, key string) bool {
	cp.store.mu.Lock()
	defer cp.store.mu.Unlock()
	for _, op := range cp.store.state.Ops {
		if op.VMID == spec.ID && op.Type == OpDefine && op.State == OpFailed && op.Spec == spec {
			if key == "" || op.IdemKey == key || op.IdemKey == "" {
				return true
			}
		}
	}
	return false
}

func (cp *ControlPlane) runOperation(opID string) {
	defer cp.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			if r == simulatedCrash {
				// The simulated process is dead; recovery happens on restart.
				return
			}
			panic(r)
		}
	}()

	cp.store.mu.Lock()
	op := cp.store.state.Ops[opID]
	cp.store.mu.Unlock()
	if op == nil || op.Done() {
		cp.release(opVMID(op, opID))
		return
	}
	if err := cp.execute(op); err != nil {
		_ = cp.compensate(op)
		// Mark the failure durable while the per-VM slot is still held, so a
		// retry cannot observe an unowned transient state.
		_ = cp.finishOp(opID, OpFailed, err)
		cp.release(op.VMID)
		return
	}
	// Release the slot before publishing success: once Wait reports
	// Succeeded, a follow-up submission must never see the VM as busy.
	cp.release(op.VMID)
	_ = cp.finishOp(opID, OpSucceeded, nil)
}

// compensate moves a VM out of a transient state when a workflow failed, so
// the VM is immediately retryable and the ledger never counts resources for
// work that did not complete.
func (cp *ControlPlane) compensate(op *opRec) error {
	if op.Type == OpDefine {
		return nil
	}
	cp.store.mu.Lock()
	rec := cp.store.state.VMs[op.VMID]
	state := VMState("")
	if rec != nil {
		state = rec.State
	}
	cp.store.mu.Unlock()

	var target VMState
	switch op.Type {
	case OpStart:
		if state == StateStarting {
			target = StateStopped
		}
	case OpStop:
		if state == StateStopping {
			target = StateRunning
		}
	case OpRestart:
		if state == StateRestarting {
			target = StateRunning
		}
	case OpDestroy:
		if state == StateDestroying {
			// Destroy failed while backend teardown errored: return the VM to
			// its pre-destroy stable state. Without a record of the origin we
			// conservatively return to Stopped (backend confirmed not running
			// only on full teardown); callers can retry Start/Destroy.
			target = StateStopped
		}
	}
	if target == "" {
		return nil
	}
	cp.backend.markRunning(op.VMID, target == StateRunning)
	return cp.store.Apply(Event{Kind: evVMState, VMID: op.VMID, State: target})
}

func opVMID(op *opRec, fallback string) string {
	if op != nil {
		return op.VMID
	}
	return fallback
}

// recoverOperations resumes every non-terminal operation exactly once. VM
// operations are resumed in per-VM order so the same-VM serialization
// guarantee holds across restarts.
func (cp *ControlPlane) recoverOperations() {
	cp.store.mu.Lock()
	var pending []*opRec
	for _, op := range cp.store.state.Ops {
		if !op.Done() {
			cpCopy := *op
			pending = append(pending, &cpCopy)
		}
	}
	runningVMs := map[string]bool{}
	for _, rec := range cp.store.state.VMs {
		runningVMs[rec.Spec.ID] = rec.State == StateRunning
	}
	order := make(map[string]int)
	for i, id := range cp.store.state.Order {
		order[id] = i
	}
	cp.store.mu.Unlock()

	sortOps(pending, order)

	// Running view of power state is derived from durable state.
	for id, running := range runningVMs {
		cp.backend.markRunning(id, running)
	}

	for _, op := range pending {
		cp.mu.Lock()
		cp.busy[op.VMID] = true
		cp.mu.Unlock()
		err := cp.execute(op)
		if err != nil {
			_ = cp.compensate(op)
			_ = cp.finishOp(op.ID, OpFailed, err)
		} else {
			_ = cp.finishOp(op.ID, OpSucceeded, nil)
		}
		cp.release(op.VMID)
	}
}

func sortOps(ops []*opRec, vmOrder map[string]int) {
	less := func(i, j int) bool {
		a, b := ops[i], ops[j]
		if a.VMID != b.VMID {
			return vmOrder[a.VMID] < vmOrder[b.VMID]
		}
		return a.CreatedAt.Before(b.CreatedAt)
	}
	for i := 1; i < len(ops); i++ {
		for j := i; j > 0 && less(j, j-1); j-- {
			ops[j], ops[j-1] = ops[j-1], ops[j]
		}
	}
}

func (cp *ControlPlane) finishOp(opID string, st OperationState, execErr error) error {
	cp.store.mu.Lock()
	cur := cp.store.state.Ops[opID]
	if cur == nil || cur.Done() {
		cp.store.mu.Unlock()
		return nil
	}
	cur.State = st
	if execErr != nil {
		cur.Error = execErr.Error()
		cur.Reason = reasonName(execErr)
	}
	cpCopy := *cur
	cp.store.mu.Unlock()
	return cp.store.Apply(Event{Kind: evOpUpsert, Op: &cpCopy})
}

func reasonName(err error) string {
	for _, s := range []error{
		ErrVMNotFound, ErrAlreadyExists, ErrInvalidArgument, ErrInvalidTransition,
		ErrDuplicateRequest, ErrCapacityExceeded, ErrSnapshotNotFound,
		ErrBackendFailed, ErrClosed,
	} {
		if errors.Is(err, s) {
			return s.Error()
		}
	}
	return ""
}

package microvm

import (
	"strings"
	"time"
)

// 各模拟操作被拆成的步骤数：步骤越多、延迟越大，越容易在测试中
// 制造“操作进行到一半”的并发/崩溃窗口。
const (
	stepsCreate   = 3
	stepsStart    = 2
	stepsStop     = 1
	stepsRestart  = 3
	stepsDestroy  = 2
	stepsSnapshot = 2
	stepsRollback = 2
)

func (c *Controller) commit(events []*walEvent) error {
	return c.store.Txn(events, nil)
}

// Define 定义虚拟机：同步完成校验与容量预留并持久化，随后异步创建。
func (c *Controller) Define(spec VMSpec, idemKey string) (*Operation, error) {
	if err := validateSpec(spec); err != nil {
		return nil, err
	}
	if strings.TrimSpace(idemKey) == "" {
		return nil, newError(KindInvalidRequest, "empty idempotency key")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClosed
	}

	var events []*walEvent
	var rec *opRecord
	var buildErr error
	c.store.View(func(st *memState) {
		if existing, ok := st.VMs[spec.ID]; ok && existing.State != string(StateDestroyed) {
			buildErr = newError(KindAlreadyExists, "vm %q already defined", spec.ID)
			return
		}
		if dup, derr := c.idemLookup(st, spec.ID, idemKey, OpCreate); derr != nil {
			buildErr = derr
			return
		} else if dup != nil {
			rec = dup // 幂等重试：返回首次记录
			return
		}
		host, herr := chooseHost(st, spec)
		if herr != nil {
			buildErr = herr
			return
		}
		now := time.Now()
		vm := &vmRecord{
			ID: spec.ID, Name: spec.Name, VCPU: spec.VCPU, MemoryMiB: spec.MemoryMiB,
			DiskMiB: spec.DiskMiB, Host: host, State: string(StatePending),
			Disk: []byte{}, CreatedAt: now, UpdatedAt: now,
		}
		rec = &opRecord{
			ID: newID("op"), IdemKey: idemKey, Type: string(OpCreate), VMID: spec.ID,
			Status: string(OpPending), CreatedAt: now, UpdatedAt: now,
		}
		events = []*walEvent{
			{Kind: evVMUpsert, VM: vm},
			{Kind: evOpUpsert, Op: rec},
		}
	})
	if buildErr != nil {
		return nil, buildErr
	}
	if events != nil {
		if err := c.commit(events); err != nil {
			return nil, err
		}
		c.activeMu.Lock()
		c.active[spec.ID] = true
		c.activeMu.Unlock()
	}
	c.launchLocked(rec)
	return opFromRecord(rec), nil
}

// submit 为已存在 VM 提交一个生命周期操作，集中处理目标存在性、
// 幂等重试、同 VM 互斥与状态机校验。
func (c *Controller) submit(vmID, idemKey string, op OpType, allow func(VMState) bool, interState VMState, extra func(st *memState, vm *vmRecord, rec *opRecord) ([]*walEvent, error)) (*Operation, error) {
	if strings.TrimSpace(vmID) == "" {
		return nil, newError(KindInvalidRequest, "empty vm id")
	}
	if strings.TrimSpace(idemKey) == "" {
		return nil, newError(KindInvalidRequest, "empty idempotency key")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errClosed
	}

	var events []*walEvent
	var rec *opRecord
	var buildErr error
	c.store.View(func(st *memState) {
		vm, ok := st.VMs[vmID]
		if !ok || vm.State == string(StateDestroyed) {
			buildErr = newError(KindNotFound, "vm %q not found", vmID)
			return
		}
		if dup, derr := c.idemLookup(st, vmID, idemKey, op); derr != nil {
			buildErr = derr
			return
		} else if dup != nil {
			rec = dup
			return
		}
		if vmBusyLocked(st, vmID) {
			buildErr = newError(KindOperationActive, "vm %q has an operation in progress", vmID)
			return
		}
		if !allow(VMState(vm.State)) {
			buildErr = newError(KindInvalidState, "op %s not allowed in state %s", op, vm.State)
			return
		}
		now := time.Now()
		newRec := &opRecord{
			ID: newID("op"), IdemKey: idemKey, Type: string(op), VMID: vmID,
			Status: string(OpPending), PrevState: vm.State,
			CreatedAt: now, UpdatedAt: now,
		}
		events = []*walEvent{{Kind: evOpUpsert, Op: newRec}}
		if extra != nil {
			more, e := extra(st, vm, newRec)
			if e != nil {
				buildErr = e
				events = nil
				return
			}
			events = append(events, more...)
		}
		if interState != "" {
			// 进入 starting 时同步预留运行时容量（与操作记录同一事务）：
			// 并发启动在串行提交点即可被容量拒绝；启动失败时由完成
			// 事务释放预留，崩溃恢复也不会重复占用。
			if op == OpStart {
				h, ok := st.Hosts[vm.Host]
				if !ok {
					buildErr = newError(KindNotFound, "host %q not found", vm.Host)
					events = nil
					return
				}
				u := computeUsage(vm.Host, st, h)
				if u.AvailableCPU() < vm.VCPU || u.AvailableMemoryMiB() < vm.MemoryMiB {
					buildErr = newError(KindCapacity, "host %q: insufficient runtime capacity for start (cpu need=%d avail=%d; mem need=%dMiB avail=%dMiB)",
						vm.Host, vm.VCPU, u.AvailableCPU(), vm.MemoryMiB, u.AvailableMemoryMiB())
					events = nil
					return
				}
				vm.Held = true
			}
			vm.State = string(interState)
			vm.UpdatedAt = now
			events = append(events, &walEvent{Kind: evVMUpsert, VM: clone(vm)})
		}
		rec = newRec
	})
	if buildErr != nil {
		return nil, buildErr
	}
	if events != nil {
		if err := c.commit(events); err != nil {
			return nil, err
		}
		c.activeMu.Lock()
		c.active[vmID] = true
		c.activeMu.Unlock()
	}
	c.launchLocked(rec)
	return opFromRecord(rec), nil
}

// Start 仅 stopped/failed 状态可启动。
func (c *Controller) Start(vmID, idemKey string) (*Operation, error) {
	return c.submit(vmID, idemKey, OpStart, func(s VMState) bool {
		return s == StateStopped || s == StateFailed
	}, StateStarting, nil)
}

// Stop 仅 running/failed 状态可停止；停止后运行时容量确定释放。
func (c *Controller) Stop(vmID, idemKey string) (*Operation, error) {
	return c.submit(vmID, idemKey, OpStop, func(s VMState) bool {
		return s == StateRunning || s == StateFailed
	}, StateStopping, nil)
}

// Restart 仅 running 状态可重启。
func (c *Controller) Restart(vmID, idemKey string) (*Operation, error) {
	return c.submit(vmID, idemKey, OpRestart, func(s VMState) bool {
		return s == StateRunning
	}, StateRestarting, nil)
}

// Destroy 在任何非 destroyed 状态下都可提交，级联删除快照并释放
// 全部 CPU/内存/存储容量。
func (c *Controller) Destroy(vmID, idemKey string) (*Operation, error) {
	return c.submit(vmID, idemKey, OpDestroy, func(s VMState) bool {
		return true
	}, StateDestroying, nil)
}

// Snapshot 给 running/stopped 的 VM 打快照。镜像在受理时完整复制、
// 空间在同一事务预留，因此操作进行中写入的数据不会混入快照。
func (c *Controller) Snapshot(vmID, idemKey string) (*Operation, error) {
	return c.submit(vmID, idemKey, OpSnapshot, func(s VMState) bool {
		return s == StateRunning || s == StateStopped
	}, StateSnapshotting, func(st *memState, vm *vmRecord, rec *opRecord) ([]*walEvent, error) {
		snapID := newID("snap")
		rec.TargetSnap = snapID
		snap := &snapshotRecord{
			ID: snapID, VMID: vmID,
			Disk:      append([]byte(nil), vm.Disk...),
			SizeMiB:   usedDiskMiB(int64(len(vm.Disk)), vm.DiskMiB),
			CreatedAt: time.Now(),
		}
		h, ok := st.Hosts[vm.Host]
		if !ok {
			return nil, newError(KindNotFound, "host %q not found", vm.Host)
		}
		u := computeUsage(vm.Host, st, h)
		if u.AvailableStorageMiB() < snap.SizeMiB {
			return nil, newError(KindCapacity, "host %q: insufficient storage for snapshot (need=%dMiB avail=%dMiB)", vm.Host, snap.SizeMiB, u.AvailableStorageMiB())
		}
		return []*walEvent{{Kind: evSnapshotUpsert, Snapshot: snap}}, nil
	})
}

// Rollback 回滚到指定快照。running/stopped 均可回滚并保持回滚前的
// 运行状态；磁盘内容整体替换，回滚之后写入的字节不可能残留。
func (c *Controller) Rollback(vmID, snapID, idemKey string) (*Operation, error) {
	if strings.TrimSpace(snapID) == "" {
		return nil, newError(KindInvalidRequest, "empty snapshot id")
	}
	return c.submit(vmID, idemKey, OpRollback, func(s VMState) bool {
		return s == StateRunning || s == StateStopped
	}, StateRollingBack, func(st *memState, vm *vmRecord, rec *opRecord) ([]*walEvent, error) {
		if _, ok := st.Snapshots[snapKey(vmID, snapID)]; !ok {
			return nil, newError(KindSnapshotMissing, "snapshot %q not found for vm %q", snapID, vmID)
		}
		rec.TargetSnap = snapID
		return nil, nil
	})
}

// DeleteSnapshot 删除快照并立即完整回收存储空间。
func (c *Controller) DeleteSnapshot(vmID, snapID string) error {
	if err := c.checkOpen(); err != nil {
		return err
	}
	var events []*walEvent
	var buildErr error
	c.store.View(func(st *memState) {
		vm, ok := st.VMs[vmID]
		if !ok || vm.State == string(StateDestroyed) {
			buildErr = newError(KindNotFound, "vm %q not found", vmID)
			return
		}
		if vmBusyLocked(st, vmID) {
			buildErr = newError(KindOperationActive, "vm %q has an operation in progress", vmID)
			return
		}
		snap, ok := st.Snapshots[snapKey(vmID, snapID)]
		if !ok {
			buildErr = newError(KindSnapshotMissing, "snapshot %q not found for vm %q", snapID, vmID)
			return
		}
		events = []*walEvent{{
			Kind:     evSnapshotDelete,
			Snapshot: &snapshotRecord{VMID: vmID, ID: snap.ID},
		}}
	})
	if buildErr != nil {
		return buildErr
	}
	return c.commit(events)
}

// WriteData 向运行中虚拟机的模拟磁盘追加数据；仅 running 可写。
func (c *Controller) WriteData(vmID string, data []byte) error {
	if len(data) == 0 {
		return newError(KindInvalidRequest, "empty data")
	}
	if err := c.checkOpen(); err != nil {
		return err
	}
	var events []*walEvent
	var buildErr error
	c.store.View(func(st *memState) {
		vm, ok := st.VMs[vmID]
		if !ok || vm.State == string(StateDestroyed) {
			buildErr = newError(KindNotFound, "vm %q not found", vmID)
			return
		}
		if vm.State != string(StateRunning) {
			buildErr = newError(KindInvalidState, "write data requires state %s, got %s", StateRunning, vm.State)
			return
		}
		if vmBusyLocked(st, vmID) {
			buildErr = newError(KindOperationActive, "vm %q has an operation in progress", vmID)
			return
		}
		h := st.Hosts[vm.Host]
		grow := usedDiskMiB(int64(len(vm.Disk)+len(data)), vm.DiskMiB) -
			usedDiskMiB(int64(len(vm.Disk)), vm.DiskMiB)
		u := computeUsage(vm.Host, st, h)
		if u.AvailableStorageMiB() < grow {
			buildErr = newError(KindCapacity, "host %q: insufficient storage for write (need=%dMiB avail=%dMiB)", vm.Host, grow, u.AvailableStorageMiB())
			return
		}
		updated := clone(vm)
		updated.Disk = append(append([]byte(nil), vm.Disk...), data...)
		updated.UpdatedAt = time.Now()
		events = []*walEvent{{Kind: evVMUpsert, VM: updated}}
	})
	if buildErr != nil {
		return buildErr
	}
	return c.commit(events)
}

// ReadData 读取虚拟机模拟磁盘的当前内容。
func (c *Controller) ReadData(vmID string) ([]byte, error) {
	var out []byte
	var err error
	c.store.View(func(st *memState) {
		vm, ok := st.VMs[vmID]
		if !ok || vm.State == string(StateDestroyed) {
			err = newError(KindNotFound, "vm %q not found", vmID)
			return
		}
		out = append([]byte(nil), vm.Disk...)
	})
	return out, err
}

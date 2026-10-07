package microvm

import "time"

// launchLocked 在持有控制器锁时启动操作执行协程。协程会自行抢锁，
// 因此不会与当前调用形成嵌套持锁。终态操作（幂等重试命中）不重复执行。
func (c *Controller) launchLocked(rec *opRecord) {
	if rec == nil {
		return
	}
	if rec.Status == string(OpSucceeded) || rec.Status == string(OpFailed) {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.execute(rec.ID)
	}()
}

// execute 驱动单个操作到确定结论。所有实际状态变更都在最后一个
// 原子事务里提交；执行本身（模拟后端步骤）不修改任何资源账目，
// 因此从头重放幂等，也不会重复占用资源。
func (c *Controller) execute(opID string) {
	for {
		var rec *opRecord
		c.store.View(func(st *memState) {
			rec = clone(st.Ops[opID])
		})
		if rec == nil {
			c.releaseActive(opID)
			return
		}
		if rec.Status == string(OpSucceeded) || rec.Status == string(OpFailed) {
			c.releaseActive(opID)
			return
		}

		// 进入 running（崩溃恢复时也是一次幂等的状态确认）。
		running := clone(rec)
		running.Status = string(OpRunning)
		running.UpdatedAt = time.Now()
		if err := c.store.Txn([]*walEvent{{Kind: evOpUpsert, Op: running}}, nil); err != nil {
			// 存储不可用：短暂退避后重试，避免操作丢在内存里。
			if !c.sleep() {
				return
			}
			continue
		}

		steps := stepsFor(OpType(rec.Type))
		execErr := c.backend.run(c.ctx, rec.VMID, OpType(rec.Type), steps)

		events, finishErr := c.buildCompletion(rec, execErr)
		if finishErr != nil {
			// 理论上仅在状态自相矛盾时发生；把操作标记为 failed 并保留现场。
			c.markExecutionError(rec, finishErr)
			c.releaseActive(opID)
			return
		}
		if err := c.store.Txn(events, nil); err != nil {
			if !c.sleep() {
				return // 模拟崩溃：进程退出，重启后由 recover 重放
			}
			continue
		}
		c.releaseActive(opID)
		return
	}
}

func stepsFor(op OpType) int {
	switch op {
	case OpCreate:
		return stepsCreate
	case OpStart:
		return stepsStart
	case OpStop:
		return stepsStop
	case OpRestart:
		return stepsRestart
	case OpDestroy:
		return stepsDestroy
	case OpSnapshot:
		return stepsSnapshot
	case OpRollback:
		return stepsRollback
	default:
		return 1
	}
}

func (c *Controller) sleep() bool {
	select {
	case <-c.ctx.Done():
		return false
	case <-time.After(5 * time.Millisecond):
		return true
	}
}

func (c *Controller) releaseActive(opID string) {
	var vmID string
	c.store.View(func(st *memState) {
		if r, ok := st.Ops[opID]; ok {
			vmID = r.VMID
		}
	})
	if vmID != "" {
		c.activeMu.Lock()
		delete(c.active, vmID)
		c.activeMu.Unlock()
	}
}

func (c *Controller) markExecutionError(rec *opRecord, cause error) {
	failed := clone(rec)
	failed.Status = string(OpFailed)
	failed.Error = cause.Error()
	failed.UpdatedAt = time.Now()
	_ = c.store.Txn([]*walEvent{{Kind: evOpUpsert, Op: failed}}, nil)
}

// buildCompletion 在一致读视图下构造操作的完成事件：
// 一个操作终态事件 + 必要的 VM/快照事件，全部由调用方原子提交。
func (c *Controller) buildCompletion(rec *opRecord, execErr error) ([]*walEvent, error) {
	var events []*walEvent
	var extra []*walEvent

	c.store.View(func(st *memState) {
		vm := clone(st.VMs[rec.VMID])
		now := time.Now()

		done := clone(rec)
		done.UpdatedAt = now
		if execErr != nil {
			done.Status = string(OpFailed)
			done.Error = execErr.Error()
		} else {
			done.Status = string(OpSucceeded)
		}
		events = append(events, &walEvent{Kind: evOpUpsert, Op: done})

		if execErr != nil {
			switch OpType(rec.Type) {
			case OpCreate:
				vm.State = string(StateCreateFailed)
			case OpStart:
				// 预留的运行时容量随失败事务释放，VM 回到 stopped；
				// 客户机从未真正运行，不存在“丢资源”。
				vm.State = string(StateStopped)
				vm.Held = false
			case OpSnapshot:
				vm.State = rec.PrevState
				// 快照未完成：删除受理时预留的镜像，空间完整回收。
				extra = append(extra, &walEvent{
					Kind:     evSnapshotDelete,
					Snapshot: &snapshotRecord{VMID: vm.ID, ID: rec.TargetSnap},
				})
			case OpRollback:
				vm.State = rec.PrevState // 磁盘保持失败前内容不动
			case OpDestroy:
				// 销毁失败：回到可解释的失败态，资源占用维持现状，可重试销毁。
				if vm.Held {
					vm.State = string(StateFailed)
				} else {
					vm.State = string(StateStopped)
				}
			default:
				// start/stop/restart 失败：已占用就保留占用，避免资源丢失；
				// 未占用就保持未占用，避免凭空超卖。
				vm.State = string(StateFailed)
			}
			vm.UpdatedAt = now
			events = append(events, &walEvent{Kind: evVMUpsert, VM: vm})
			events = append(events, extra...)
			return
		}

		switch OpType(rec.Type) {
		case OpCreate:
			vm.State = string(StateStopped)
			vm.Held = false
		case OpStart:
			vm.State = string(StateRunning)
			vm.Held = true
		case OpStop:
			vm.State = string(StateStopped)
			vm.Held = false
		case OpRestart:
			vm.State = string(StateRunning)
			vm.Held = true // 重启期间不释放、不重复申请运行时容量
		case OpDestroy:
			vm.State = string(StateDestroyed)
			vm.Held = false
			vm.Disk = nil
			for _, snap := range st.Snapshots {
				if snap.VMID == vm.ID {
					extra = append(extra, &walEvent{
						Kind:     evSnapshotDelete,
						Snapshot: &snapshotRecord{VMID: vm.ID, ID: snap.ID},
					})
				}
			}
		case OpSnapshot:
			vm.State = rec.PrevState
			done.Result = rec.TargetSnap
		case OpRollback:
			snap := st.Snapshots[snapKey(vm.ID, rec.TargetSnap)]
			if snap == nil {
				// 快照在操作受理后被并发删除是不允许的（同 VM 互斥），
				// 到达这里说明数据损坏：明确失败而非写入半截状态。
				return
			}
			vm.Disk = append([]byte(nil), snap.Disk...)
			vm.State = rec.PrevState
		}
		vm.UpdatedAt = now
		events = append(events, &walEvent{Kind: evVMUpsert, VM: vm})
		events = append(events, extra...)
	})
	return events, nil
}

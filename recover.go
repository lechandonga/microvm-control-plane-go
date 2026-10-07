package microvm

import "time"

// recover 在控制面打开时恢复崩溃现场：
//
//  1. 重建每台 VM 的互斥标记（是否有未完成操作）；
//  2. 把停留在 pending/running 的操作重新投递执行。模拟后端的每个
//     步骤都幂等，恢复重放要么把操作补到成功，要么产生一次明确的
//     failed（可重新提交同类请求重试），不会永远卡在进行中；
//  3. 若 VM 停在中间态但找不到任何操作记录（理论上不会发生），
//     则把它归置到可解释的失败态，资源占用维持账目可核对。
//
// 关键的资源语义：运行时容量（CPU/内存）只在 start 成功事务里
// 占用、stop/destroy 成功事务里释放，崩溃不会出现“已运行但未记账”
// 或“已释放但仍运行”的窗口。
func (c *Controller) recover() {
	var toRun []string
	c.store.View(func(st *memState) {
		for _, r := range st.Ops {
			if r.Status == string(OpPending) || r.Status == string(OpRunning) {
				c.activeMu.Lock()
				c.active[r.VMID] = true
				c.activeMu.Unlock()
				toRun = append(toRun, r.ID)
			}
		}
	})

	// 修复孤立中间态：没有未完成操作却停在中间态的 VM。
	var fixups []*walEvent
	c.store.View(func(st *memState) {
		now := time.Now()
		for _, vm := range st.VMs {
			if vmBusyLocked(st, vm.ID) || vm.State == string(StateDestroyed) {
				continue
			}
			switch VMState(vm.State) {
			case StateCreating, StateStarting, StateStopping, StateRestarting,
				StateSnapshotting, StateRollingBack, StateDestroying:
				fixed := clone(vm)
				if fixed.Held {
					fixed.State = string(StateFailed)
				} else {
					fixed.State = string(StateCreateFailed)
				}
				fixed.UpdatedAt = now
				fixups = append(fixups, &walEvent{Kind: evVMUpsert, VM: fixed})
			}
		}
	})
	if len(fixups) > 0 {
		_ = c.store.Txn(fixups, nil)
	}

	for _, id := range toRun {
		c.wg.Add(1)
		go func(opID string) {
			defer c.wg.Done()
			c.execute(opID)
		}(id)
	}
}

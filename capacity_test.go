package microvm

import "testing"

// TestCapacity 覆盖超配比核算、放置、防超卖以及停止/销毁/删快照的容量释放。
func TestCapacity(t *testing.T) {
	t.Run("超配比决定可分配容量", func(t *testing.T) {
		c := newController(t)
		u1, _ := c.HostUsage("h1")
		if u1.CPUCap != 8 || u1.MemoryCapMiB != 1024 || u1.StorageCapMiB != 100 {
			t.Fatalf("h1 caps wrong: %+v", u1)
		}
		u2, _ := c.HostUsage("h2")
		if u2.CPUCap != 8 || u2.MemoryCapMiB != 1024 || u2.StorageCapMiB != 100 {
			t.Fatalf("h2 overcommit caps wrong: %+v", u2)
		}
	})

	t.Run("放置不超卖并选择可容纳的宿主机", func(t *testing.T) {
		c := newController(t)
		// 存储在定义时预留：h1 上放两台 50MiB 磁盘后再无空间，
		// 第三台 50MiB 磁盘必须落到 h2。
		defineAndCreate(t, c, specFor("big1", 1, 64, 50), "k1")
		defineAndCreate(t, c, specFor("big2", 1, 64, 50), "k2")
		defineAndCreate(t, c, specFor("big3", 1, 64, 50), "k3")
		if vm, _ := c.GetVM("big1"); vm.Host != "h1" {
			t.Fatalf("big1 host=%s", vm.Host)
		}
		if vm, _ := c.GetVM("big2"); vm.Host != "h1" {
			t.Fatalf("big2 host=%s", vm.Host)
		}
		if vm, _ := c.GetVM("big3"); vm.Host != "h2" {
			t.Fatalf("big3 expected h2, got %s", vm.Host)
		}
		assertUsageInvariants(t, c)
	})

	t.Run("容量不足拒绝创建", func(t *testing.T) {
		c := newController(t)
		// 存储放不下：定义即拒绝。
		_, err := c.Define(specFor("huge", 1, 64, 99999), "kx")
		assertKind(t, err, KindCapacity)
		// CPU/内存超配允许定义（停止时不占运行时容量），但启动拒绝。
		defineAndCreate(t, c, specFor("huge2", 64, 99999, 4), "ky")
		_, err = c.Start("huge2", "kz")
		assertKind(t, err, KindCapacity)
		assertState(t, c, "huge2", StateStopped)
		// 被拒绝后账目必须没有任何变化。
		u, _ := c.HostUsage("h1")
		if u.CPUUsed != 0 || u.MemoryUsedMiB != 0 || u.StorageUsedMiB != 4 {
			t.Fatalf("ledger wrong after rejected start: %+v", u)
		}
	})

	t.Run("运行时容量随启动占用随停止释放", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("cap1", 2, 256, 10), "k1")
		u, _ := c.HostUsage("h1")
		if u.CPUUsed != 0 || u.MemoryUsedMiB != 0 || u.StorageUsedMiB != 10 {
			t.Fatalf("stopped vm should only reserve storage: %+v", u)
		}
		startVM(t, c, "cap1", "k2")
		u, _ = c.HostUsage("h1")
		if u.CPUUsed != 2 || u.MemoryUsedMiB != 256 || u.StorageUsedMiB != 10 || u.VMsRunning != 1 {
			t.Fatalf("running usage wrong: %+v", u)
		}
		// cap2 在 cap1 停止期间定义（存储够），但再启动 cap1 后
		// 2+9 > 8，cap2 启动必须被容量拒绝。
		defineAndCreate(t, c, specFor("cap2", 9, 64, 4), "k3")
		_, err := c.Start("cap2", "k4") // cap1 运行中：2+9 > 8
		assertKind(t, err, KindCapacity)
		assertState(t, c, "cap2", StateStopped)
		// 容量不足不能留下预留：cap1 的账目保持不变。
		u, _ = c.HostUsage("h1")
		if u.CPUUsed != 2 || u.MemoryUsedMiB != 256 {
			t.Fatalf("rejected start leaked reservation: %+v", u)
		}

		op, err := c.Stop("cap1", "k5")
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
		waitOp(t, c, op)
		u, _ = c.HostUsage("h1")
		if u.CPUUsed != 0 || u.MemoryUsedMiB != 0 || u.StorageUsedMiB != 14 {
			t.Fatalf("after stop usage wrong: %+v", u)
		}
	})

	t.Run("销毁与删除快照完整回收存储", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("cap3", 1, 64, 10), "k1")
		startVM(t, c, "cap3", "k2")
		sop, err := c.Snapshot("cap3", "k3")
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		sop = waitOp(t, c, sop)
		u, _ := c.HostUsage("h1")
		if u.StorageUsedMiB != 20 { // disk 10 + snapshot 10
			t.Fatalf("snapshot storage not counted: %+v", u)
		}
		if err := c.DeleteSnapshot("cap3", sop.Result); err != nil {
			t.Fatalf("delete snapshot: %v", err)
		}
		u, _ = c.HostUsage("h1")
		if u.StorageUsedMiB != 10 {
			t.Fatalf("snapshot storage not released: %+v", u)
		}

		dop, err := c.Destroy("cap3", "k4")
		if err != nil {
			t.Fatalf("destroy: %v", err)
		}
		waitOp(t, c, dop)
		u, _ = c.HostUsage("h1")
		if u.CPUUsed != 0 || u.MemoryUsedMiB != 0 || u.StorageUsedMiB != 0 || u.VMsRunning != 0 {
			t.Fatalf("destroy did not fully release: %+v", u)
		}
	})

	t.Run("快照空间不足时被拒绝且不留占用", func(t *testing.T) {
		c := newController(t, HostConfig{
			Name: "small", CPUPhysical: 8, MemoryPhysicalMiB: 1024,
			StoragePhysicalMiB: 20, CPUOvercommit: 1, MemoryOvercommit: 1, StorageOvercommit: 1,
		})
		defineAndCreate(t, c, specFor("s1", 1, 64, 10), "k1")
		op, err := c.Snapshot("s1", "k2")
		if err != nil {
			t.Fatalf("first snapshot should fit: %v", err)
		}
		waitOp(t, c, op)
		// 磁盘10 + 一张快照10 = 20 已满；下一张快照必须被拒绝。
		_, err = c.Snapshot("s1", "k3")
		assertKind(t, err, KindCapacity)
		used, _ := c.HostUsage("small")
		if used.StorageUsedMiB != 20 {
			t.Fatalf("rejected snapshot leaked reservation: %+v", used)
		}
	})
}

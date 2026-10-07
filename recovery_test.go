package microvm

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// reopen 模拟进程崩溃后用同一数据目录重启：不等待进行中的操作，
// 直接关闭（取消后端步骤），然后重新 Open 触发恢复重放。
func reopen(t *testing.T, dir string, hosts ...HostConfig) *Controller {
	t.Helper()
	if len(hosts) == 0 {
		hosts = testHosts()
	}
	c, err := Open(Config{DataDir: dir, Hosts: hosts})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	c.Backend().SetStepDelay(testStepDelay)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestRecovery 覆盖操作进行中崩溃后的确定性恢复。
func TestRecovery(t *testing.T) {
	t.Run("创建进行中崩溃：恢复后重放完成，磁盘只占一份", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		c, err := Open(Config{DataDir: dir, Hosts: testHosts()})
		if err != nil {
			t.Fatal(err)
		}
		c.Backend().SetStepDelay(testStepDelay)

		release := make(chan struct{})
		entered := make(chan struct{})
		var once atomic.Bool
		c.Backend().SetHook(func(vmID string, op OpType, step int) {
			if op == OpCreate && step == 1 && once.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		})
		op, err := c.Define(specFor("cr1", 1, 64, 8), "def")
		if err != nil {
			t.Fatal(err)
		}
		<-entered
		c.SimulateCrash() // 进程在创建步骤中“死掉”
		close(release)

		c2 := reopen(t, dir)
		fin, err := c2.WaitOperation(context.Background(), op.ID)
		if err != nil {
			t.Fatal(err)
		}
		if fin.Status != OpSucceeded {
			t.Fatalf("recovered create should replay to success, got %s: %s", fin.Status, fin.Error)
		}
		assertState(t, c2, "cr1", StateStopped)
		u, _ := c2.HostUsage(c2.mustHost("cr1"))
		if u.StorageUsedMiB != 8 {
			t.Fatalf("storage double-counted after replay: %+v", u)
		}
		assertUsageInvariants(t, c2)
	})

	t.Run("启动提交后崩溃：恢复重放为运行，CPU/内存恰好占用一次", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		c, err := Open(Config{DataDir: dir, Hosts: testHosts()})
		if err != nil {
			t.Fatal(err)
		}
		c.Backend().SetStepDelay(testStepDelay)
		defineAndCreate(t, c, specFor("cr2", 2, 128, 8), "def")

		release := make(chan struct{})
		entered := make(chan struct{})
		var once atomic.Bool
		c.Backend().SetHook(func(vmID string, op OpType, step int) {
			if op == OpStart && step == 1 && once.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		})
		op, err := c.Start("cr2", "up")
		if err != nil {
			t.Fatal(err)
		}
		<-entered
		c.SimulateCrash()
		close(release)

		c2 := reopen(t, dir)
		fin, _ := c2.WaitOperation(context.Background(), op.ID)
		if fin.Status != OpSucceeded {
			t.Fatalf("start replay: %s", fin.Status)
		}
		assertState(t, c2, "cr2", StateRunning)
		u, _ := c2.HostUsage(c2.mustHost("cr2"))
		if u.CPUUsed != 2 || u.MemoryUsedMiB != 128 {
			t.Fatalf("runtime capacity after recovery wrong: %+v", u)
		}
	})

	t.Run("故障注入点在重启后仍把操作落到明确失败并可重试", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		c, err := Open(Config{DataDir: dir, Hosts: testHosts()})
		if err != nil {
			t.Fatal(err)
		}
		c.Backend().SetStepDelay(testStepDelay)
		defineAndCreate(t, c, specFor("cr3", 1, 64, 8), "def")

		release := make(chan struct{})
		entered := make(chan struct{})
		var once atomic.Bool
		c.Backend().SetHook(func(vmID string, op OpType, step int) {
			if op == OpStart && step == 1 && once.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		})
		op, _ := c.Start("cr3", "up-fail")
		<-entered
		c.SimulateCrash()
		close(release)

		// 重启后安排第一次重放失败；操作必须走到 failed 而非卡住。
		c2 := reopen(t, dir)
		c2.Backend().FailOnce("cr3", OpStart, 1)
		fin, _ := c2.WaitOperation(context.Background(), op.ID)
		if fin.Status != OpFailed {
			t.Fatalf("expected failed after injected fault, got %s", fin.Status)
		}
		assertState(t, c2, "cr3", StateStopped)
		u, _ := c2.HostUsage(c2.mustHost("cr3"))
		if u.CPUUsed != 0 || u.MemoryUsedMiB != 0 {
			t.Fatalf("failed start leaked runtime capacity: %+v", u)
		}
		// 用新幂等键重试成功。
		startVM(t, c2, "cr3", "up-ok")
		assertState(t, c2, "cr3", StateRunning)
	})

	t.Run("最后一批事务半写入掉电：撕裂行被拒绝，已 fsync 状态保留", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		c, err := Open(Config{DataDir: dir, Hosts: testHosts()})
		if err != nil {
			t.Fatal(err)
		}
		c.Backend().SetStepDelay(testStepDelay)
		defineAndCreate(t, c, specFor("cr4", 1, 64, 8), "def")
		startVM(t, c, "cr4", "up")

		// 直接在 WAL 末尾追加一段撕裂数据模拟掉电写坏最后一行。
		walPath := filepath.Join(dir, walFile)
		f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString("{\"seq\":999999,\"kind\":\"vm_up")
		_ = f.Sync()
		_ = f.Close()
		c.SimulateCrash()

		// 撕裂行必须被检测到（不能静默当成新状态）。
		if _, err := Open(Config{DataDir: dir, Hosts: testHosts()}); err == nil {
			t.Fatal("expected torn WAL to be rejected")
		}
	})

	t.Run("快照进行中崩溃：恢复后快照成功且容量只计一次", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "data")
		c, err := Open(Config{DataDir: dir, Hosts: testHosts()})
		if err != nil {
			t.Fatal(err)
		}
		c.Backend().SetStepDelay(testStepDelay)
		defineAndCreate(t, c, specFor("cr5", 1, 64, 8), "def")
		startVM(t, c, "cr5", "up")
		if err := c.WriteData("cr5", []byte("POINT")); err != nil {
			t.Fatal(err)
		}

		release := make(chan struct{})
		entered := make(chan struct{})
		var once atomic.Bool
		c.Backend().SetHook(func(vmID string, op OpType, step int) {
			if op == OpSnapshot && step == 1 && once.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		})
		op, _ := c.Snapshot("cr5", "snap")
		<-entered
		c.SimulateCrash()
		close(release)

		c2 := reopen(t, dir)
		fin, _ := c2.WaitOperation(context.Background(), op.ID)
		if fin.Status != OpSucceeded {
			t.Fatalf("snapshot recovery: %s", fin.Status)
		}
		snaps, _ := c2.ListSnapshots("cr5")
		if len(snaps) != 1 || string(snaps[0].Disk) != "POINT" {
			t.Fatalf("snapshot after recovery wrong: %+v", snaps)
		}
		u, _ := c2.HostUsage(c2.mustHost("cr5"))
		if u.StorageUsedMiB != 16 { // disk 8 + snap 8
			t.Fatalf("snapshot storage after recovery: %+v", u)
		}
		assertState(t, c2, "cr5", StateRunning)
	})
}

func (c *Controller) mustHost(vmID string) string {
	vm, err := c.GetVM(vmID)
	if err != nil {
		panic(err)
	}
	return vm.Host
}

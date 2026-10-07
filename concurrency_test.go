package microvm

import (
	"sync"
	"testing"
)

// TestConcurrency 覆盖同一 VM 并发操作互斥、批量启动容量不超卖、
// 以及大量操作交错后账目守恒。全部用例应在 -race 下通过。
func TestConcurrency(t *testing.T) {
	t.Run("同一台 VM 的并发生命周期操作不交错", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("race1", 1, 64, 4), "d")

		const n = 30
		var wg sync.WaitGroup
		var mu sync.Mutex
		accepted, rejected := 0, 0
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				// 交替尝试 start/stop，每个请求独立幂等键。
				var (
					op  *Operation
					err error
				)
				if i%2 == 0 {
					op, err = c.Start("race1", "start-"+itoa(i))
				} else {
					op, err = c.Stop("race1", "stop-"+itoa(i))
				}
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					accepted++
					go func() { _ = op }()
				case KindOf(err) == KindOperationActive || KindOf(err) == KindInvalidState:
					rejected++
				default:
					t.Errorf("unexpected error: %v", err)
				}
			}(i)
		}
		wg.Wait()

		// 所有被受理的操作都必须走到确定终态。
		for _, op := range c.ListOperations("race1") {
			fin := waitOp(t, c, op)
			if fin.Status != OpSucceeded && fin.Status != OpFailed {
				t.Fatalf("op %s stuck in %s", fin.ID, fin.Status)
			}
		}
		if accepted+rejected != n {
			t.Fatalf("accepted=%d rejected=%d total=%d", accepted, rejected, n)
		}
		// 任意时刻串行化意味着：最终状态唯一且账目守恒。
		vm, err := c.GetVM("race1")
		if err != nil {
			t.Fatal(err)
		}
		u, _ := c.HostUsage(vm.Host)
		switch vm.State {
		case StateRunning:
			if u.CPUUsed != 1 || u.MemoryUsedMiB != 64 {
				t.Fatalf("running ledger wrong: %+v", u)
			}
		case StateStopped, StateFailed:
			if u.CPUUsed != 0 || u.MemoryUsedMiB != 0 {
				t.Fatalf("stopped ledger wrong: %+v", u)
			}
		default:
			t.Fatalf("unexpected final state %s", vm.State)
		}
		assertUsageInvariants(t, c)
	})

	t.Run("并发启动多台 VM 不超卖容量", func(t *testing.T) {
		c := newController(t)
		// 单台宿主机 4 CPU，无超配；每台 1C，最多 4 台同时运行。
		c2 := newController(t, HostConfig{
			Name: "solo", CPUPhysical: 4, MemoryPhysicalMiB: 400,
			StoragePhysicalMiB: 1000,
			CPUOvercommit:      1, MemoryOvercommit: 1, StorageOvercommit: 1,
		})
		_ = c
		const m = 12
		for i := 0; i < m; i++ {
			defineAndCreate(t, c2, specFor("vm"+itoa(i), 1, 32, 4), "def-"+itoa(i))
		}
		var wg sync.WaitGroup
		var started int64 = 0
		var mu sync.Mutex
		for i := 0; i < m; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				op, err := c2.Start("vm"+itoa(i), "up-"+itoa(i))
				if err == nil {
					fin := waitOp(t, c2, op)
					if fin.Status == OpSucceeded {
						mu.Lock()
						started++
						mu.Unlock()
					}
				}
			}(i)
		}
		wg.Wait()

		u, _ := c2.HostUsage("solo")
		if u.CPUUsed > 4 {
			t.Fatalf("CPU oversold under concurrency: %d", u.CPUUsed)
		}
		if u.CPUUsed != int(started) || int(started) > 4 {
			t.Fatalf("started=%d but cpu used=%d", started, u.CPUUsed)
		}
		assertUsageInvariants(t, c2)
	})

	t.Run("快照/回滚与生命周期并发不产生脏状态", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("mix", 1, 64, 20), "d")
		startVM(t, c, "mix", "up")

		var wg sync.WaitGroup
		kinds := []string{"snap", "write", "stop", "start", "destroy"}
		for i := 0; i < 40; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := kinds[i%len(kinds)] + "-" + itoa(i)
				switch kinds[i%len(kinds)] {
				case "snap":
					if op, err := c.Snapshot("mix", key); err == nil {
						waitOp(t, c, op)
					}
				case "write":
					_ = c.WriteData("mix", []byte{byte(i)})
				case "stop":
					if op, err := c.Stop("mix", key); err == nil {
						waitOp(t, c, op)
					}
				case "start":
					if op, err := c.Start("mix", key); err == nil {
						waitOp(t, c, op)
					}
				case "destroy":
					if op, err := c.Destroy("mix", key); err == nil {
						waitOp(t, c, op)
					}
				}
			}(i)
		}
		wg.Wait()

		// 收敛后：VM 要么 destroyed，要么处于稳定/失败态；账目守恒。
		vm, err := c.GetVM("mix")
		if err != nil {
			if KindOf(err) != KindNotFound {
				t.Fatal(err)
			}
			u, _ := c.HostUsage("h1")
			if u.CPUUsed != 0 || u.MemoryUsedMiB != 0 {
				t.Fatalf("destroyed vm left runtime usage: %+v", u)
			}
			return
		}
		switch vm.State {
		case StateRunning, StateStopped, StateFailed:
		default:
			t.Fatalf("non-terminal intermediate state leaked: %s", vm.State)
		}
		assertUsageInvariants(t, c)
	})
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

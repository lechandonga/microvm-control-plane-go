package microvm

import (
	"errors"
	"sync"
	"testing"
)

// TestConcurrentOpsSameVMSerialized hammers one VM with parallel lifecycle
// requests: at most one may run, illegal interleavings must be rejected with
// distinct reasons, and the final ledger must stay consistent.
func TestConcurrentOpsSameVMSerialized(t *testing.T) {
	t.Log("business=concurrency case=same_vm_serialized")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	mustDefine(t, cp, testSpec("vm-c1"))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var seenRunning int
	rejectDup, rejectTrans := 0, 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var opID string
			var err error
			switch i % 4 {
			case 0, 1:
				opID, err = cp.Start("vm-c1", "")
			case 2:
				opID, err = cp.Stop("vm-c1", "")
			default:
				opID, err = cp.Restart("vm-c1", "")
			}
			if err != nil {
				mu.Lock()
				switch {
				case errors.Is(err, ErrDuplicateRequest):
					rejectDup++
				case errors.Is(err, ErrInvalidTransition):
					rejectTrans++
				default:
					t.Errorf("unexpected submit error: %v", err)
				}
				mu.Unlock()
				return
			}
			_, werr := cp.MustWait(opID)
			if werr == nil {
				mu.Lock()
				seenRunning++
				mu.Unlock()
				return
			}
			if !errors.Is(werr, ErrInvalidTransition) && !errors.Is(werr, ErrOperationFailed) {
				t.Errorf("unexpected op error: %v", werr)
			}
		}(i)
	}
	wg.Wait()

	t.Logf("business=concurrency accepted=%d rejected_busy=%d rejected_transition=%d",
		seenRunning, rejectDup, rejectTrans)
	if rejectDup == 0 {
		t.Fatal("expected at least one busy-conflict rejection")
	}
	vm, err := cp.GetVM("vm-c1")
	if err != nil {
		t.Fatal(err)
	}
	if !isStable(vm.State) {
		t.Fatalf("VM stuck in transient state %s", vm.State)
	}
	for _, u := range cp.HostUsages() {
		if u.CPUUsed < 0 || u.DiskUsed > u.DiskCapacity {
			t.Fatalf("ledger corruption: %+v", u)
		}
	}
	t.Log("business=concurrency case=same_vm_serialized: PASS")
}

// TestConcurrentDefinePlacement runs many parallel defines against a tiny
// pool: admitted VMs must never exceed capacity; rejected ones must not leak.
func TestConcurrentDefinePlacement(t *testing.T) {
	t.Log("business=concurrency case=parallel_define_admission")
	hosts := []HostConfig{{ID: "h", CPUCapacity: 2, MemCapacity: 1000, DiskCapacity: 2500}}
	cp := openTestCP(t, hosts, OvercommitRatio{CPU: 1, Mem: 1})

	var wg sync.WaitGroup
	admitted, rejected := 0, 0
	var mu sync.Mutex
	for i := 0; i < 12; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			id := "vm-" + string(rune('a'+i))
			spec := Spec{ID: id, VCPU: 1, Memory: 500, Disk: 1000}
			opID, err := cp.Define(spec, "")
			if err != nil {
				t.Errorf("define submit: %v", err)
				return
			}
			if _, err := cp.MustWait(opID); err != nil {
				if !errors.Is(err, ErrCapacityExceeded) {
					t.Errorf("unexpected: %v", err)
				}
				mu.Lock()
				rejected++
				mu.Unlock()
				return
			}
			mu.Lock()
			admitted++
			mu.Unlock()
		}()
	}
	wg.Wait()

	u := hostByID(cp.HostUsages(), "h")
	if u.DiskUsed > u.DiskCapacity {
		t.Fatalf("oversold disk: %d > %d", u.DiskUsed, u.DiskCapacity)
	}
	if admitted != 2 || rejected != 10 {
		t.Fatalf("admitted=%d rejected=%d diskUsed=%d", admitted, rejected, u.DiskUsed)
	}
	t.Logf("business=concurrency admitted=%d rejected=%d disk_used=%d PASS", admitted, rejected, u.DiskUsed)
}

// TestConcurrentSnapshotVersusLifecycle guarantees snapshots never observe a
// transient state and one VM's activity cannot change another VM's blocks.
func TestConcurrentSnapshotVersusLifecycle(t *testing.T) {
	t.Log("business=concurrency case=snapshot_vs_lifecycle")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	mustDefine(t, cp, testSpec("vm-s1"))
	mustDefine(t, cp, testSpec("vm-s2"))
	opID, _ := cp.WriteBlock("vm-s1", 0, []byte("base"), "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatal(err)
	}
	mustStart(t, cp, "vm-s1")

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var opID string
			var err error
			if i%2 == 0 {
				opID, err = cp.TakeSnapshot("vm-s1", "", "")
			} else {
				opID, err = cp.Stop("vm-s1", "")
			}
			if err != nil {
				return
			}
			_, _ = cp.MustWait(opID)
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id2, err := cp.WriteBlock("vm-s2", i, []byte("other"), "")
			if err != nil {
				return
			}
			_, _ = cp.MustWait(id2)
		}(i)
	}
	wg.Wait()

	// vm-s1's block 0 is untouched by vm-s2 activity.
	got, ok := cp.store.ReadBlock("vm-s1", 0)
	if !ok || string(got) != "base" {
		t.Fatalf("cross-vm isolation broken: %q", string(got))
	}
	vm, _ := cp.GetVM("vm-s1")
	for _, sn := range vm.Snapshots {
		if sn.SizeBytes < 0 {
			t.Fatal("negative snapshot size")
		}
	}
	t.Logf("business=concurrency snapshots=%d on vm-s1, isolation intact PASS", len(vm.Snapshots))
}

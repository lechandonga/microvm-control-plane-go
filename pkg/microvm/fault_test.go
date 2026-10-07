package microvm

import (
	"errors"
	"sync/atomic"
	"testing"
)

// TestBackendFailureIsExplicitAndRetryable injects exactly one simulated
// hypervisor failure: the operation must end Failed with ErrBackendFailed,
// the VM must remain in a valid stable state, and a retry must succeed.
func TestBackendFailureIsExplicitAndRetryable(t *testing.T) {
	t.Log("business=fault case=backend_fail_then_retry")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	mustDefine(t, cp, testSpec("vm-f"))

	var calls int32
	cp.Backend().SetFaultInjector(func(OperationType) bool {
		return atomic.AddInt32(&calls, 1) == 1
	})
	opID, err := cp.Start("vm-f", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := waitFail(t, cp, opID); !errors.Is(err, ErrBackendFailed) {
		t.Fatalf("want ErrBackendFailed, got %v", err)
	}
	if got, _ := cp.GetVM("vm-f"); got.State != StateStopped {
		t.Fatalf("failed start must leave VM Stopped, got %s", got.State)
	}

	// Second attempt clears the injected fault and succeeds.
	mustStart(t, cp, "vm-f")
	if got, _ := cp.GetVM("vm-f"); got.State != StateRunning {
		t.Fatalf("retry state: %s", got.State)
	}
	t.Log("business=fault case=backend_fail_then_retry: PASS")
}

// TestFailedOperationNeverOccupiesResources proves a snapshot failure charges
// no snapshot bytes and a failed start holds no CPU/memory.
func TestFailedOperationNeverOccupiesResources(t *testing.T) {
	t.Log("business=fault case=failed_op_no_resources")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	mustDefine(t, cp, testSpec("vm-fr"))
	before := cp.HostUsages()

	cp.Backend().SetFaultInjector(func(t OperationType) bool { return true })
	opID, _ := cp.Start("vm-fr", "")
	_ = waitFail(t, cp, opID)
	opID, _ = cp.TakeSnapshot("vm-fr", "ghost", "")
	_ = waitFail(t, cp, opID)

	after := cp.HostUsages()
	for i := range before {
		if before[i].CPUUsed != after[i].CPUUsed || before[i].MemUsed != after[i].MemUsed || before[i].DiskUsed != after[i].DiskUsed {
			t.Fatalf("failed ops changed ledger:\nbefore %+v\nafter  %+v", before[i], after[i])
		}
	}
	vm, _ := cp.GetVM("vm-fr")
	if len(vm.Snapshots) != 0 {
		t.Fatalf("failed snapshot leaked: %+v", vm.Snapshots)
	}
	t.Log("business=fault case=failed_op_no_resources: PASS")
}

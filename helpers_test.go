package microvm

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

const testStepDelay = 2 * time.Millisecond

func testHosts() []HostConfig {
	return []HostConfig{{
		Name: "h1", CPUPhysical: 8, MemoryPhysicalMiB: 1024, StoragePhysicalMiB: 100,
		CPUOvercommit: 1, MemoryOvercommit: 1, StorageOvercommit: 1,
	}, {
		Name: "h2", CPUPhysical: 4, MemoryPhysicalMiB: 512, StoragePhysicalMiB: 100,
		CPUOvercommit: 2, MemoryOvercommit: 2, StorageOvercommit: 1,
	}}
}

func newController(t *testing.T, hosts ...HostConfig) *Controller {
	t.Helper()
	if len(hosts) == 0 {
		hosts = testHosts()
	}
	dir := filepath.Join(t.TempDir(), "data")
	c, err := Open(Config{DataDir: dir, Hosts: hosts})
	if err != nil {
		t.Fatalf("open controller: %v", err)
	}
	c.Backend().SetStepDelay(testStepDelay)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func waitOp(t *testing.T, c *Controller, op *Operation) *Operation {
	t.Helper()
	got, err := c.WaitOperation(context.Background(), op.ID)
	if err != nil {
		t.Fatalf("wait op %s: %v", op.ID, err)
	}
	return got
}

func defineAndCreate(t *testing.T, c *Controller, spec VMSpec, key string) *VM {
	t.Helper()
	op, err := c.Define(spec, key)
	if err != nil {
		t.Fatalf("define %s: %v", spec.ID, err)
	}
	if got := waitOp(t, c, op); got.Status != OpSucceeded {
		t.Fatalf("create %s: status=%s err=%s", spec.ID, got.Status, got.Error)
	}
	vm, err := c.GetVM(spec.ID)
	if err != nil {
		t.Fatalf("get vm: %v", err)
	}
	return vm
}

func startVM(t *testing.T, c *Controller, id, key string) {
	t.Helper()
	op, err := c.Start(id, key)
	if err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	if got := waitOp(t, c, op); got.Status != OpSucceeded {
		t.Fatalf("start %s: status=%s err=%s", id, got.Status, got.Error)
	}
}

func specFor(id string, vcpu int, mem, disk int64) VMSpec {
	return VMSpec{ID: id, Name: id + "-name", VCPU: vcpu, MemoryMiB: mem, DiskMiB: disk}
}

func assertKind(t *testing.T, err error, want Kind) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error kind %s, got nil", want)
	}
	if got := KindOf(err); got != want {
		t.Fatalf("error kind: got %s (%v), want %s", got, err, want)
	}
}

func assertState(t *testing.T, c *Controller, id string, want VMState) {
	t.Helper()
	vm, err := c.GetVM(id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if vm.State != want {
		t.Fatalf("vm %s state: got %s want %s", id, vm.State, want)
	}
}

func assertUsageInvariants(t *testing.T, c *Controller) {
	t.Helper()
	c.store.View(func(st *memState) {
		for name, h := range st.Hosts {
			u := computeUsage(name, st, h)
			if u.CPUUsed < 0 || u.MemoryUsedMiB < 0 || u.StorageUsedMiB < 0 {
				t.Fatalf("host %s negative usage", name)
			}
			if u.CPUUsed > u.CPUCap || u.MemoryUsedMiB > u.MemoryCapMiB || u.StorageUsedMiB > u.StorageCapMiB {
				t.Fatalf("host %s overcommitted: %+v", name, u)
			}
			if u.CPUUsed+u.AvailableCPU() != u.CPUCap ||
				u.MemoryUsedMiB+u.AvailableMemoryMiB() != u.MemoryCapMiB ||
				u.StorageUsedMiB+u.AvailableStorageMiB() != u.StorageCapMiB {
				t.Fatalf("host %s ledger mismatch: %+v", name, u)
			}
		}
	})
}

package microvm

import (
	"errors"
	"testing"
)

func hostByID(usages []HostUsage, id string) HostUsage {
	for _, u := range usages {
		if u.ID == id {
			return u
		}
	}
	return HostUsage{}
}

// TestPlacementAndLedger covers first-fit placement, no-oversell admission,
// full release on stop/destroy, and snapshot-space accounting.
func TestPlacementAndLedger(t *testing.T) {
	t.Log("business=capacity case=placement_and_release")
	hosts := []HostConfig{{ID: "only", CPUCapacity: 4, MemCapacity: 4000, DiskCapacity: 3000}}
	cp := openTestCP(t, hosts, OvercommitRatio{CPU: 1, Mem: 1})

	spec := Spec{ID: "a", VCPU: 2, Memory: 2000, Disk: 1000}
	mustDefine(t, cp, spec)
	u := hostByID(cp.HostUsages(), "only")
	if u.CPUUsed != 0 || u.MemUsed != 0 || u.DiskUsed != 1000 {
		t.Fatalf("stopped ledger: %+v", u)
	}
	mustStart(t, cp, "a")
	u = hostByID(cp.HostUsages(), "only")
	if u.CPUUsed != 2 || u.MemUsed != 2000 || u.DiskUsed != 1000 {
		t.Fatalf("running ledger: %+v", u)
	}
	if u.CPUFree() != 2 || u.MemFree() != 2000 {
		t.Fatalf("free ledger: %+v", u)
	}

	// Second VM also fits the host's CPU/memory headroom (2+2=4 vCPU).
	mustDefine(t, cp, Spec{ID: "b", VCPU: 2, Memory: 2000, Disk: 1000})
	mustStart(t, cp, "b")
	if got, _ := cp.GetVM("b"); got.State != StateRunning {
		t.Fatalf("b should be running, got %s", got.State)
	}

	// Host is now exactly full of CPU (4/4) and memory (4000/4000). A third
	// VM places while stopped (disk still has room) but its start must be
	// rejected by admission and must not change any accounting.
	mustDefine(t, cp, Spec{ID: "c", VCPU: 1, Memory: 100, Disk: 100})
	startID, err := cp.Start("c", "")
	if err != nil && !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("oversell submit reason: %v", err)
	}
	if err == nil {
		if err := waitFail(t, cp, startID); !errors.Is(err, ErrCapacityExceeded) {
			t.Fatalf("oversell async reason: %v", err)
		}
	}
	if got, _ := cp.GetVM("c"); got.State != StateStopped {
		t.Fatalf("rejected start must leave VM stopped, got %s", got.State)
	}

	// Stopping a frees CPU/memory, then c can start.
	opID, _ := cp.Stop("a", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatal(err)
	}
	mustStart(t, cp, "c")
	u = hostByID(cp.HostUsages(), "only")
	if u.CPUUsed != 3 || u.MemUsed != 2100 || u.DiskUsed != 2100 {
		t.Fatalf("after swap ledger: %+v", u)
	}

	// A bigger VM cannot even be placed: disk free is 900.
	big := Spec{ID: "big", VCPU: 1, Memory: 100, Disk: 1500}
	opID, err = cp.Define(big, "")
	if err != nil {
		t.Fatalf("define submit: %v", err)
	}
	if err := waitFail(t, cp, opID); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("placement failure reason: %v", err)
	}
	// A VM that fails placement must not hold any resources.
	u = hostByID(cp.HostUsages(), "only")
	if u.DiskUsed != 2100 {
		t.Fatalf("failed placement leaked disk: %d", u.DiskUsed)
	}

	// Destroy c releases CPU/memory and its disk reservation.
	opID, _ = cp.Destroy("c", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatal(err)
	}
	u = hostByID(cp.HostUsages(), "only")
	if u.CPUUsed != 2 || u.MemUsed != 2000 || u.DiskUsed != 2000 {
		t.Fatalf("after destroy ledger: %+v", u)
	}

	// Destroy the remaining running b and stopped a: pool empties completely.
	opID, _ = cp.Destroy("b", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatal(err)
	}
	opID, _ = cp.Destroy("a", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatal(err)
	}
	u = hostByID(cp.HostUsages(), "only")
	if u.CPUUsed != 0 || u.MemUsed != 0 || u.DiskUsed != 0 {
		t.Fatalf("pool not empty after all destroys: %+v", u)
	}
	t.Log("business=capacity case=placement_and_release: PASS")
}

// TestOvercommitRatio verifies CPU/memory scale by the configured ratio while
// disk always stays physically capped.
func TestOvercommitRatio(t *testing.T) {
	t.Log("business=capacity case=overcommit")
	hosts := []HostConfig{{ID: "h", CPUCapacity: 4, MemCapacity: 1000, DiskCapacity: 100000}}
	cp := openTestCP(t, hosts, OvercommitRatio{CPU: 2, Mem: 2})
	for i := 0; i < 4; i++ {
		mustDefine(t, cp, Spec{ID: "v" + string(rune('a'+i)), VCPU: 2, Memory: 500, Disk: 100})
	}
	// 4*2=8 vCPU committed against 4*2=8 effective: all must start.
	for i := 0; i < 4; i++ {
		mustStart(t, cp, "v"+string(rune('a'+i)))
	}
	u := hostByID(cp.HostUsages(), "h")
	if u.CPUUsed != 8 || u.MemUsed != 2000 {
		t.Fatalf("overcommitted ledger: %+v", u)
	}
	// Fifth VM places (disk) but exceeds the CPU overcommit allowance at start.
	mustDefine(t, cp, Spec{ID: "v5", VCPU: 2, Memory: 500, Disk: 100})
	startID, err := cp.Start("v5", "")
	if err != nil && !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("overcommit cap at submit: %v", err)
	}
	if err == nil {
		if err := waitFail(t, cp, startID); !errors.Is(err, ErrCapacityExceeded) {
			t.Fatalf("overcommit cap at start: %v", err)
		}
	}
	if got, _ := cp.GetVM("v5"); got.State != StateStopped {
		t.Fatalf("overcommitted VM must stay stopped, got %s", got.State)
	}
	t.Log("business=capacity case=overcommit: PASS")
}

// TestSnapshotSpaceChargedAndReclaimed proves snapshot COW bytes enter the
// ledger and come back completely when the snapshot is deleted.
func TestSnapshotSpaceChargedAndReclaimed(t *testing.T) {
	t.Log("business=capacity case=snapshot_space")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	mustDefine(t, cp, testSpec("vm-ss"))

	write := func(idx int, payload string) {
		opID, err := cp.WriteBlock("vm-ss", idx, []byte(payload), "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cp.MustWait(opID); err != nil {
			t.Fatal(err)
		}
	}
	write(0, "hello")
	write(1, "world")
	opID, _ := cp.TakeSnapshot("vm-ss", "snap1", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatal(err)
	}
	base := hostByID(cp.HostUsages(), "h1").DiskUsed

	// Overwrite block 0: its old bytes are now kept only by the snapshot.
	write(0, "HELLO!")
	u := hostByID(cp.HostUsages(), "h1")
	wantExtra := int64(len("hello"))
	if u.DiskUsed != base+wantExtra {
		t.Fatalf("snapshot delta: got %d want %d (base %d)", u.DiskUsed, base+wantExtra, base)
	}

	// Deleting the snapshot returns the uniquely-held bytes.
	delID, _ := cp.DeleteSnapshot("vm-ss", "snap1", "")
	if _, err := cp.MustWait(delID); err != nil {
		t.Fatal(err)
	}
	u = hostByID(cp.HostUsages(), "h1")
	if u.DiskUsed != base {
		t.Fatalf("snapshot delete did not fully reclaim: %d vs base %d", u.DiskUsed, base)
	}
	t.Log("business=capacity case=snapshot_space: PASS")
}

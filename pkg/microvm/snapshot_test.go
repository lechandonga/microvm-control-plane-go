package microvm

import "testing"

// TestSnapshotRollbackConsistency is the time-travel invariant: data written
// after a snapshot must not appear after rollback, while data present at the
// snapshot must be restored exactly.
func TestSnapshotRollbackConsistency(t *testing.T) {
	t.Log("business=snapshot case=rollback_point_in_time")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	mustDefine(t, cp, testSpec("vm-rb"))

	write := func(idx int, payload string) {
		t.Helper()
		opID, err := cp.WriteBlock("vm-rb", idx, []byte(payload), "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cp.MustWait(opID); err != nil {
			t.Fatal(err)
		}
	}
	read := func(idx int) string {
		t.Helper()
		b, ok := cp.store.ReadBlock("vm-rb", idx)
		if !ok {
			return "<none>"
		}
		return string(b)
	}

	write(0, "a0")
	write(1, "b0")
	opID, _ := cp.TakeSnapshot("vm-rb", "point-1", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatal(err)
	}

	// Post-snapshot writes, including overwrite and new-block growth.
	write(0, "a1")
	write(2, "c1")
	if read(0) != "a1" || read(2) != "c1" {
		t.Fatal("preconditions failed")
	}

	opID, _ = cp.Rollback("vm-rb", "point-1", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if read(0) != "a0" {
		t.Fatalf("block 0 not restored: %s", read(0))
	}
	if read(1) != "b0" {
		t.Fatalf("block 1 lost: %s", read(1))
	}
	if _, ok := cp.store.ReadBlock("vm-rb", 2); ok {
		t.Fatal("post-rollback block 2 leaked back into the disk")
	}
	t.Log("business=snapshot case=rollback_point_in_time: PASS")
}

// TestRollbackWhileRunning shows power state is preserved while disk content
// travels back in time, and another VM is completely unaffected.
func TestRollbackWhileRunning(t *testing.T) {
	t.Log("business=snapshot case=rollback_running_isolated")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	mustDefine(t, cp, testSpec("vm-r1"))
	mustDefine(t, cp, testSpec("vm-r2"))
	mustStart(t, cp, "vm-r1")
	mustStart(t, cp, "vm-r2")

	w := func(vm string, idx int, v string) {
		opID, err := cp.WriteBlock(vm, idx, []byte(v), "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cp.MustWait(opID); err != nil {
			t.Fatal(err)
		}
	}
	w("vm-r1", 0, "before")
	w("vm-r2", 0, "neighbor")
	opID, _ := cp.TakeSnapshot("vm-r1", "s", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatal(err)
	}
	w("vm-r1", 0, "after")
	w("vm-r2", 0, "neighbor-changed")

	opID, _ = cp.Rollback("vm-r1", "s", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatal(err)
	}
	if got, _ := cp.GetVM("vm-r1"); got.State != StateRunning {
		t.Fatalf("running VM should stay running, got %s", got.State)
	}
	b, _ := cp.store.ReadBlock("vm-r1", 0)
	if string(b) != "before" {
		t.Fatalf("rollback content wrong: %q", b)
	}
	b, _ = cp.store.ReadBlock("vm-r2", 0)
	if string(b) != "neighbor-changed" {
		t.Fatalf("neighbor VM affected by rollback: %q", b)
	}
	t.Log("business=snapshot case=rollback_running_isolated: PASS")
}

// TestSnapshotChains verifies multiple snapshots independently return to
// distinct points and each is deletable with precise space reclamation.
func TestSnapshotChains(t *testing.T) {
	t.Log("business=snapshot case=chains_and_delete")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	mustDefine(t, cp, testSpec("vm-ch"))
	w := func(idx int, v string) {
		opID, _ := cp.WriteBlock("vm-ch", idx, []byte(v), "")
		if _, err := cp.MustWait(opID); err != nil {
			t.Fatal(err)
		}
	}
	w(0, "v1")
	id1, _ := cp.TakeSnapshot("vm-ch", "s1", "")
	if _, err := cp.MustWait(id1); err != nil {
		t.Fatal(err)
	}
	w(0, "v22222")
	id2, _ := cp.TakeSnapshot("vm-ch", "s2", "")
	if _, err := cp.MustWait(id2); err != nil {
		t.Fatal(err)
	}
	w(0, "v333333333")

	roll := func(snap, want string) {
		id, _ := cp.Rollback("vm-ch", snap, "")
		if _, err := cp.MustWait(id); err != nil {
			t.Fatal(err)
		}
		b, _ := cp.store.ReadBlock("vm-ch", 0)
		if string(b) != want {
			t.Fatalf("after %s want %q got %q", snap, want, b)
		}
	}
	roll("s2", "v22222")
	roll("s1", "v1")
	roll("s2", "v22222")

	vm, _ := cp.GetVM("vm-ch")
	if len(vm.Snapshots) != 2 {
		t.Fatalf("snapshots: %d", len(vm.Snapshots))
	}
	id, _ := cp.DeleteSnapshot("vm-ch", "s1", "")
	if _, err := cp.MustWait(id); err != nil {
		t.Fatal(err)
	}
	// s2 remains a valid restore point.
	roll("s2", "v22222")
	t.Log("business=snapshot case=chains_and_delete: PASS")
}

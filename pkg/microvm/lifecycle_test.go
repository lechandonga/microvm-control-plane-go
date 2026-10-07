package microvm

import (
	"errors"
	"testing"
)

// TestLifecycleHappyPath covers the legal define -> start -> restart -> stop
// -> destroy chain and verifies every intermediate state.
func TestLifecycleHappyPath(t *testing.T) {
	t.Log("business=lifecycle case=happy_path: legal transitions and final destroy")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	vm := mustDefine(t, cp, testSpec("vm-lc"))
	if vm.State != StateStopped || vm.HostID == "" {
		t.Fatalf("after define: %+v", vm)
	}

	mustStart(t, cp, "vm-lc")
	if got, _ := cp.GetVM("vm-lc"); got.State != StateRunning {
		t.Fatalf("want Running, got %s", got.State)
	}
	if !cp.backend.IsRunning("vm-lc") {
		t.Fatal("backend power state not running")
	}

	opID, _ := cp.Restart("vm-lc", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got, _ := cp.GetVM("vm-lc"); got.State != StateRunning {
		t.Fatalf("want Running after restart, got %s", got.State)
	}

	opID, _ = cp.Stop("vm-lc", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if got, _ := cp.GetVM("vm-lc"); got.State != StateStopped {
		t.Fatalf("want Stopped, got %s", got.State)
	}

	opID, _ = cp.Destroy("vm-lc", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, err := cp.GetVM("vm-lc"); !errors.Is(err, ErrVMNotFound) {
		t.Fatalf("want ErrVMNotFound, got %v", err)
	}
	t.Log("business=lifecycle case=happy_path: PASS")
}

// TestIllegalTransitions verifies each rejected request carries the
// distinguishable ErrInvalidTransition reason.
func TestIllegalTransitions(t *testing.T) {
	t.Log("business=lifecycle case=illegal_transitions")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	mustDefine(t, cp, testSpec("vm-il"))

	try := func(name string, fn func() (string, error)) {
		t.Helper()
		if opID, err := fn(); err == nil {
			_, werr := cp.MustWait(opID)
			if !errors.Is(werr, ErrInvalidTransition) {
				t.Fatalf("%s: want ErrInvalidTransition, got %v", name, werr)
			}
		} else if !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("%s: want ErrInvalidTransition at submit, got %v", name, err)
		}
	}
	try("stop while stopped", func() (string, error) { return cp.Stop("vm-il", "") })
	try("restart while stopped", func() (string, error) { return cp.Restart("vm-il", "") })

	mustStart(t, cp, "vm-il")
	try("start while running", func() (string, error) { return cp.Start("vm-il", "") })

	opID, _ := cp.Destroy("vm-il", "")
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, err := cp.Start("vm-il", ""); !errors.Is(err, ErrVMNotFound) {
		t.Fatalf("want ErrVMNotFound on destroyed vm, got %v", err)
	}
	t.Log("business=lifecycle case=illegal_transitions: PASS")
}

// TestValidationAndMissingTargets checks bad arguments, duplicates and
// unknown targets independently.
func TestValidationAndMissingTargets(t *testing.T) {
	t.Log("business=lifecycle case=validation_and_missing")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})

	bad := testSpec("")
	if _, err := cp.Define(bad, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty id: %v", err)
	}
	bad = testSpec("vm-bad")
	bad.VCPU = 0
	if _, err := cp.Define(bad, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero vcpu: %v", err)
	}
	bad = testSpec("vm-bad")
	bad.Memory = -1
	if _, err := cp.Define(bad, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative memory: %v", err)
	}

	mustDefine(t, cp, testSpec("vm-dup"))
	if _, err := cp.Define(testSpec("vm-dup"), ""); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate define: %v", err)
	}
	if _, err := cp.Start("ghost", ""); !errors.Is(err, ErrVMNotFound) {
		t.Fatalf("unknown target: %v", err)
	}
	if _, err := cp.Rollback("vm-dup", "no-such-snap", ""); err == nil {
		t.Fatal("rollback to missing snapshot should be rejected")
	} else if !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("want ErrSnapshotNotFound, got %v", err)
	}
	t.Log("business=lifecycle case=validation_and_missing: PASS")
}

// TestIdempotentRetry proves repeated submissions with the same key converge
// on one operation and one final effect.
func TestIdempotentRetry(t *testing.T) {
	t.Log("business=lifecycle case=idempotent_retry")
	cp := openTestCP(t, nil, OvercommitRatio{CPU: 1, Mem: 1})
	first, err := cp.Define(testSpec("vm-idem"), "key-define")
	if err != nil {
		t.Fatal(err)
	}
	second, err := cp.Define(testSpec("vm-idem"), "key-define")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("idempotent retry created new op: %s != %s", first, second)
	}
	if _, err := cp.MustWait(first); err != nil {
		t.Fatal(err)
	}
	third, err := cp.Start("vm-idem", "key-start")
	if err != nil {
		t.Fatal(err)
	}
	fourth, err := cp.Start("vm-idem", "key-start")
	if err != nil {
		t.Fatal(err)
	}
	if third != fourth {
		t.Fatal("start retry not idempotent")
	}
	if _, err := cp.MustWait(third); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.MustWait(fourth); err != nil {
		t.Fatal(err)
	}
	t.Log("business=lifecycle case=idempotent_retry: PASS")
}

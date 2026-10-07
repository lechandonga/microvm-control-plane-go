package microvm

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// crashDir returns a data dir and a hook that kills the process model on the
// first event matching the predicate.
func crashHarness(t *testing.T, pred func(Event) bool) (string, CrashHook, *sync.WaitGroup) {
	t.Helper()
	var crashed sync.WaitGroup
	crashed.Add(1)
	var once sync.Once
	hook := func(ev Event) bool {
		if pred(ev) {
			once.Do(func() { crashed.Done() })
			return true
		}
		return false
	}
	return filepath.Join(t.TempDir(), "data"), hook, &crashed
}

func reopen(t *testing.T, dir string) *ControlPlane {
	t.Helper()
	cp, err := Open(Config{Dir: dir, BackendDelay: time.Millisecond})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = cp.Close() })
	return cp
}

// TestCrashDuringDefine: the placement event is durable-or-nothing; after
// restart the operation replays and the VM lands exactly once in Stopped with
// exactly one disk reservation.
func TestCrashDuringDefine(t *testing.T) {
	t.Log("business=recovery case=crash_mid_define")
	dir, hook, crashed := crashHarness(t, func(ev Event) bool {
		return ev.Kind == evVMPlaced
	})
	cp, err := Open(Config{Dir: dir, Hosts: testHosts(), Overcommit: OvercommitRatio{CPU: 1, Mem: 1},
		BackendDelay: time.Millisecond, CrashHook: hook})
	if err != nil {
		t.Fatal(err)
	}
	opID, _ := cp.Define(testSpec("vm-cd"), "")
	crashed.Wait()
	// The process model is dead at this point: close without waiting for the
	// killed worker, and restart like a real supervisor would.
	_ = cp.Close()

	cp2 := reopen(t, dir)
	op, err := cp2.MustWait(opID)
	if err != nil {
		t.Fatalf("replayed define: %v", err)
	}
	if !op.Succeeded() {
		t.Fatalf("define not succeeded after replay: %+v", op)
	}
	vm, err := cp2.GetVM("vm-cd")
	if err != nil {
		t.Fatal(err)
	}
	if vm.State != StateStopped || vm.HostID == "" {
		t.Fatalf("bad vm after replay: %+v", vm)
	}
	u := hostByID(cp2.HostUsages(), vm.HostID)
	if u.DiskUsed != 1000 {
		t.Fatalf("double/missing reservation: diskUsed=%d", u.DiskUsed)
	}
	t.Log("business=recovery case=crash_mid_define: PASS")
}

// TestCrashDuringStart recovers from the transient Starting state: the VM
// must finish in Running, never stay Starting, and CPU/memory count once.
func TestCrashDuringStart(t *testing.T) {
	t.Log("business=recovery case=crash_mid_start")
	dir0 := filepath.Join(t.TempDir(), "data")
	cp0, err := Open(Config{Dir: dir0, Hosts: testHosts(), Overcommit: OvercommitRatio{CPU: 1, Mem: 1},
		BackendDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	mustDefine(t, cp0, testSpec("vm-cs"))
	_ = cp0.Close()

	dir, hook, crashed := crashHarness(t, func(ev Event) bool {
		return ev.Kind == evVMState && ev.VMID == "vm-cs" && ev.State == StateRunning
	})
	// Reuse the prepared directory: move approach is awkward, so rebuild in
	// the same temp dir instead.
	_ = dir
	cp1, err := Open(Config{Dir: dir0, BackendDelay: time.Millisecond, CrashHook: hook})
	if err != nil {
		t.Fatal(err)
	}
	opID, _ := cp1.Start("vm-cs", "")
	crashed.Wait()
	_ = cp1.Close()

	cp2 := reopen(t, dir0)
	op, err := cp2.MustWait(opID)
	if err != nil {
		t.Fatal(err)
	}
	if !op.Succeeded() {
		t.Fatalf("start replay: %+v", op)
	}
	vm, _ := cp2.GetVM("vm-cs")
	if vm.State != StateRunning {
		t.Fatalf("want Running, got %s", vm.State)
	}
	u := hostByID(cp2.HostUsages(), vm.HostID)
	if u.CPUUsed != 2 || u.MemUsed != 1024 {
		t.Fatalf("ledger after start replay: %+v", u)
	}
	if !cp2.Backend().IsRunning("vm-cs") {
		t.Fatal("runtime view not re-derived to running")
	}
	t.Log("business=recovery case=crash_mid_start: PASS")
}

// TestCrashDuringSnapshot: either the snapshot completes on replay or, if its
// event never landed, it is failed/retryable; the disk content is intact and
// there is no half snapshot.
func TestCrashDuringSnapshot(t *testing.T) {
	t.Log("business=recovery case=crash_mid_snapshot")
	dir0 := filepath.Join(t.TempDir(), "data")
	cp0, err := Open(Config{Dir: dir0, Hosts: testHosts(), Overcommit: OvercommitRatio{CPU: 1, Mem: 1},
		BackendDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	mustDefine(t, cp0, testSpec("vm-cr"))
	id, _ := cp0.WriteBlock("vm-cr", 0, []byte("payload"), "")
	if _, err := cp0.MustWait(id); err != nil {
		t.Fatal(err)
	}
	_ = cp0.Close()

	var crashed sync.WaitGroup
	crashed.Add(1)
	var once sync.Once
	hook := func(ev Event) bool {
		if ev.Kind == evSnapCreated && ev.VMID == "vm-cr" {
			once.Do(func() { crashed.Done() })
			return true
		}
		return false
	}
	cp1, err := Open(Config{Dir: dir0, BackendDelay: time.Millisecond, CrashHook: hook})
	if err != nil {
		t.Fatal(err)
	}
	opID, _ := cp1.TakeSnapshot("vm-cr", "snap-x", "")
	crashed.Wait()
	_ = cp1.Close()

	cp2 := reopen(t, dir0)
	op, err := cp2.MustWait(opID)
	if err != nil {
		t.Fatalf("snapshot replay should complete: %v", err)
	}
	if !op.Succeeded() {
		t.Fatalf("snapshot state: %+v", op)
	}
	vm, _ := cp2.GetVM("vm-cr")
	if len(vm.Snapshots) != 1 || vm.Snapshots[0].ID != "snap-x" {
		t.Fatalf("half snapshot state: %+v", vm.Snapshots)
	}
	b, _ := cp2.store.ReadBlock("vm-cr", 0)
	if string(b) != "payload" {
		t.Fatalf("disk corrupted by crash: %q", b)
	}

	// Rollback after restart must still give exact point-in-time content.
	rb, _ := cp2.WriteBlock("vm-cr", 0, []byte("changed"), "")
	if _, err := cp2.MustWait(rb); err != nil {
		t.Fatal(err)
	}
	rid, _ := cp2.Rollback("vm-cr", "snap-x", "")
	if _, err := cp2.MustWait(rid); err != nil {
		t.Fatal(err)
	}
	b, _ = cp2.store.ReadBlock("vm-cr", 0)
	if string(b) != "payload" {
		t.Fatalf("post-restart rollback wrong: %q", b)
	}
	t.Log("business=recovery case=crash_mid_snapshot: PASS")
}

// TestCrashBeforeIntentDurable: a crash before the operation record lands is
// invisible after restart; a client retry behaves exactly like the first try.
func TestCrashBeforeIntentDurable(t *testing.T) {
	t.Log("business=recovery case=crash_before_intent")
	dir := filepath.Join(t.TempDir(), "data")
	var crashed sync.WaitGroup
	crashed.Add(1)
	var once sync.Once
	hook := func(ev Event) bool {
		if ev.Kind == evOpUpsert && ev.Op != nil && ev.Op.Type == OpDefine {
			once.Do(func() { crashed.Done() })
			return true
		}
		return false
	}
	cp1, err := Open(Config{Dir: dir, Hosts: testHosts(), Overcommit: OvercommitRatio{CPU: 1, Mem: 1},
		BackendDelay: time.Millisecond, CrashHook: hook})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cp1.Define(testSpec("vm-ce"), ""); err == nil || !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed from dead process model, got %v", err)
	}
	crashed.Wait()
	_ = cp1.Close()

	cp2 := reopen(t, dir)
	mustDefine(t, cp2, testSpec("vm-ce"))
	vm, _ := cp2.GetVM("vm-ce")
	if vm.State != StateStopped {
		t.Fatalf("retry after pre-intent crash: %s", vm.State)
	}
	t.Log("business=recovery case=crash_before_intent: PASS")
}

// TestCheckpointReplay compacts the log and proves recovery from a checkpoint
// plus an empty WAL keeps VMs, snapshots and the ledger identical.
func TestCheckpointReplay(t *testing.T) {
	t.Log("business=recovery case=checkpoint_then_restart")
	dir := filepath.Join(t.TempDir(), "data")
	cp0, err := Open(Config{Dir: dir, Hosts: testHosts(), Overcommit: OvercommitRatio{CPU: 2, Mem: 2},
		BackendDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	mustDefine(t, cp0, testSpec("vm-k1"))
	mustStart(t, cp0, "vm-k1")
	id, _ := cp0.WriteBlock("vm-k1", 3, []byte("xyz"), "")
	if _, err := cp0.MustWait(id); err != nil {
		t.Fatal(err)
	}
	id, _ = cp0.TakeSnapshot("vm-k1", "k-snap", "")
	if _, err := cp0.MustWait(id); err != nil {
		t.Fatal(err)
	}
	if err := cp0.Store().Checkpoint(); err != nil {
		t.Fatal(err)
	}
	// More traffic after compaction must be recovered from the post-checkpoint WAL.
	id, _ = cp0.Stop("vm-k1", "")
	if _, err := cp0.MustWait(id); err != nil {
		t.Fatal(err)
	}
	_ = cp0.Close()

	cp1 := reopen(t, dir)
	vm, err := cp1.GetVM("vm-k1")
	if err != nil {
		t.Fatal(err)
	}
	if vm.State != StateStopped || len(vm.Snapshots) != 1 {
		t.Fatalf("post-checkpoint recovery: state=%s snaps=%d", vm.State, len(vm.Snapshots))
	}
	b, ok := cp1.store.ReadBlock("vm-k1", 3)
	if !ok || string(b) != "xyz" {
		t.Fatalf("block recovery: %q ok=%v", b, ok)
	}
	after := cp1.HostUsages()
	u := hostByID(after, vm.HostID)
	if u.CPUUsed != 0 || u.DiskUsed != 1000 {
		t.Fatalf("ledger after checkpoint restart: %+v", u)
	}
	t.Log("business=recovery case=checkpoint_then_restart: PASS")
}

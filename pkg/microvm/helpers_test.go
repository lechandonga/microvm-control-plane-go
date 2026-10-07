package microvm

import (
	"path/filepath"
	"testing"
	"time"
)

func testHosts() []HostConfig {
	return []HostConfig{
		{ID: "h1", CPUCapacity: 4, MemCapacity: 4096, DiskCapacity: 10_000},
		{ID: "h2", CPUCapacity: 8, MemCapacity: 8192, DiskCapacity: 20_000},
	}
}

func testSpec(id string) Spec {
	return Spec{ID: id, Name: id, VCPU: 2, Memory: 1024, Disk: 1000}
}

func openTestCP(t *testing.T, hosts []HostConfig, ratio OvercommitRatio) *ControlPlane {
	t.Helper()
	if hosts == nil {
		hosts = testHosts()
	}
	cp, err := Open(Config{
		Dir:          filepath.Join(t.TempDir(), "data"),
		Hosts:        hosts,
		Overcommit:   ratio,
		BackendDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = cp.Close() })
	return cp
}

func mustDefine(t *testing.T, cp *ControlPlane, spec Spec) VM {
	t.Helper()
	opID, err := cp.Define(spec, "")
	if err != nil {
		t.Fatalf("Define: %v", err)
	}
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatalf("Define wait: %v", err)
	}
	vm, err := cp.GetVM(spec.ID)
	if err != nil {
		t.Fatalf("GetVM: %v", err)
	}
	return vm
}

func mustStart(t *testing.T, cp *ControlPlane, id string) {
	t.Helper()
	opID, err := cp.Start(id, "")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := cp.MustWait(opID); err != nil {
		t.Fatalf("Start wait: %v", err)
	}
}

func waitFail(t *testing.T, cp *ControlPlane, opID string) error {
	t.Helper()
	_, err := cp.MustWait(opID)
	if err == nil {
		t.Fatalf("expected operation failure")
	}
	return err
}

package microvm

import (
	"os"
	"path/filepath"
	"testing"
)

// TestStoreCompaction 覆盖 WAL 压缩后跨重启状态一致、容量无损。
func TestStoreCompaction(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	c, err := Open(Config{DataDir: dir, Hosts: testHosts()})
	if err != nil {
		t.Fatal(err)
	}
	c.Backend().SetStepDelay(0)

	// 制造超过 compactionEvents 条事件（定义/销毁循环）。
	for i := 0; i < 120; i++ {
		id := "vmc" + itoa(i)
		defineAndCreate(t, c, specFor(id, 1, 16, 1), "d-"+itoa(i))
		op, err := c.Destroy(id, "x-"+itoa(i))
		if err != nil {
			t.Fatalf("destroy: %v", err)
		}
		waitOp(t, c, op)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	walInfo, _ := os.Stat(filepath.Join(dir, walFile))
	cpInfo, err := os.Stat(filepath.Join(dir, checkpointFile))
	if err != nil {
		t.Fatalf("checkpoint not written: %v", err)
	}
	if walInfo.Size() >= cpInfo.Size() {
		// 压缩后 WAL 应被截断（墓碑之外几乎为空）。
		t.Fatalf("compaction did not shrink wal: wal=%d cp=%d", walInfo.Size(), cpInfo.Size())
	}

	c2 := reopen(t, dir)
	if vms := c2.ListVMs(); len(vms) != 0 {
		t.Fatalf("compaction lost tombstones/state: %d live vms", len(vms))
	}
	u, _ := c2.HostUsage("h1")
	if u.CPUUsed != 0 || u.MemoryUsedMiB != 0 || u.StorageUsedMiB != 0 {
		t.Fatalf("ledger after compacted restart: %+v", u)
	}
	// 压缩恢复后仍可正常工作。
	defineAndCreate(t, c2, specFor("after", 1, 16, 2), "da")
	assertState(t, c2, "after", StateStopped)
}

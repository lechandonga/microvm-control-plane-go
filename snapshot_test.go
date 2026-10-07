package microvm

import (
	"sync"
	"testing"
)

// TestSnapshotConsistency 覆盖快照点一致、回滚隔离与跨 VM 不串数据。
func TestSnapshotConsistency(t *testing.T) {
	t.Run("运行中快照与回滚：回滚后写入不残留", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("svm1", 1, 64, 10), "d")
		startVM(t, c, "svm1", "s")

		if err := c.WriteData("svm1", []byte("A")); err != nil {
			t.Fatalf("write A: %v", err)
		}
		sop, err := c.Snapshot("svm1", "snap1")
		if err != nil {
			t.Fatalf("snapshot: %v", err)
		}
		sop = waitOp(t, c, sop)
		snapA := sop.Result

		if err := c.WriteData("svm1", []byte("B")); err != nil {
			t.Fatalf("write B: %v", err)
		}
		data, _ := c.ReadData("svm1")
		if string(data) != "AB" {
			t.Fatalf("expected AB, got %q", data)
		}

		// 回滚到 A 时刻；B 必须彻底消失。
		rop, err := c.Rollback("svm1", snapA, "rb1")
		if err != nil {
			t.Fatalf("rollback: %v", err)
		}
		waitOp(t, c, rop)
		data, _ = c.ReadData("svm1")
		if string(data) != "A" {
			t.Fatalf("rollback leaked later write: got %q want A", data)
		}
		assertState(t, c, "svm1", StateRunning) // 回滚后保持运行
	})

	t.Run("快照受理后进行中的写入不混入镜像", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("svm2", 1, 64, 10), "d")
		startVM(t, c, "svm2", "s")
		if err := c.WriteData("svm2", []byte("BASE")); err != nil {
			t.Fatal(err)
		}

		release := make(chan struct{})
		entered := make(chan struct{})
		var hookOnce sync.Once
		var blockOnce sync.Once
		c.Backend().SetHook(func(vmID string, op OpType, step int) {
			if op != OpSnapshot || step != 1 {
				return
			}
			hookOnce.Do(func() { close(entered) })
			blockOnce.Do(func() { <-release })
		})
		sop, err := c.Snapshot("svm2", "snap")
		if err != nil {
			t.Fatalf("snapshot submit: %v", err)
		}
		<-entered

		// 快照镜像在受理事务中已定型：进行中写盘被状态机拒绝
		//（非 running），因此不可能污染快照，结论可解释。
		err = c.WriteData("svm2", []byte("LATE"))
		assertKind(t, err, KindInvalidState)
		close(release)
		sop = waitOp(t, c, sop)

		// 快照镜像只包含 BASE；后来的 LATE 只存在于活动磁盘。
		snaps, _ := c.ListSnapshots("svm2")
		if len(snaps) != 1 || string(snaps[0].Disk) != "BASE" {
			t.Fatalf("snapshot content wrong: %+v", snaps)
		}
		data, _ := c.ReadData("svm2")
		if string(data) != "BASE" {
			t.Fatalf("rejected write must not modify live disk: %q", data)
		}
		c.Backend().SetHook(nil)
	})

	t.Run("已停止 VM 可打快照并回滚，回滚后仍停止", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("svm3", 1, 64, 10), "d")
		// 启动写点数据再停止。
		startVM(t, c, "svm3", "u")
		if err := c.WriteData("svm3", []byte("X")); err != nil {
			t.Fatal(err)
		}
		waitOp(t, c, mustOp(c.Stop("svm3", "down")))

		sop, err := c.Snapshot("svm3", "snap")
		if err != nil {
			t.Fatalf("stopped snapshot: %v", err)
		}
		sop = waitOp(t, c, sop)

		rop, err := c.Rollback("svm3", sop.Result, "rb")
		if err != nil {
			t.Fatalf("stopped rollback: %v", err)
		}
		waitOp(t, c, rop)
		assertState(t, c, "svm3", StateStopped)
		data, _ := c.ReadData("svm3")
		if string(data) != "X" {
			t.Fatalf("stopped rollback data: %q", data)
		}
	})

	t.Run("回滚到不存在的快照被拒绝", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("svm4", 1, 64, 10), "d")
		_, err := c.Rollback("svm4", "nope", "rb")
		assertKind(t, err, KindSnapshotMissing)
	})

	t.Run("快照相互隔离：一台 VM 的回滚不影响另一台", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("iso-a", 1, 64, 10), "da")
		defineAndCreate(t, c, specFor("iso-b", 1, 64, 10), "db")
		startVM(t, c, "iso-a", "ua")
		startVM(t, c, "iso-b", "ub")
		if err := c.WriteData("iso-a", []byte("AA")); err != nil {
			t.Fatal(err)
		}
		if err := c.WriteData("iso-b", []byte("BB")); err != nil {
			t.Fatal(err)
		}
		sop, err := c.Snapshot("iso-a", "sa")
		if err != nil {
			t.Fatal(err)
		}
		sop = waitOp(t, c, sop)
		if err := c.WriteData("iso-a", []byte("CC")); err != nil {
			t.Fatal(err)
		}
		if err := c.WriteData("iso-b", []byte("DD")); err != nil {
			t.Fatal(err)
		}
		waitOp(t, c, mustOp(c.Rollback("iso-a", sop.Result, "ra")))

		a, _ := c.ReadData("iso-a")
		b, _ := c.ReadData("iso-b")
		if string(a) != "AA" {
			t.Fatalf("iso-a should roll back to AA, got %q", a)
		}
		if string(b) != "BBDD" {
			t.Fatalf("iso-b must be unaffected, got %q", b)
		}
		// 快照查询也不能串台。
		snapsA, _ := c.ListSnapshots("iso-a")
		snapsB, _ := c.ListSnapshots("iso-b")
		if len(snapsA) != 1 || len(snapsB) != 0 {
			t.Fatalf("snapshot cross-talk: a=%d b=%d", len(snapsA), len(snapsB))
		}
	})
}

func mustOp(op *Operation, err error) *Operation {
	if err != nil {
		panic(err)
	}
	return op
}

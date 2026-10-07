package microvm

import (
	"testing"
)

// TestStateMachine 覆盖生命周期状态机的合法路径与非法迁移拒绝。
func TestStateMachine(t *testing.T) {
	t.Run("define_create_start_stop_restart_destroy 合法全流程", func(t *testing.T) {
		c := newController(t)
		vm := defineAndCreate(t, c, specFor("vm-ok", 1, 64, 4), "k1")
		if vm.State != StateStopped {
			t.Fatalf("new vm state=%s want stopped", vm.State)
		}
		startVM(t, c, "vm-ok", "k2")
		assertState(t, c, "vm-ok", StateRunning)

		op, err := c.Restart("vm-ok", "k3")
		if err != nil {
			t.Fatalf("restart: %v", err)
		}
		if got := waitOp(t, c, op); got.Status != OpSucceeded {
			t.Fatalf("restart: %s %s", got.Status, got.Error)
		}
		assertState(t, c, "vm-ok", StateRunning)

		op, err = c.Stop("vm-ok", "k4")
		if err != nil {
			t.Fatalf("stop: %v", err)
		}
		if got := waitOp(t, c, op); got.Status != OpSucceeded {
			t.Fatalf("stop: %s %s", got.Status, got.Error)
		}
		assertState(t, c, "vm-ok", StateStopped)

		op, err = c.Destroy("vm-ok", "k5")
		if err != nil {
			t.Fatalf("destroy: %v", err)
		}
		if got := waitOp(t, c, op); got.Status != OpSucceeded {
			t.Fatalf("destroy: %s %s", got.Status, got.Error)
		}
		if _, err := c.GetVM("vm-ok"); KindOf(err) != KindNotFound {
			t.Fatalf("destroyed vm lookup: %v", err)
		}
	})

	t.Run("非法迁移被拒绝并给出原因", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("vm-bad", 1, 64, 4), "k1")

		// stopped 状态不能 stop / restart / 再 stop
		_, err := c.Stop("vm-bad", "k2")
		assertKind(t, err, KindInvalidState)
		_, err = c.Restart("vm-bad", "k3")
		assertKind(t, err, KindInvalidState)

		// 目标不存在
		_, err = c.Start("ghost", "k4")
		assertKind(t, err, KindNotFound)
	})

	t.Run("非法参数与重复定义", func(t *testing.T) {
		c := newController(t)
		_, err := c.Define(specFor("vm1", 0, 64, 4), "k")
		assertKind(t, err, KindInvalidRequest)
		_, err = c.Define(specFor("vm2", 1, 0, 4), "k")
		assertKind(t, err, KindInvalidRequest)
		_, err = c.Define(specFor("vm3", 1, 64, 0), "k")
		assertKind(t, err, KindInvalidRequest)
		_, err = c.Define(specFor("vm4", 1, 64, 4), "")
		assertKind(t, err, KindInvalidRequest)

		defineAndCreate(t, c, specFor("vm-dup", 1, 64, 4), "k9")
		_, err = c.Define(specFor("vm-dup", 1, 64, 4), "k10")
		assertKind(t, err, KindAlreadyExists)
	})

	t.Run("销毁后同名 ID 可重新定义", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("vm-rec", 1, 64, 4), "k1")
		op, err := c.Destroy("vm-rec", "k2")
		if err != nil {
			t.Fatalf("destroy: %v", err)
		}
		waitOp(t, c, op)
		defineAndCreate(t, c, specFor("vm-rec", 1, 64, 4), "k3")
	})
}

// TestIdempotency 覆盖同一请求多次重试结果一致。
func TestIdempotency(t *testing.T) {
	t.Run("相同幂等键重复提交返回同一操作", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("vm-idem", 1, 64, 4), "def-1")

		op1, err := c.Start("vm-idem", "start-1")
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		op2, err := c.Start("vm-idem", "start-1")
		if err != nil {
			t.Fatalf("retry start: %v", err)
		}
		if op1.ID != op2.ID {
			t.Fatalf("idempotent retry created new op: %s vs %s", op1.ID, op2.ID)
		}
		got := waitOp(t, c, op2)
		if got.Status != OpSucceeded {
			t.Fatalf("retried op status=%s", got.Status)
		}
		// 再次重试，结论不变。
		op3, _ := c.Start("vm-idem", "start-1")
		if op3.ID != op1.ID || op3.Status != OpSucceeded {
			t.Fatalf("second retry mismatch: %+v", op3)
		}
	})

	t.Run("幂等键复用于不同操作类型被拒绝", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("vm-key", 1, 64, 4), "same-key")
		_, err := c.Start("vm-key", "same-key")
		assertKind(t, err, KindDuplicate)
	})

	t.Run("失败操作可以用新幂等键重试并成功", func(t *testing.T) {
		c := newController(t)
		defineAndCreate(t, c, specFor("vm-retry", 1, 64, 4), "def")
		c.Backend().FailOnce("vm-retry", OpStart, 1)
		op, err := c.Start("vm-retry", "start-fail")
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		got := waitOp(t, c, op)
		if got.Status != OpFailed {
			t.Fatalf("expected failed op, got %s", got.Status)
		}
		assertState(t, c, "vm-retry", StateStopped)
		// 同一键重试返回失败记录；换新键可成功。
		again, _ := c.Start("vm-retry", "start-fail")
		if again.Status != OpFailed {
			t.Fatalf("same key should replay failed record, got %s", again.Status)
		}
		startVM(t, c, "vm-retry", "start-ok")
		assertState(t, c, "vm-retry", StateRunning)
	})
}

package microvm

import (
	"context"
	"sync"
	"time"
)

// Backend 是模拟的虚拟化后端：它不创建任何真实的虚拟机、磁盘或
// 进程，只通过“步骤 + 可配延迟 + 故障注入”来模拟耗时与失败。
//
// 每个操作被拆成若干幂等步骤，恢复重放时从头再跑一遍也不会
// 重复占用资源——真实的资源账目全部由控制器以 WAL 事件为准。
type Backend struct {
	mu sync.Mutex

	// stepDelay 是每个模拟步骤的耗时，默认 0（测试可拉大以便制造并发窗口）。
	stepDelay time.Duration

	// failAt 指示某次操作在第几步失败（1-based 步骤序号）；
	// key 为 vmID + "/" + opType，值为失败步骤；命中后删除，只失败一次。
	failAt map[string]int

	// hook 在每一步开始前调用，测试可用它阻塞、触发模拟崩溃或观测进度。
	hook func(vmID string, op OpType, step int)
}

// NewBackend 创建一个默认无延迟、无故障的模拟后端。
func NewBackend() *Backend {
	return &Backend{failAt: map[string]int{}}
}

// SetStepDelay 配置每个模拟步骤的耗时。
func (b *Backend) SetStepDelay(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stepDelay = d
}

// SetHook 安装步骤前回调（传 nil 可移除）。
func (b *Backend) SetHook(hook func(vmID string, op OpType, step int)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hook = hook
}

// FailOnce 安排指定 VM 的下一次指定操作在 step 步骤失败一次。
func (b *Backend) FailOnce(vmID string, op OpType, step int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failAt[vmID+"/"+string(op)] = step
}

func (b *Backend) shouldFail(vmID string, op OpType, step int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := vmID + "/" + string(op)
	if s, ok := b.failAt[key]; ok && s == step {
		delete(b.failAt, key)
		return true
	}
	return false
}

func (b *Backend) runStep(ctx context.Context, vmID string, op OpType, step int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	hook := b.hook
	delay := b.stepDelay
	b.mu.Unlock()
	if hook != nil {
		hook(vmID, op, step)
	}
	if b.shouldFail(vmID, op, step) {
		return newError(KindFailed, "simulated backend failure: vm=%s op=%s step=%d", vmID, op, step)
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// run 执行一个完整的模拟操作。steps 为步骤数，每一步都幂等。
func (b *Backend) run(ctx context.Context, vmID string, op OpType, steps int) error {
	for step := 1; step <= steps; step++ {
		if err := b.runStep(ctx, vmID, op, step); err != nil {
			return err
		}
	}
	return nil
}

package microvm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"
)

// Config 是控制面的本地配置。
type Config struct {
	DataDir string // WAL/checkpoint 目录
	Hosts   []HostConfig
}

// Controller 是本地微虚拟机编排控制面。
type Controller struct {
	store   *store
	backend *Backend

	mu sync.Mutex
	// active 记录每台 VM 当前是否已有进行中的异步操作。
	activeMu sync.Mutex
	active   map[string]bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

// Open 打开或创建控制面，恢复持久化状态并重放所有未完成操作。
// 恢复结束后，原先卡在 pending/running 的操作都会落到确定结论，
// 不会一直停在进行中。
func Open(cfg Config) (*Controller, error) {
	st, err := openStore(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Controller{
		store:   st,
		backend: NewBackend(),
		active:  map[string]bool{},
		ctx:     ctx,
		cancel:  cancel,
	}
	// 注册/更新宿主机配置（首次启动写入，重复 Open 不产生重复占用）。
	for _, h := range cfg.Hosts {
		if err := validateHost(h); err != nil {
			return nil, err
		}
		rec := hostConfigToRecord(h)
		if err := st.Txn([]*walEvent{{Kind: evHostUpsert, Host: rec}}, nil); err != nil {
			return nil, err
		}
	}
	c.recover()
	return c, nil
}

// Backend 暴露模拟后端，供测试配置延迟与故障注入。
func (c *Controller) Backend() *Backend { return c.backend }

// Close 优雅关闭：等待所有进行中的操作落定（成功或失败）后再关。
func (c *Controller) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	c.wg.Wait()
	c.cancel()
	_ = c.store.compactIfNeeded()
	return c.store.close()
}

// SimulateCrash 模拟进程在操作进行中被杀死：取消所有执行中的
// 操作（模拟后端步骤因此中止），不做优雅收尾，直接关闭存储。
// 之后用 Open 重新打开同一数据目录即可观测重启恢复行为。
func (c *Controller) SimulateCrash() {
	c.cancel()
	// 不等待执行协程：真实崩溃时内核直接回收线程。被钩子阻塞的
	// 协程会随测试进程继续存在，但它们在 ctx 取消后不再提交状态；
	// 关闭存储后其任何迟到的写入都会失败并被忽略。
	_ = c.store.close()
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
}

// ---------------------------------------------------------------------------
// 查询接口
// ---------------------------------------------------------------------------

// GetVM 返回虚拟机当前状态；已销毁或不存在返回 KindNotFound。
func (c *Controller) GetVM(id string) (*VM, error) {
	if strings.TrimSpace(id) == "" {
		return nil, newError(KindInvalidRequest, "empty vm id")
	}
	var out *VM
	c.store.View(func(st *memState) {
		if r, ok := st.VMs[id]; ok && r.State != string(StateDestroyed) {
			out = vmFromRecord(r)
		}
	})
	if out == nil {
		return nil, newError(KindNotFound, "vm %q not found", id)
	}
	return out, nil
}

// ListVMs 列出所有未销毁的虚拟机。
func (c *Controller) ListVMs() []*VM {
	var out []*VM
	c.store.View(func(st *memState) {
		for _, r := range st.VMs {
			if r.State != string(StateDestroyed) {
				out = append(out, vmFromRecord(r))
			}
		}
	})
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// GetOperation 按操作 ID 查询异步操作记录。
func (c *Controller) GetOperation(id string) (*Operation, error) {
	var out *Operation
	c.store.View(func(st *memState) {
		if r, ok := st.Ops[id]; ok {
			out = opFromRecord(r)
		}
	})
	if out == nil {
		return nil, newError(KindNotFound, "operation %q not found", id)
	}
	return out, nil
}

// ListOperations 返回某台 VM 的全部操作（按序列号排序）。
func (c *Controller) ListOperations(vmID string) []*Operation {
	var out []*Operation
	c.store.View(func(st *memState) {
		for _, r := range st.Ops {
			if r.VMID == vmID {
				out = append(out, opFromRecord(r))
			}
		}
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// WaitOperation 阻塞等待操作到达终态；控制面关闭时返回当前状态。
func (c *Controller) WaitOperation(ctx context.Context, id string) (*Operation, error) {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		op, err := c.GetOperation(id)
		if err != nil {
			return nil, err
		}
		if op.Status == OpSucceeded || op.Status == OpFailed {
			return op, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			return op, nil
		case <-ticker.C:
		}
	}
}

// ListSnapshots 返回一台 VM 的全部快照。
func (c *Controller) ListSnapshots(vmID string) ([]*Snapshot, error) {
	var out []*Snapshot
	errNotFound := false
	c.store.View(func(st *memState) {
		vm, ok := st.VMs[vmID]
		if !ok || vm.State == string(StateDestroyed) {
			errNotFound = true
			return
		}
		for _, r := range st.Snapshots {
			if r.VMID == vmID {
				out = append(out, snapshotFromRecord(r))
			}
		}
	})
	if errNotFound {
		return nil, newError(KindNotFound, "vm %q not found", vmID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// HostUsage 返回一台宿主机任一时刻的容量账目；used 由当前持久化
// 状态直接求和得到（不做增量累加），因此天然不会漂移。
func (c *Controller) HostUsage(host string) (HostUsage, error) {
	var u HostUsage
	err := newError(KindNotFound, "host %q not found", host)
	c.store.View(func(st *memState) {
		h, ok := st.Hosts[host]
		if !ok {
			return
		}
		err = nil
		u = computeUsage(host, st, h)
	})
	return u, err
}

func computeUsage(host string, st *memState, h *hostRecord) HostUsage {
	u := HostUsage{
		Name:          host,
		CPUCap:        int(scaleDown(int64(h.CPUPhysical), h.CPUOvercommit)),
		MemoryCapMiB:  scaleDown(h.MemoryPhysicalMiB, h.MemoryOvercommit),
		StorageCapMiB: scaleDown(h.StoragePhysicalMiB, h.StorageOvercommit),
	}
	for _, vm := range st.VMs {
		if vm.Host != host || vm.State == string(StateDestroyed) {
			continue
		}
		// 磁盘空间从创建成功起保留到销毁完成，保证不会超卖。
		u.StorageUsedMiB += usedDiskMiB(int64(len(vm.Disk)), vm.DiskMiB)
		if vm.Held {
			u.CPUUsed += vm.VCPU
			u.MemoryUsedMiB += vm.MemoryMiB
			u.VMsRunning++
		}
	}
	for _, snap := range st.Snapshots {
		if vm, ok := st.VMs[snap.VMID]; ok && vm.Host == host {
			u.StorageUsedMiB += snap.SizeMiB
		}
	}
	return u
}

// scaleDown 计算 floor(physical * ratio)；ratio<=0 视为 1.0。
func scaleDown(physical int64, ratio float64) int64 {
	if ratio <= 0 {
		ratio = 1
	}
	return int64(float64(physical) * ratio)
}

// usedDiskMiB 按 MiB 向上取整，模拟磁盘永远不会低估占用。
func usedDiskMiB(dataBytes, logicalMiB int64) int64 {
	dataMiB := (dataBytes + 1024*1024 - 1) / (1024 * 1024)
	if logicalMiB > dataMiB {
		return logicalMiB
	}
	return dataMiB
}

// ---------------------------------------------------------------------------
// 参数校验
// ---------------------------------------------------------------------------

func validateHost(h HostConfig) error {
	if strings.TrimSpace(h.Name) == "" {
		return newError(KindInvalidRequest, "empty host name")
	}
	if h.CPUPhysical <= 0 || h.MemoryPhysicalMiB <= 0 || h.StoragePhysicalMiB <= 0 {
		return newError(KindInvalidRequest, "host %q capacities must be positive", h.Name)
	}
	if h.CPUOvercommit < 1 || h.MemoryOvercommit < 1 || h.StorageOvercommit < 1 {
		return newError(KindInvalidRequest, "host %q overcommit ratios must be >= 1", h.Name)
	}
	return nil
}

func validateSpec(spec VMSpec) error {
	if strings.TrimSpace(spec.ID) == "" {
		return newError(KindInvalidRequest, "empty vm id")
	}
	if strings.TrimSpace(spec.Name) == "" {
		return newError(KindInvalidRequest, "vm %q: empty name", spec.ID)
	}
	if spec.VCPU <= 0 {
		return newError(KindInvalidRequest, "vm %q: vcpu must be positive", spec.ID)
	}
	if spec.MemoryMiB <= 0 {
		return newError(KindInvalidRequest, "vm %q: memory must be positive", spec.ID)
	}
	if spec.DiskMiB <= 0 {
		return newError(KindInvalidRequest, "vm %q: disk must be positive", spec.ID)
	}
	return nil
}

func hostConfigToRecord(h HostConfig) *hostRecord {
	return &hostRecord{
		Name: h.Name, CPUPhysical: h.CPUPhysical,
		MemoryPhysicalMiB: h.MemoryPhysicalMiB, StoragePhysicalMiB: h.StoragePhysicalMiB,
		CPUOvercommit: h.CPUOvercommit, MemoryOvercommit: h.MemoryOvercommit,
		StorageOvercommit: h.StorageOvercommit,
	}
}

// chooseHost 放置 VM：指定宿主机则用之，否则按名字序选第一个
// 同时满足 CPU、内存与存储（含磁盘/快照）约束的宿主机。
func chooseHost(st *memState, spec VMSpec) (string, error) {
	candidates := make([]*hostRecord, 0, len(st.Hosts))
	if spec.Host != "" {
		h, ok := st.Hosts[spec.Host]
		if !ok {
			return "", newError(KindNotFound, "host %q not found", spec.Host)
		}
		candidates = append(candidates, h)
	} else {
		names := make([]string, 0, len(st.Hosts))
		for name := range st.Hosts {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			candidates = append(candidates, st.Hosts[name])
		}
		if len(candidates) == 0 {
			return "", newError(KindCapacity, "no host configured")
		}
	}
	var lastErr error
	for _, h := range candidates {
		u := computeUsage(h.Name, st, h)
		// 定义阶段只对真实占用字节的存储做放置约束；CPU/内存是
		// 启动时才占用的运行时资源，在 Start 提交点再次核算。
		if u.AvailableStorageMiB() < spec.DiskMiB {
			lastErr = newError(KindCapacity, "host %q: insufficient storage (need=%dMiB avail=%dMiB)", h.Name, spec.DiskMiB, u.AvailableStorageMiB())
			continue
		}
		return h.Name, nil
	}
	return "", lastErr
}

// ---------------------------------------------------------------------------
// 幂等键与 ID 生成
// ---------------------------------------------------------------------------

// idemLookup 返回同一幂等键下该 VM 已存在的操作（不同类型即冲突）。
func (c *Controller) idemLookup(st *memState, vmID, key string, op OpType) (*opRecord, error) {
	for _, r := range st.Ops {
		if r.IdemKey == key {
			if r.VMID != vmID || r.Type != string(op) {
				return nil, newError(KindDuplicate, "idempotency key %q already used by another request", key)
			}
			return r, nil
		}
	}
	return nil, nil
}

func newID(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + "-" + hex.EncodeToString(b[:]) + "-" + strconv62(time.Now().UnixNano())
}

func strconv62(n int64) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	if n == 0 {
		return "0"
	}
	var out []byte
	for n > 0 {
		out = append([]byte{digits[n%62]}, out...)
		n /= 62
	}
	return string(out)
}

var errClosed = newError(KindClosed, "controller is closed")

func (c *Controller) checkOpen() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errClosed
	}
	return nil
}

// vmBusyLocked 在持有控制器锁/存储视图时判断该 VM 是否有
// 未完成操作。事实来源是持久化的操作记录（最新一条非终态即忙），
// 内存 active 集合只用于崩溃恢复时的快速重建，避免因调度滞后
// 导致“操作已落定却仍报忙”。
func vmBusyLocked(st *memState, vmID string) bool {
	var latest int64 = -1
	var last *opRecord
	for _, r := range st.Ops {
		if r.VMID == vmID && r.Seq > latest {
			latest, last = r.Seq, r
		}
	}
	return last != nil && last.Status != string(OpSucceeded) && last.Status != string(OpFailed)
}

func (c *Controller) isActive(vmID string) bool {
	busy := false
	c.store.View(func(st *memState) { busy = vmBusyLocked(st, vmID) })
	return busy
}

func (c *Controller) setActive(vmID string) {
	c.activeMu.Lock()
	c.active[vmID] = true
	c.activeMu.Unlock()
}
func (c *Controller) clearActive(vmID string) {
	c.activeMu.Lock()
	delete(c.active, vmID)
	c.activeMu.Unlock()
}

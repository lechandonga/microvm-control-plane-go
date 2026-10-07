package microvm

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// 存储目录中的文件名。
const (
	walFile          = "state.wal"
	checkpointFile   = "state.cp"
	cpTempFile       = "state.cp.tmp"
	compactionEvents = 256 // 每追加这么多条事件做一次 checkpoint 压缩
)

// 事件种类：恢复时按种类把记录投影回内存状态。
const (
	evHostUpsert     = "host_upsert"
	evVMUpsert       = "vm_upsert"
	evSnapshotUpsert = "snapshot_upsert"
	evSnapshotDelete = "snapshot_delete"
	evOpUpsert       = "op_upsert"
)

// walEvent 是 WAL 中的一条事件。记录本身是完整快照（不是补丁），
// 恢复逻辑因此非常直白，也天然支持幂等重放。
type walEvent struct {
	Seq  int64     `json:"seq"`
	Time time.Time `json:"time"`
	Kind string    `json:"kind"`

	VM       *vmRecord       `json:"vm,omitempty"`
	Snapshot *snapshotRecord `json:"snapshot,omitempty"`
	Op       *opRecord       `json:"op,omitempty"`
	Host     *hostRecord     `json:"host,omitempty"`
}

type vmRecord struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	VCPU      int       `json:"vcpu"`
	MemoryMiB int64     `json:"memory_mib"`
	DiskMiB   int64     `json:"disk_mib"`
	Host      string    `json:"host"`
	State     string    `json:"state"`
	Held      bool      `json:"held"`
	Disk      []byte    `json:"disk"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type snapshotRecord struct {
	ID        string    `json:"id"`
	VMID      string    `json:"vm_id"`
	Disk      []byte    `json:"disk"`
	SizeMiB   int64     `json:"size_mib"`
	CreatedAt time.Time `json:"created_at"`
}

type opRecord struct {
	ID         string    `json:"id"`
	IdemKey    string    `json:"idem_key"`
	Type       string    `json:"type"`
	VMID       string    `json:"vm_id"`
	Status     string    `json:"status"`
	TargetSnap string    `json:"target_snap,omitempty"`
	PrevState  string    `json:"prev_state,omitempty"`
	Error      string    `json:"error,omitempty"`
	Result     string    `json:"result,omitempty"`
	Seq        int64     `json:"seq"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type hostRecord struct {
	Name               string  `json:"name"`
	CPUPhysical        int     `json:"cpu_physical"`
	MemoryPhysicalMiB  int64   `json:"memory_physical_mib"`
	StoragePhysicalMiB int64   `json:"storage_physical_mib"`
	CPUOvercommit      float64 `json:"cpu_overcommit"`
	MemoryOvercommit   float64 `json:"memory_overcommit"`
	StorageOvercommit  float64 `json:"storage_overcommit"`
}

type memState struct {
	VMs       map[string]*vmRecord
	Snapshots map[string]*snapshotRecord // key: vmID + "/" + snapID
	Ops       map[string]*opRecord       // key: opID
	Hosts     map[string]*hostRecord
}

func newMemState() *memState {
	return &memState{
		VMs:       map[string]*vmRecord{},
		Snapshots: map[string]*snapshotRecord{},
		Ops:       map[string]*opRecord{},
		Hosts:     map[string]*hostRecord{},
	}
}

func (m *memState) ensureMaps() {
	if m.VMs == nil {
		m.VMs = map[string]*vmRecord{}
	}
	if m.Snapshots == nil {
		m.Snapshots = map[string]*snapshotRecord{}
	}
	if m.Ops == nil {
		m.Ops = map[string]*opRecord{}
	}
	if m.Hosts == nil {
		m.Hosts = map[string]*hostRecord{}
	}
}

type checkpoint struct {
	NextSeq int64     `json:"next_seq"` // 已包含事件的最大 seq + 1
	State   *memState `json:"state"`
}

// store 是基于追加式 WAL + 周期 checkpoint 的持久化存储。
// 所有方法都在互斥锁内执行；控制器的复合状态变更通过 Txn 一次
// 落盘多条事件，保证不会出现“改了一半”的状态。
type store struct {
	dir           string
	mu            sync.Mutex
	f             *os.File
	state         *memState
	nextSeq       int64
	eventsSinceCp int
	closed        bool
}

// openStore 打开（必要时创建）数据目录，加载 checkpoint 并重放 WAL。
func openStore(dir string) (*store, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("store: empty data directory")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: create dir: %w", err)
	}
	s := &store{dir: dir, state: newMemState()}

	if data, err := os.ReadFile(filepath.Join(dir, checkpointFile)); err == nil {
		if len(data) > 0 {
			var cp checkpoint
			if err := json.Unmarshal(data, &cp); err != nil {
				return nil, fmt.Errorf("store: decode checkpoint: %w", err)
			}
			if cp.State != nil {
				s.state = cp.State
				s.state.ensureMaps()
			}
			s.nextSeq = cp.NextSeq
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("store: read checkpoint: %w", err)
	}

	f, err := os.OpenFile(filepath.Join(dir, walFile), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store: open wal: %w", err)
	}
	s.f = f

	if err := s.replayWAL(); err != nil {
		f.Close()
		return nil, err
	}
	// 上次运行可能已达压缩阈值但没来得及在关闭时压缩：
	// 在接受任何新写入之前先做一次安全压缩。
	if s.eventsSinceCp >= compactionEvents {
		if err := s.compactLocked(); err != nil {
			return nil, fmt.Errorf("store: startup compaction: %w", err)
		}
	}
	return s, nil
}

// replayWAL 顺序重放 WAL；checkpoint 已投影的事件按 seq 去重。
func (s *store) replayWAL() error {
	f, err := os.Open(filepath.Join(s.dir, walFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024) // 模拟磁盘可能较大
	count := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var ev walEvent
		if err := json.Unmarshal(line, &ev); err != nil {
			return fmt.Errorf("store: decode wal event: %w", err)
		}
		if ev.Seq < s.nextSeq {
			continue // checkpoint 已包含
		}
		s.applyEvent(&ev)
		s.nextSeq = ev.Seq + 1
		count++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("store: scan wal: %w", err)
	}
	s.eventsSinceCp = count
	return nil
}

func (s *store) applyEvent(ev *walEvent) {
	switch ev.Kind {
	case evHostUpsert:
		s.state.Hosts[ev.Host.Name] = clone(ev.Host)
	case evVMUpsert:
		s.state.VMs[ev.VM.ID] = clone(ev.VM)
	case evSnapshotUpsert:
		s.state.Snapshots[snapKey(ev.Snapshot.VMID, ev.Snapshot.ID)] = clone(ev.Snapshot)
	case evSnapshotDelete:
		// 删除事件复用快照记录的键字段。
		delete(s.state.Snapshots, snapKey(ev.Snapshot.VMID, ev.Snapshot.ID))
	case evOpUpsert:
		s.state.Ops[ev.Op.ID] = clone(ev.Op)
	}
}

func snapKey(vmID, snapID string) string { return vmID + "/" + snapID }

// Txn 在一次持久化事务内追加多条事件并更新内存状态。
// 事件先整体 fsync 落盘，再投影到内存；进程重启后状态由事件重建。
func (s *store) Txn(events []*walEvent, mutate func(st *memState)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("store: closed")
	}
	if len(events) == 0 {
		if mutate != nil {
			mutate(s.state)
		}
		return nil
	}
	now := time.Now()
	var buf []byte
	for _, ev := range events {
		ev.Seq = s.nextSeq
		if ev.Time.IsZero() {
			ev.Time = now
		}
		line, err := json.Marshal(ev)
		if err != nil {
			return fmt.Errorf("store: encode event: %w", err)
		}
		buf = append(buf, line...)
		buf = append(buf, '\n')
		s.nextSeq++
		s.eventsSinceCp++
	}
	if _, err := s.f.Write(buf); err != nil {
		return fmt.Errorf("store: write wal: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("store: sync wal: %w", err)
	}
	for _, ev := range events {
		s.applyEvent(ev)
	}
	if mutate != nil {
		mutate(s.state)
	}
	return nil
}

// needsCompaction 报告 WAL 是否积累到应压缩的阈值。
func (s *store) needsCompaction() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eventsSinceCp >= compactionEvents
}

// compactLocked 在互斥锁内写出 checkpoint 并截断 WAL。
// 压缩期间 Txn 持锁不会插入新事件，因此 checkpoint 与截断之间
// 不存在丢失窗口；崩溃后靠事件 seq 去重保持幂等。
func (s *store) compactLocked() error {
	cp := checkpoint{NextSeq: s.nextSeq, State: s.state}
	data, err := json.Marshal(&cp)
	if err != nil {
		return err
	}
	tmpPath := filepath.Join(s.dir, cpTempFile)
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	tmp, err := os.OpenFile(tmpPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	tmp.Close()
	if err := os.Rename(tmpPath, filepath.Join(s.dir, checkpointFile)); err != nil {
		return err
	}
	dir, err := os.Open(s.dir)
	if err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	// 追加句柄需要关闭后以空文件重开，完成截断。
	if err := s.f.Close(); err != nil {
		return err
	}
	walPath := filepath.Join(s.dir, walFile)
	if err := os.WriteFile(walPath, nil, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.f = f
	s.eventsSinceCp = 0
	return nil
}

// View 在互斥语义下一致地读取内存状态。回调内不得调用 Txn。
func (s *store) View(fn func(st *memState)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.state)
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

// compactIfNeeded 在没有并发写的安全点压缩 WAL。
func (s *store) compactIfNeeded() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.eventsSinceCp < compactionEvents {
		return nil
	}
	return s.compactLocked()
}

// truncateWAL 仅供测试：截掉 WAL 末尾 tailToDrop 字节，
// 模拟最后一批事务写到一半掉电。
func (s *store) truncateWAL(tailToDrop int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := filepath.Join(s.dir, walFile)
	data, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	if tailToDrop < 0 {
		tailToDrop = 0
	}
	if tailToDrop > len(data) {
		tailToDrop = len(data)
	}
	return os.WriteFile(name, data[:len(data)-tailToDrop], 0o644)
}

func clone[T any](p *T) *T {
	if p == nil {
		return nil
	}
	data, _ := json.Marshal(p)
	var v T
	_ = json.Unmarshal(data, &v)
	return &v
}

// 以下为持久化记录与对外类型之间的转换。

func vmFromRecord(r *vmRecord) *VM {
	return &VM{
		ID: r.ID, Name: r.Name, VCPU: r.VCPU, MemoryMiB: r.MemoryMiB,
		DiskMiB: r.DiskMiB, Host: r.Host, State: VMState(r.State), Held: r.Held,
		Disk:      append([]byte(nil), r.Disk...),
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}

func snapshotFromRecord(r *snapshotRecord) *Snapshot {
	return &Snapshot{
		ID: r.ID, VMID: r.VMID, Disk: append([]byte(nil), r.Disk...),
		SizeMiB: r.SizeMiB, CreatedAt: r.CreatedAt,
	}
}

func opFromRecord(r *opRecord) *Operation {
	return &Operation{
		ID: r.ID, IdemKey: r.IdemKey, Type: OpType(r.Type), VMID: r.VMID,
		Status: OpStatus(r.Status), TargetSnap: r.TargetSnap,
		Error: r.Error, Result: r.Result, Seq: r.Seq,
		CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
	}
}

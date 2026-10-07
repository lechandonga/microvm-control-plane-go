package microvm

import (
	"fmt"
	"time"
)

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

// VMState 是虚拟机生命周期状态机中的状态。
type VMState string

const (
	StatePending      VMState = "pending"       // 已定义，模拟磁盘尚未创建
	StateCreating     VMState = "creating"      // 创建操作执行中
	StateStopped      VMState = "stopped"       // 已创建，未运行
	StateStarting     VMState = "starting"      // 启动操作执行中
	StateRunning      VMState = "running"       // 运行中
	StateStopping     VMState = "stopping"      // 停止操作执行中
	StateRestarting   VMState = "restarting"    // 重启操作执行中
	StateSnapshotting VMState = "snapshotting"  // 快照操作执行中
	StateRollingBack  VMState = "rolling_back"  // 回滚操作执行中
	StateDestroying   VMState = "destroying"    // 销毁操作执行中
	StateCreateFailed VMState = "create_failed" // 创建失败，可重试创建或销毁
	StateFailed       VMState = "failed"        // 启动/重启等操作失败，可重试或停止/销毁
	StateDestroyed    VMState = "destroyed"     // 已销毁（墓碑记录）
)

// IsTerminal 报告状态是否为终态。
func (s VMState) IsTerminal() bool { return s == StateDestroyed }

// OpType 标识异步操作的种类。
type OpType string

const (
	OpCreate   OpType = "create"
	OpStart    OpType = "start"
	OpStop     OpType = "stop"
	OpRestart  OpType = "restart"
	OpDestroy  OpType = "destroy"
	OpSnapshot OpType = "snapshot"
	OpRollback OpType = "rollback"
)

// OpStatus 是异步操作的生命周期状态。
type OpStatus string

const (
	OpPending   OpStatus = "pending" // 已受理，尚未执行（崩溃可能停在这里）
	OpRunning   OpStatus = "running" // 执行中
	OpSucceeded OpStatus = "succeeded"
	OpFailed    OpStatus = "failed" // 明确失败，可重新提交同类请求重试
)

// VMSpec 是虚拟机的定义参数，容量单位统一为兆字节（MiB）。
type VMSpec struct {
	ID        string
	Name      string
	VCPU      int    // vCPU 核数
	MemoryMiB int64  // 运行时内存
	DiskMiB   int64  // 磁盘逻辑大小（同时是初始占用）
	Host      string // 可选：指定宿主机；为空时由放置器选择
}

// VM 是虚拟机的持久化视图。
type VM struct {
	ID        string
	Name      string
	VCPU      int
	MemoryMiB int64
	DiskMiB   int64
	Host      string
	State     VMState

	// Held 表示运行时资源（CPU/内存）当前是否已在宿主机上占用。
	// Running 恒为 true；崩溃恢复时 running 状态会先经 replay 确认。
	Held bool

	// Disk 是模拟磁盘的当前内容（字节即数据），随事件一起持久化。
	Disk []byte

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Snapshot 是一台虚拟机某一时刻的完整磁盘镜像。
type Snapshot struct {
	ID        string
	VMID      string
	Disk      []byte
	SizeMiB   int64 // 计入宿主机存储的占用
	CreatedAt time.Time
}

// Operation 是一次异步操作的持久化记录。
type Operation struct {
	ID         string
	IdemKey    string // 客户端幂等键
	Type       OpType
	VMID       string
	Status     OpStatus
	TargetSnap string // snapshot/rollback 操作的目标快照
	Error      string
	Result     string // 成功时的结果说明（如新建快照 ID）
	Seq        int64
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// HostConfig 描述一台本地模拟宿主机的物理容量与超配比。
// 可分配容量 = 物理容量 * 超配比（向下取整）。存储超配比默认 1.0：
// 磁盘与快照是真实占用的字节，不允许超卖。
type HostConfig struct {
	Name               string
	CPUPhysical        int
	MemoryPhysicalMiB  int64
	StoragePhysicalMiB int64
	CPUOvercommit      float64
	MemoryOvercommit   float64
	StorageOvercommit  float64
}

// HostUsage 是某台宿主机任一时刻的账目快照。
type HostUsage struct {
	Name           string
	CPUCap         int
	MemoryCapMiB   int64
	StorageCapMiB  int64
	CPUUsed        int
	MemoryUsedMiB  int64
	StorageUsedMiB int64
	VMsRunning     int
}

// AvailableCPU 等字段便于调用方直接核对账目。
func (u HostUsage) AvailableCPU() int          { return u.CPUCap - u.CPUUsed }
func (u HostUsage) AvailableMemoryMiB() int64  { return u.MemoryCapMiB - u.MemoryUsedMiB }
func (u HostUsage) AvailableStorageMiB() int64 { return u.StorageCapMiB - u.StorageUsedMiB }

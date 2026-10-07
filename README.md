# microvm-control-plane-go

本地运行的微虚拟机（micro-VM）编排控制面，Go 实现，**内置模拟虚拟化后端**：
不依赖 KVM / QEMU / Firecracker / libvirt，不访问任何外部服务，不需要凭据。
所有状态都落盘在本地数据目录（fsync 的 WAL + checkpoint），进程崩溃重启后可恢复。

核心目标：在并发操作、进程崩溃重启、异常输入下，虚拟机状态、资源占用账目
和快照都得到**一致且可解释**的结果。

## 能力一览

- 生命周期：定义（Define）、启动（Start）、停止（Stop）、重启（Restart）、销毁（Destroy）。
- 严格状态机：只有合法迁移可提交，非法迁移 / 重复提交 / 目标不存在 / 参数非法
  全部被拒绝，并返回可区分的哨兵错误（见 `pkg/microvm/errors.go`）。
- 异步操作：Create / Start / Snapshot 等耗时流程返回操作 ID，进度和结果持久化；
  `Wait` 可等待终态。
- 幂等：每个请求可带 `IdempotencyKey`，同一 key 重复提交永远返回同一个操作 ID，
  多次重试与第一次结果一致。
- 串行保证：同一台 VM 同一时刻只有一个工作流在执行，不会出现改了一半的状态。
- 本地资源池：多台模拟宿主机，first-fit 放置；CPU/内存支持可配置超配比，
  磁盘绝不超卖；Stop/Destroy/删快照后容量完整归还。
- 快照与回滚：运行中或已停止的 VM 都可以打内容寻址的 copy-on-write 快照，
  回滚严格回到快照时间点，不会混入之后写入的数据；VM 之间完全隔离。
- 崩溃恢复：操作意图先落盘；重启时把所有非终态操作重放到确定结论（成功或
  显式失败可重试），不会卡在进行中，资源不会丢失或重复占用。

## 快速开始

```bash
go run ./cmd/mvctl            # 端到端演示：define→start→write→snapshot→stop→destroy
go test ./...                 # 普通单测
CGO_ENABLED=1 go test -race -v ./...   # 竞态检测 + 详细业务日志（推荐）
```

数据目录默认在 `./.mvctl-data`（`wal.log` + `checkpoint.json`），删除即重置。

## 作为库使用

```go
cp, _ := microvm.Open(microvm.Config{
    Dir: "./data",
    Hosts: []microvm.HostConfig{
        {ID: "h1", CPUCapacity: 4, MemCapacity: 4096, DiskCapacity: 10_000},
    },
    Overcommit:   microvm.OvercommitRatio{CPU: 2, Mem: 1},
    BackendDelay: 5 * time.Millisecond,
})
defer cp.Close()

opID, err := cp.Define(microvm.Spec{ID: "vm-1", VCPU: 2, Memory: 1024, Disk: 1000}, "idem-1")
op, err := cp.Wait(context.Background(), opID) // Succeeded / Failed + 可区分原因

opID, _ = cp.TakeSnapshot("vm-1", "snap-a", "idem-2")
cp.MustWait(opID)
// ... 写入新数据 ...
opID, _ = cp.Rollback("vm-1", "snap-a", "idem-3")
cp.MustWait(opID) // 磁盘内容精确回到 snap-a

for _, u := range cp.HostUsages() { // 可解释的容量账目
    fmt.Printf("%s cpu=%d/%d mem=%d/%d disk=%d/%d\n",
        u.ID, u.CPUUsed, u.CPUCapacity, u.MemUsed, u.MemCapacity, u.DiskUsed, u.DiskCapacity)
}
```

## 文档

- `docs/lifecycle.md`：生命周期状态机。
- `docs/async-and-recovery.md`：异步操作语义、WAL/checkpoint 与崩溃恢复。
- `docs/capacity.md`：容量核算、超配与放置规则。
- `docs/snapshots.md`：快照、COW 计费与时间点回滚规则。
- `docs/testing.md`：本地验证方法与用例覆盖矩阵。

## 代码结构

```
pkg/microvm/
  errors.go         可区分的哨兵错误原因
  types.go          VM/快照/宿主机/操作等领域类型
  statemachine.go   权威状态迁移表
  backend.go        模拟 hypervisor（延迟 + 可注入故障）
  store.go          fsync WAL + checkpoint，事件溯源
  views.go          只读视图与从权威状态重算的容量账目
  executor.go       各异步工作流的确定性执行/重放逻辑
  controlplane.go   提交校验、幂等、每 VM 串行、崩溃恢复
  api.go            类型化 API
cmd/mvctl           本地端到端演示
```

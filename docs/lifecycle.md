# 生命周期状态机

状态保存在 WAL 投影中（见 `async-and-recovery.md`），是权威状态；模拟后端的
“电源状态”只是运行时视图，重启时由持久化状态重新推导。

## 状态

| 状态 | 含义 | 是否持有 CPU/内存 | 是否持有磁盘预留 |
| --- | --- | --- | --- |
| `Pending` | 定义已记录，尚未放置到宿主机 | 否 | 否 |
| `Stopped` | 已放置、磁盘已预留、未运行 | 否 | 是（spec 声明值） |
| `Starting` | Start 工作流执行中（瞬态） | 尚未计入 | 是 |
| `Running` | 运行中 | 是 | 是 |
| `Stopping` | Stop 工作流执行中（瞬态） | 停止后释放 | 是 |
| `Restarting` | Restart 工作流执行中（瞬态） | 保持计入 | 是 |
| `Destroying` | Destroy 工作流执行中（瞬态） | 完成后释放 | 完成后释放 |
| `Destroyed` | 终态，记录随后被删除（查询返回 not found） | 否 | 否 |

## 合法迁移

权威迁移表在 `pkg/microvm/statemachine.go`：

```
Pending    --Define-->    Stopped          # Define 完成放置与磁盘预留
Pending    --Destroy-->   Destroying -> Destroyed
Stopped    --Start-->     Starting -> Running
Stopped    --Destroy-->   Destroying -> Destroyed
Running    --Stop-->      Stopping -> Stopped
Running    --Restart-->   Restarting -> Running
Running    --Destroy-->   Destroying -> Destroyed
```

- 快照（Snapshot）、回滚（Rollback）、删除快照（DeleteSnapshot）、写块（WriteBlock）
  只允许在稳定态 `Stopped` / `Running` 发起，且不改变电源状态。
- 任何不在迁移表中的组合返回 `ErrInvalidTransition`，错误消息同时给出
  “当前状态 + 被拒绝的操作”。
- 瞬态状态是并发互斥的核心：同一 VM 上第二个工作流在提交时就会收到
  `ErrDuplicateRequest`（另一操作进行中），不会排队产生交错状态。

## 拒绝原因（可区分）

| 哨兵错误 | 触发场景 |
| --- | --- |
| `ErrInvalidArgument` | 空 ID、VCPU/内存/磁盘非正数、缺快照 ID、写块参数非法 |
| `ErrAlreadyExists` | 定义一个已存在的 VM |
| `ErrVMNotFound` | 操作或查询目标不存在（含已销毁） |
| `ErrSnapshotNotFound` | 回滚/删除不存在的快照（提交时同步拒绝） |
| `ErrInvalidTransition` | 当前状态不允许该操作 |
| `ErrDuplicateRequest` | 同一 VM 已有进行中的操作 |
| `ErrCapacityExceeded` | 放置无磁盘空间，或启动超过 CPU/内存（含超配）额度 |
| `ErrBackendFailed` | 模拟 hypervisor 注入的故障 |
| `ErrOperationFailed` | 异步操作终态为失败（包装上述具体原因，可用 errors.Is 解包） |

## 失败后的状态补偿

后端故障会让工作流在**状态改变前**或**改变后**失败。失败时控制面会把 VM
补偿回合法稳定态，保证立即可重试、账目不脏：

- Start 在 `Starting` 失败 → 回到 `Stopped`，不占 CPU/内存。
- Stop 在 `Stopping` 失败 → 回到 `Running`，资源不被误释放。
- Restart 在 `Restarting` 失败 → 回到 `Running`。
- Destroy 在 `Destroying` 失败 → 回到 `Stopped`，可重新 Start 或再次 Destroy。
- Define 在放置阶段失败 → 留在 `Pending`，同规格重试即可恢复。

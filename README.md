# 微虚拟机编排控制面（Go，本地模拟后端）

一个完全本地运行的微虚拟机（microVM）编排控制面原型。它**不依赖**
KVM、QEMU、libvirt，也不依赖任何外部服务或凭据：虚拟化后端由进程内的
模拟器扮演，状态全部持久化在本地目录的 WAL + checkpoint 中。

目标是让虚拟机的生命周期、资源容量账目与快照在**并发操作、进程崩溃重启、
异常输入**下都得到一致且可解释的结果。

- 纯 Go 标准库，零第三方依赖
- 事件溯源持久化（`state.wal` + `state.cp`，每条事务 `fsync`）
- 异步操作（创建/启动/快照等）带进度与终态记录，崩溃后确定性重放
- CPU / 内存 / 存储三维容量核算，可配置超配比，放置不超卖
- 时间点一致的快照与回滚，按 VM 严格隔离
- 全部语义都有单测覆盖，并可在竞态检测（`-race`）下通过

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `types.go` | 状态、操作、快照、宿主机等领域模型与错误分类 |
| `errors.go` | 可区分的业务错误类型（`KindOf(err)`） |
| `store.go` | WAL + checkpoint 事件存储，原子事务与崩溃重放 |
| `backend.go` | 模拟虚拟化后端：可配步骤延迟、一次性故障注入、步骤钩子 |
| `controller.go` | 控制面入口：开关、查询、参数校验、放置、容量账目 |
| `lifecycle.go` | define/start/stop/restart/destroy/snapshot/rollback/读写盘 |
| `executor.go` | 异步操作执行器：幂等步骤、完成事务、失败收敛 |
| `recover.go` | 打开控制面时的崩溃恢复 |
| `cmd/mvctl` | 最小本地演示 CLI |
| `*_test.go` | 七大业务域的测试用例 |

## 快速验证

```bash
# 普通测试（日志打印业务覆盖地图 + 每个子用例的名称与 PASS/FAIL）
go test -v ./...

# 竞态检测（并发用例必须在该模式下通过；需要 CGO，机器上有 gcc 即可）
CGO_ENABLED=1 go test -race -v ./...

# 本地演示：定义 → 启动 → 停止，并打印宿主机账目
go run ./cmd/mvctl
```

不需要 root，不需要任何系统虚拟化组件，临时数据目录位于
`${TMPDIR}/mvctl-data`（删除即可清空）。

## 生命周期状态机

```text
                 define
                   │
                   ▼
               pending ──create──► creating ──成功──► stopped
                   ▲                   │
                   │                失败│
                   │                   ▼
   （销毁后同名 ID 可重新定义）    create_failed ──新 create(重试)──► creating
                                       │
                                       └──destroy──► destroying ──► destroyed(终态)

 stopped ──start──► starting ──成功──► running ──stop──► stopping ──► stopped
   ▲                  │失败             │  ▲                  ▲
   │                  ▼                 │  │                  │
   └──start 失败回到 stopped         restart 成功            stop 成功
                                      ▼
                                 restarting ──成功──► running

 running/stopped ──snapshot──► snapshotting ──完成──► 回到原状态(running/stopped)
 running/stopped ──rollback──► rolling_back ──完成──► 回到原状态(运行态保持运行)
 任何非终态 ──destroy──► destroying ──成功──► destroyed（级联删除全部快照）
```

非法迁移被拒绝，错误带明确分类（`errors.go`）：

- `invalid_request`：参数非法（空 ID、容量非正数、空幂等键等）
- `not_found`：VM、宿主机或操作不存在；已销毁 VM 等同不存在
- `already_exists`：重复定义未销毁的同名 VM
- `invalid_state`：当前状态不允许该操作
- `duplicate`：同一幂等键被用于不同的请求（不同 VM 或不同操作类型）
- `operation_active`：同一台 VM 已有进行中的异步操作
- `capacity`：放置或启动会导致超卖
- `snapshot_missing`：回滚/删除的快照不存在
- `failed`：模拟后端执行失败（明确失败，可以用新幂等键重试）
- `closed`：控制面已关闭

### 操作互斥与并发

- 每台 VM 同一时刻最多有一个生命周期操作。判定的事实来源是持久化的
  **该 VM 最新一条操作记录**（非终态即忙），因此不存在“内存标记滞后
  导致永远报忙”的窗口。
- “读取-校验-构造事件-提交”在控制面串行化执行，状态迁移与操作记录
  在同一个 WAL 事务里落盘，不会留下改了一半的状态。
- 数据面读写（`WriteData`）只在 `running` 接受；快照/回滚进行中
  （中间态）写入会被 `invalid_state` 拒绝，因此不可能污染快照时点。

### 幂等语义

- 每个写请求必须带客户端幂等键。相同（VM + 操作类型 + 幂等键）的
  重试返回**第一次的操作记录与最终结论**，无论操作处于进行中还是已终态。
- 同一幂等键用于别的 VM 或别的操作类型，返回 `duplicate`。
- 操作失败后，用**新的幂等键**可以重新提交同类请求完成恢复。

## 异步操作与重启恢复语义

创建、启动、停止、重启、销毁、快照、回滚都是异步操作：

1. 请求通过校验后，操作记录先以 `pending` 落盘（同时把 VM 状态迁移到
   对应中间态，需要预留的容量在同一事务预留）。
2. 执行器把状态推进为 `running`，然后调用模拟后端跑若干**幂等步骤**。
   模拟后端不维护任何资源账目，真正的账目只由 WAL 事件决定。
3. 最后一个原子事务写入：操作终态（`succeeded`/`failed`）+ VM 目标状态
   + 快照的新增/删除。

进程崩溃后重新 `Open`：

- 所有停在 `pending`/`running` 的操作被重新投递，从头重放幂等步骤：
  - 成功路径补到 `succeeded`（如创建/启动/快照进行中崩溃 → 恢复后成功）；
  - 恢复期间若注入了故障，则落到明确的 `failed`，不会永远卡在进行中；
- 资源不会重复占用：CPU/内存只在“进入 starting”的事务里预留一次，
  在 stop/destroy 成功事务里释放；快照空间只在快照受理事务里计入一次，
  快照失败时随完成事务回收；
- WAL 最后一行写坏（掉电撕裂）会在重放时被检测到并报错，避免把半截
  数据当成新状态（已 `fsync` 的完整行全部保留）。

模拟后端支持：

- `Backend().SetStepDelay(d)`：每步耗时，用于制造并发/崩溃窗口；
- `Backend().FailOnce(vmID, op, step)`：让下一次指定操作在某一步失败一次；
- `Backend().SetHook(fn)`：每步开始前回调，测试用它在任意步骤模拟崩溃。

`Controller.SimulateCrash()` 取消执行上下文并直接关闭存储（不做优雅
收尾），随后用同一目录 `Open` 即可观测恢复。

## 容量核算与放置规则

宿主机可配置物理容量与三个独立超配比（默认存储 1.0，即不超卖）：

```
可分配 = floor(物理容量 × 超配比)
```

账目（`HostUsage`）每次都由持久化状态**即时求和**，不做增量累加，
因此不会漂移；恒有 `used + available = capacity`，且任意时刻
`used <= capacity`。

- **存储（按 MiB 向上取整，永不低估）**
  - 定义 VM 时即预留 `diskMiB`，创建失败仍保留（VM 处于 create_failed，
    可重试创建或销毁），`destroy` 成功后随磁盘一起释放；
  - 写盘增长（`WriteData`）在 running 下实时校验余量；
  - 每张快照按其镜像大小额外计入所在宿主机，删除快照完整回收；
  - 存储超配比默认/建议 1.0：磁盘与快照是真实字节，不允许超卖。
- **CPU / 内存**
  - 只在运行时占用：进入 `starting` 的事务原子预留，`stop`/`destroy`
    成功后释放；`restart` 期间不释放也不重复申请；
  - 启动预留时再次核算容量，因此并发启动在串行提交点被容量拒绝，
    绝不超卖。
- **放置**：可在 `VMSpec.Host` 指定宿主机；否则按宿主机名字序选第一个
  能容纳**存储**的节点（first-fit）。定义阶段只按存储放置，运行时容量
  在启动点核算。

## 快照与回滚规则

- running 与 stopped 的 VM 都可以打快照、回滚；回滚后保持回滚前的
  运行状态（running 回滚后仍 running，stopped 同理）。
- **时间点一致**：快照镜像在快照请求**被受理的那个事务**里就从 VM 磁盘
  完整复制并预留空间。快照操作进行中写入磁盘会被状态机拒绝；即使快照
  在中途崩溃，恢复后看到的也是受理时点的镜像。
- 回滚把活动磁盘**整体替换**为快照镜像，回滚之后才写入的字节不可能残留。
- 快照按 VM 隔离：一台 VM 的快照、回滚、删除不影响其它 VM；快照列表、
  容量账目都以 VM→宿主机的归属独立核算。
- 删除快照立即释放空间；销毁 VM 时其全部快照级联删除并一并回收。
- 回滚/删除不存在的快照返回 `snapshot_missing`。

## 数据格式（本地目录）

- `state.wal`：JSON Lines 追加日志，每行一个带单调 `seq` 的事件
  （宿主机/VM/快照/操作的完整快照 upsert，或快照删除）；每批 `fsync`。
- `state.cp`：checkpoint（含 `nextSeq` 与投影后的全量状态）。
  累积 256 条事件后，在无后台并发写的安全点（打开恢复时、优雅关闭时）
  做一次“写临时文件 → fsync → 原子 rename → 截断 WAL”的压缩。

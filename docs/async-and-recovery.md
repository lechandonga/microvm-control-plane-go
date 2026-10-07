# 异步操作与重启恢复

## 异步流程

`Submit` 做完全部**同步校验**（参数、目标存在性、状态机、快照存在性、
同 VM 是否忙碌），随后：

1. 构造操作记录（`Pending`），把**意图**作为事件 fsync 落盘（`op_upsert`）。
2. 如果带了幂等键，再落盘 `idempotency` 映射。
3. 置该 VM 的“忙碌”标记，启动一个 worker 执行工作流：`Running` → 终态。
4. `Wait(opID)` 轮询持久化状态，终态为 `Succeeded` 或 `Failed`；
   `Failed` 的错误用 `errors.Is` 可解包出具体原因（`ErrBackendFailed` 等）。

校验错误不会创建操作记录；容量准入（放置磁盘、启动 CPU/内存）在工作流内执行，
因此可能表现为同步拒绝或操作终态失败，两种形式原因相同。

## 每 VM 串行

- 内存中的忙碌表保证同一 VM 同时只有一个 worker；冲突提交立即得到
  `ErrDuplicateRequest`。
- worker 失败时：先做状态补偿并把 `Failed` 落盘（仍持有串行槽），再释放槽；
  成功时：先释放槽再发布 `Succeeded`。因此 `Wait` 返回成功后，后续提交
  绝不会看到“VM 还忙”。

## 幂等

- 每个请求可带 `IdempotencyKey`；同 key 重提直接返回**第一次的操作 ID**，
  无论第一次仍在运行还是已有终态。效果：重试 N 次 == 第一次。
- 工作流本身也按事件幂等：重放时根据持久化状态判断（例如 VM 已 Running 时
  Start 直接成功；快照 ID 已存在时 Snapshot 直接成功；VM 已不存在时
  Destroy 直接成功）。

## 持久化模型

- 数据目录包含：
  - `wal.log`：append-only、JSON 行、每条 fsync；
  - `checkpoint.json`：周期性全量快照 + WAL 序号，原子 rename 落盘并 fsync 目录。
- 所有状态变化都是事件（`vm_defined / vm_placed / vm_state / block_write /
  snap_created / rolled_back / snap_deleted / vm_destroyed / op_upsert /
  idempotency`）。内存投影**只在事件 fsync 成功之后**前进，崩溃不可能暴露
  日志里没有的状态。
- 放置（`PlaceAtomically`）在同一把存储锁内完成“重算账本 → 容量判定 →
  落盘 placed 事件 → 前进投影”，并发 Define 不可能超卖。

## 崩溃恢复（确定性结论）

重启时 `Open`：

1. 载入 checkpoint，重放其后的 WAL（按序号去重），重建完整投影。
2. 按 VM 创建顺序、同 VM 按操作时间排序，找出所有非终态操作（`Pending/Running`）。
3. 逐个重新执行：执行器从持久化状态推导该做什么（见上文幂等），最终每个操作
   都落到**确定结论**：要么重放成功（`Succeeded`），要么显式失败（`Failed`，
   带原因，可立即重试）。不会有操作永远卡在进行中。
4. 模拟后端的运行时电源状态由恢复后的 VM 状态重新推导。

关键场景：

- 崩溃在“意图落盘之前”：重启后看不到该操作，客户端重试等同首次提交。
- 崩溃在 Define 的 placed 事件：事件要么完全没发生（重试放置，账目只增一次），
  要么已发生（重放直接成功，磁盘只预留一次）。
- 崩溃在 Start 发布 Running 前：恢复时从 `Starting` 继续，最终 Running，
  CPU/内存恰好计入一次。
- 崩溃在 Snapshot 事件前后：快照要么完整存在、要么不存在，不会有半个快照；
  之后回滚仍精确。

测试通过 `Config.CrashHook` 在“事件 fsync 前”确定性地杀掉进程模型来模拟
真实崩溃（见 `pkg/microvm/recovery_test.go`）。

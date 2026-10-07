# 本地验证方法

全部用例只使用内置模拟后端，**不依赖 KVM/QEMU/libvirt、外部服务或凭据**。

## 运行

```bash
# 推荐：竞态检测 + 详细日志（能看到覆盖了哪些业务、各业务用例与结果）
CGO_ENABLED=1 go test -race -v ./...

# 普通模式
go test ./...

# 覆盖率
CGO_ENABLED=1 go test -race -coverprofile=cover.out ./...
go tool cover -func=cover.out
```

日志形如：

```
=== RUN   TestLifecycleHappyPath
    lifecycle_test.go:11: business=lifecycle case=happy_path: legal transitions and final destroy
    lifecycle_test.go:49: business=lifecycle case=happy_path: PASS
```

`business=` 标明业务域，`case=` 标明场景，结果以 `PASS/FAIL` 收尾。

## 用例覆盖矩阵

| 业务域 | 文件 | 用例 | 覆盖点 |
| --- | --- | --- | --- |
| lifecycle | `lifecycle_test.go` | `happy_path` | define→start→restart→stop→destroy 全链路与各中间状态 |
| lifecycle | 同上 | `illegal_transitions` | Stopped 禁止 stop/restart、Running 禁止 start、销毁后目标不存在 |
| lifecycle | 同上 | `validation_and_missing` | 空 ID/非法规格、重复定义、未知 VM、未知快照 |
| lifecycle | 同上 | `idempotent_retry` | 同幂等键重复 Define/Start 收敛为同一操作、同一结果 |
| capacity | `capacity_test.go` | `placement_and_release` | first-fit、停止释放 CPU/内存、启动准入超卖拒绝、放置磁盘超卖拒绝、Destroy 全量归还 |
| capacity | 同上 | `overcommit` | CPU/内存超配比生效、磁盘仍物理封顶 |
| capacity | 同上 | `snapshot_space` | COW 增量计入、删快照完整回收 |
| concurrency | `concurrency_test.go` | `same_vm_serialized` | 20 个并发混合请求：忙碌冲突被拒、瞬态不残留、账目不破 |
| concurrency | 同上 | `parallel_define_admission` | 12 并发 Define，恰好 admitted=容量上限，无超卖无泄漏 |
| concurrency | 同上 | `snapshot_vs_lifecycle` | 快照只接受稳定态；邻居 VM 的写入不改变本 VM 块 |
| recovery | `recovery_test.go` | `crash_mid_define` | placed 事件崩溃：重放成功，磁盘恰好预留一次 |
| recovery | 同上 | `crash_mid_start` | Starting 崩溃：重放到 Running，CPU/内存计一次，运行时视图重建 |
| recovery | 同上 | `crash_mid_snapshot` | snap_created 崩溃：重放后恰好一个快照，磁盘未损坏，仍可回滚 |
| recovery | 同上 | `crash_before_intent` | 意图落盘前崩溃：重试等同首次提交 |
| recovery | 同上 | `checkpoint_then_restart` | checkpoint 压缩 + 后续 WAL 重放，状态/快照/账目一致 |
| snapshot | `snapshot_test.go` | `rollback_point_in_time` | 覆盖/新增块回滚后精确还原，后写数据不混入 |
| snapshot | 同上 | `rollback_running_isolated` | 运行中回滚保持 Running，邻居 VM 不受影响 |
| snapshot | 同上 | `chains_and_delete` | 多快照反复跳转、删快照后其余恢复点仍有效 |
| fault | `fault_test.go` | `backend_fail_then_retry` | 注入一次后端故障：Failed + ErrBackendFailed，VM 可重试成功 |
| fault | 同上 | `failed_op_no_resources` | 失败的 Start/Snapshot 不留 CPU/内存/快照账目 |

## 崩溃注入原理

`Config.CrashHook(ev) bool` 在**每条事件 fsync 之前**被调用；返回 true 时
存储层关闭文件并 panic，进程模型立即终止（worker 不会继续执行），事件
不落盘。测试随后用同一目录重新 `Open`，验证恢复结论。这精确对应真实系统
“进程在写 WAL 中途被 kill -9 / 掉电”的语义。

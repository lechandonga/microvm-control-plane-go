package microvm

import (
	"fmt"
	"os"
	"testing"
)

// TestMain 在测试开始前打印本套件覆盖的业务地图，配合 go test -v
// 的子测试名与 PASS/FAIL 行即可看清每个业务的用例与结果。
func TestMain(m *testing.M) {
	fmt.Println("==== 微虚拟机控制面 本地模拟后端 测试套件 ====")
	fmt.Println("覆盖业务：")
	fmt.Println("  [生命周期] TestStateMachine        状态机合法路径/非法迁移/参数校验/重复定义")
	fmt.Println("  [幂等重试] TestIdempotency         同键重试一致/跨类型拒绝/失败后可重试")
	fmt.Println("  [容量放置] TestCapacity            超配比/放置防超卖/启动占用/停止销毁快照释放")
	fmt.Println("  [快照回滚] TestSnapshotConsistency 时间点一致/停止态快照/跨 VM 隔离/缺快照拒绝")
	fmt.Println("  [并发竞争] TestConcurrency         同机串行/并发启动不超卖/快照与生命周期交错 (-race)")
	fmt.Println("  [崩溃恢复] TestRecovery            创建/启动/快照中途崩溃重放/故障明确失败/撕裂 WAL")
	fmt.Println("  [持久压缩] TestStoreCompaction     WAL checkpoint 截断与压缩后重启一致性")
	fmt.Println("----------------------------------------------")
	os.Exit(m.Run())
}

package microvm

import "errors"

// Kind 对控制面返回的业务错误进行分类，调用方可以用 errors.Is
// 判断请求被拒绝的具体原因，而不必依赖错误文案。
type Kind string

const (
	KindInvalidRequest  Kind = "invalid_request"  // 参数非法
	KindNotFound        Kind = "not_found"        // 目标不存在
	KindAlreadyExists   Kind = "already_exists"   // 重复定义
	KindInvalidState    Kind = "invalid_state"    // 非法状态迁移
	KindDuplicate       Kind = "duplicate"        // 重复提交（同幂等键）
	KindOperationActive Kind = "operation_active" // 同一 VM 已有进行中的操作
	KindCapacity        Kind = "capacity"         // 宿主机容量不足
	KindSnapshotMissing Kind = "snapshot_missing" // 快照不存在
	KindFailed          Kind = "failed"           // 模拟后端执行失败（可重试）
	KindClosed          Kind = "closed"           // 控制面已关闭
)

// Error 是控制面返回的可分类错误。
type Error struct {
	Kind Kind
	Msg  string
}

func (e *Error) Error() string { return string(e.Kind) + ": " + e.Msg }

func newError(kind Kind, format string, args ...any) error {
	return &Error{Kind: kind, Msg: sprintf(format, args...)}
}

// KindOf 返回错误对应的分类；非控制面错误返回空串。
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return ""
}

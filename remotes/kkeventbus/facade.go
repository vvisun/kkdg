// Package kkeventbus 提供发布/订阅式事件总线：本地（buslocal）与 NATS（busnats）两种实现共用
// [MessageRegistry]、[EncodeMessage]、[DecodeMessage] 与 internal 线格式，保证进程内与跨进程载荷语义一致。
//
// # MessageRegistry 与 Register
//
// 使用总线前须构造 [NewMessageRegistry]（codec 可为 nil，将使用 JSON），并对每个业务 topic 注册一种消息类型：
//
//	reg := kkeventbus.NewMessageRegistry(nil)
//	if err := reg.Register("user.login", &UserLoginEvent{}); err != nil { ... }
//
// 注册规则（与 [MessageRegistry.Register] 一致）：
//   - 同一 topic 只能对应一种消息类型；若该 topic 已绑定类型，再次 Register 时传入不同类型，返回 [ErrRegisterDuplicateTopic]。
//   - 允许不同 topic 绑定同一 Go 类型（例如多个 subject 共用同一种结构体）。
//   - [EncodeMessage] 按 [IEventBus.Publish] 传入值的反射类型查表，注册时示例与发布时须一致（例如均为 *T 或均为 T）。
//   - [IEventBus.Publish] 的 topic 参数写入线信封，订阅端按信封中的 topic 做 [DecodeMessage]。
//
// # 构造总线
//
// NATS（registry 必填，不可为 nil）：
//
//	eb, err := busnats.NewEventbus(reg, busnats.WithUrl("nats://127.0.0.1:4222"))
//
// 本地（registry 必填）：
//
//	eb, err := buslocal.NewEventbus(reg)
//
// 建议将 *busnats.Eventbus / *buslocal.Eventbus 挂在应用级（例如 kkapp 的 SetExtData），在关闭应用时调用 [IEventBus.Close]；
// busnats 使用内置连接时 Close 会退订并断开 NATS，外部传入的 conn 仅退订，由调用方关闭连接。
//
// # 发布与订阅
//
// [IEventBus.Publish] 的 message 须为已在 registry 中注册过的类型实例；handler 中 [Event.Payload] 为解码后的 any，可按类型断言。
// buslocal、busnats 均未使用 context 参数，预留给其它实现。
package kkeventbus

import (
	"context"
	"time"
)

type Event struct {
	ID        string    `json:"id"`        // 事件ID
	Topic     string    `json:"topic"`     // 事件主题
	Payload   any       `json:"payload"`   // 事件载荷
	Timestamp time.Time `json:"timestamp"` // 事件时间
}

type EventHandler func(event *Event)

// IEventBus 事件总线接口
//
//	注意：buslocal和busnats中均未使用ctx参数，用于预留以兼容其他扩展实现（例如redis等）
type IEventBus interface {
	// Close 关闭事件总线
	Close() error
	// Publish 发布事件
	Publish(ctx context.Context, topic string, message any) error
	// Subscribe 订阅事件
	Subscribe(ctx context.Context, topic string, handler EventHandler) (uint64, error)
	// Unsubscribe 取消订阅
	Unsubscribe(ctx context.Context, topic string, handler EventHandler) error
	// UnsubscribeByID 根据ID取消订阅
	UnsubscribeByID(ctx context.Context, topic string, id uint64) error
	// UnsubscribeAll 取消所有订阅
	UnsubscribeAll(ctx context.Context) error
}

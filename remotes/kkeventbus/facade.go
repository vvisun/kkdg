// Package kkeventbus 提供事件总线功能
//
// 事件总线是一种发布/订阅模式的消息系统，用于在不同组件之间传递事件。
// 它提供了一种简单的方式来实现组件之间的解耦，并且可以用于实现各种模式，例如事件驱动架构、微服务架构等。
//
// 事件总线通常由以下几个部分组成：
// - 事件：事件是事件总线中的基本单位，它包含事件的类型、数据和时间戳等信息。
// - 事件处理器：事件处理器是事件总线中的处理单元，它负责处理事件并执行相应的操作。
// - 事件总线：事件总线是事件总线中的核心组件，它负责管理事件的发布和订阅，并且提供事件的存储和检索功能。
//
// buslocal 是本地事件总线实现，它使用本地内存来存储事件，适用于单机部署。
// busnats 是 NATS 事件总线实现，它使用 NATS 作为事件总线，适用于分布式部署。
//
// 使用事件总线时，需要先创建一个事件总线实例，然后使用事件总线实例来发布和订阅事件。
// 事件总线实例可以通过 SetEventbus 方法设置为全局事件总线，然后可以通过 GetEventbus 方法获取全局事件总线。
//
// 事件总线实例可以通过 Close 方法关闭，关闭后事件总线实例将不再接收和发送事件。
package kkeventbus

import (
	"context"
	"sync"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xvalue"
)

type Event struct {
	ID        string       // 事件ID
	Topic     string       // 事件主题
	Payload   xvalue.Value // 事件载荷
	Timestamp time.Time    // 事件时间
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

//----------------------------------------------------------------------
//
// 全局总线约定：在任意 goroutine 调用 Publish / Subscribe 等之前，须由初始化路径完成一次 SetEventbus。
// 读路径（GetEventbus、包级转发）不加锁，以避免热路径开销；请勿在运行期并发再次 SetEventbus。

var (
	globalEventbus   IEventBus
	globalEventbusMu sync.Mutex //仅保护 SetEventbus 的「只设一次」
)

// SetEventbus 设置事件总线，一般在初始化阶段设置一次。只允许设置一次。
func SetEventbus(eb IEventBus) {
	if eb == nil {
		kklog.Warn("cannot set a nil eventbus")
		return
	}

	globalEventbusMu.Lock()
	defer globalEventbusMu.Unlock()

	if globalEventbus != nil {
		kklog.Errorf("global eventbus already set")
		return
	}

	globalEventbus = eb
}

// GetEventbus 获取事件总线（无锁；须满足上述初始化约定）。
func GetEventbus() IEventBus {
	return globalEventbus
}

// Publish 发布事件
func Publish(ctx context.Context, topic string, message any) error {
	if globalEventbus == nil {
		return kkerrors.ErrMissingEventbusInstance
	}

	return globalEventbus.Publish(ctx, topic, message)
}

// Subscribe 订阅事件
func Subscribe(ctx context.Context, topic string, handler EventHandler) (uint64, error) {
	if globalEventbus == nil {
		return 0, kkerrors.ErrMissingEventbusInstance
	}

	return globalEventbus.Subscribe(ctx, topic, handler)
}

// Unsubscribe 取消订阅
func Unsubscribe(ctx context.Context, topic string, handler EventHandler) error {
	if globalEventbus == nil {
		return kkerrors.ErrMissingEventbusInstance
	}

	return globalEventbus.Unsubscribe(ctx, topic, handler)
}

// UnsubscribeByID 根据 Subscribe 返回的 id 取消订阅
func UnsubscribeByID(ctx context.Context, topic string, id uint64) error {
	if globalEventbus == nil {
		return kkerrors.ErrMissingEventbusInstance
	}

	return globalEventbus.UnsubscribeByID(ctx, topic, id)
}

// UnsubscribeAll 取消所有订阅
func UnsubscribeAll(ctx context.Context) error {
	if globalEventbus == nil {
		return kkerrors.ErrMissingEventbusInstance
	}

	return globalEventbus.UnsubscribeAll(ctx)
}

// Close 关闭事件总线
func Close() error {
	if globalEventbus == nil {
		return nil
	}

	return globalEventbus.Close()
}

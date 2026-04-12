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
// 如何使用：
// 1. 项目初始化时，创建一个事件总线实例，并设置为全局事件总线，建议为app级。
// eg:
//
//	type AppExtraData struct {
//		NatsBus *busnats.Eventbus
//	}
//
//	natsBus, err := busnats.NewEventbus(busnats.WithUrl("nats://127.0.0.1:4222"))
//	if err != nil {
//		return err
//	}
//	app.SetExtData(&AppExtraData{NatsBus: natsBus})
//
// 2. 在需要使用事件总线的地方，获取事件总线实例。
// eg:
//
//	appExtraData := app.GetExtData().(*AppExtraData)
//	appExtraData.NatsBus.Publish(context.Background(), "test", "hello")
//
// 3. 在需要订阅事件的地方，订阅事件。
// eg:
//
//	appExtraData.NatsBus.Subscribe(context.Background(), "test", func(event *Event) {
//		fmt.Println(event.Payload)
//	})
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

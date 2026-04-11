package kkeventbus

import (
	"context"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
	xvalue "github.com/vvisun/kkdg/utils/value"
)

type Event struct {
	ID        string       // 事件ID
	Topic     string       // 事件主题
	Payload   xvalue.Value // 事件载荷
	Timestamp time.Time    // 事件时间
}

type EventHandler func(event *Event)

const EventTopicPrefix = "kkbus."

// IEventBus 事件总线接口
//
//	注意：buslocal和busnats中均未使用ctx参数，用于预留以兼容其他扩展实现（例如redis等）
type IEventBus interface {
	// Close 关闭事件总线
	Close() error
	// Publish 发布事件
	Publish(ctx context.Context, topic string, message any) error
	// Subscribe 订阅事件
	Subscribe(ctx context.Context, topic string, handler EventHandler) error
	// Unsubscribe 取消订阅
	Unsubscribe(ctx context.Context, topic string, handler EventHandler) error
}

//----------------------------------------------------------------------

var globalEventbus IEventBus

// SetEventbus 设置事件总线
func SetEventbus(eb IEventBus) {
	if eb == nil {
		kklog.Warn("cannot set a nil eventbus")
		return
	}

	if globalEventbus != nil {
		if err := globalEventbus.Close(); err != nil {
			kklog.Errorf("the old eventbus close failed: %v", err)
		}
	}

	globalEventbus = eb
}

// GetEventbus 获取事件总线
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
func Subscribe(ctx context.Context, topic string, handler EventHandler) error {
	if globalEventbus == nil {
		return kkerrors.ErrMissingEventbusInstance
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

// Close 关闭事件总线
func Close() error {
	if globalEventbus == nil {
		return nil
	}

	return globalEventbus.Close()
}

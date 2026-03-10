package kkactor

import (
	"fmt"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkerrors"
)

// IActorFramework 是 Actor 框架的接口，负责管理 Actor 的创建、销毁、消息路由等。
type IActorFramework interface {
	GetLocator() *ActorLocator
	GetActorSystem() *actor.ActorSystem
	Send(target AlphaActorID, msg any) error
	Request(target AlphaActorID, msg any, timeout time.Duration) (any, error)
	RequestAsync(target AlphaActorID, msg any, timeout time.Duration, callback func(result any, err error)) error
}

var (
	globalActorFramework *ActorFramework
	onceActorFramework   sync.Once
)

// 获取全局Actor框架, 线上一般用全局即可，避免混乱。
func GetGlobalActorFramework() *ActorFramework {
	onceActorFramework.Do(func() {
		globalActorFramework = NewActorFramework(NewActorLocator(), actor.NewActorSystem())
	})
	return globalActorFramework
}

// ActorFramework 是 Actor 框架的核心组件，负责管理 Actor 的创建、销毁、消息路由等。
type ActorFramework struct {
	locator  *ActorLocator
	actorSys *actor.ActorSystem
}

func NewActorFramework(locator *ActorLocator, actorSys *actor.ActorSystem) *ActorFramework {
	if locator == nil {
		panic("locator is nil")
	}
	if actorSys == nil {
		panic("actorSys is nil")
	}
	return &ActorFramework{
		locator:  locator,
		actorSys: actorSys,
	}
}

func (slf *ActorFramework) GetLocator() *ActorLocator {
	return slf.locator
}

func (slf *ActorFramework) GetActorSystem() *actor.ActorSystem {
	return slf.actorSys
}

// 向指定actor发送消息
func (slf *ActorFramework) Send(target AlphaActorID, msg any) error {
	pid := slf.locator.GetActor(target)
	if pid == nil {
		return kkerrors.ErrActorNotFound
	}
	slf.actorSys.Root.Send(pid, msg)
	return nil
}

// 同步向指定actor发送消息, 等待响应
func (slf *ActorFramework) Request(target AlphaActorID, msg any, timeout time.Duration) (any, error) {
	pid := slf.locator.GetActor(target)
	if pid == nil {
		return nil, kkerrors.ErrActorNotFound
	}
	future := slf.actorSys.Root.RequestFuture(pid, msg, timeout)
	result, err := future.Result()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// 异步向指定actor发送消息, 不等待响应
func (slf *ActorFramework) RequestAsync(target AlphaActorID, msg any, timeout time.Duration, callback func(result any, err error)) error {
	pid := slf.locator.GetActor(target)
	if pid == nil {
		return kkerrors.ErrActorNotFound
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				callback(nil, fmt.Errorf("panic: %v", r))
			}
		}()
		result, err := slf.Request(target, msg, timeout)
		callback(result, err)
	}()
	return nil
}

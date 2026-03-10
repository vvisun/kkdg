package kkactor

import (
	"fmt"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kkerrors"
)

// IActorFramework 是 Actor 框架的接口，负责管理 Actor 的创建、销毁、消息路由等。
type IActorFramework interface {
	GetLocator() *ActorLocator
	GetActorSystem() *actor.ActorSystem
	SetRemoteTransport(transport actorremotes.IRemoteActorTransport) error
	GetRemoteTransport() actorremotes.IRemoteActorTransport
	Send(target LucencyActorID, msg any) error
	Request(target LucencyActorID, msg any, timeout time.Duration) (any, error)
	RequestAsync(target LucencyActorID, msg any, timeout time.Duration, callback func(result any, err error)) error
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

func NewActorSystem(options ...actor.ConfigOption) *actor.ActorSystem {
	return actor.NewActorSystem(options...)
}

//-------------------------------------------------------------------------

// ActorFramework 是 Actor 框架的核心组件。
type ActorFramework struct {
	locator         *ActorLocator                    // Actor寻址系统
	actorSys        *actor.ActorSystem               // Actor系统
	remoteTransport actorremotes.IRemoteActorTransport
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

func (slf *ActorFramework) SetRemoteTransport(transport actorremotes.IRemoteActorTransport) error {
	if slf.remoteTransport != nil {
		_ = slf.remoteTransport.Close()
	}
	slf.remoteTransport = transport
	if transport == nil {
		return nil
	}
	transport.SetReceiver(slf)
	return transport.Start()
}

func (slf *ActorFramework) GetRemoteTransport() actorremotes.IRemoteActorTransport {
	return slf.remoteTransport
}

// 向指定actor发送消息
func (slf *ActorFramework) Send(target LucencyActorID, msg any) error {
	isLocal, err := slf.locator.IsLocalActor(target)
	if err != nil {
		return err
	}
	if isLocal {
		pid, err := slf.locator.GetActor(target)
		if err != nil {
			return err
		}
		slf.actorSys.Root.Send(pid, msg)
		return nil
	}
	if slf.remoteTransport == nil {
		return kkerrors.ErrActorRemoteTransportNotConfigured
	}
	return slf.remoteTransport.Send(actorremotes.ActorRef{
		NodeID:   target.NodeID(),
		ActorKey: target.ActorKey(),
	}, msg)
}

// 同步向指定actor发送消息, 等待响应
func (slf *ActorFramework) Request(target LucencyActorID, msg any, timeout time.Duration) (any, error) {
	isLocal, err := slf.locator.IsLocalActor(target)
	if err != nil {
		return nil, err
	}
	if isLocal {
		pid, err := slf.locator.GetActor(target)
		if err != nil {
			return nil, err
		}
		future := slf.actorSys.Root.RequestFuture(pid, msg, timeout)
		result, err := future.Result()
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	if slf.remoteTransport == nil {
		return nil, kkerrors.ErrActorRemoteTransportNotConfigured
	}
	return slf.remoteTransport.Request(actorremotes.ActorRef{
		NodeID:   target.NodeID(),
		ActorKey: target.ActorKey(),
	}, msg, timeout)
}

// 异步向指定actor发送消息, 不等待响应
func (slf *ActorFramework) RequestAsync(target LucencyActorID, msg any, timeout time.Duration, callback func(result any, err error)) error {
	isLocal, err := slf.locator.IsLocalActor(target)
	if err != nil {
		return err
	}
	if !isLocal {
		if slf.remoteTransport == nil {
			return kkerrors.ErrActorRemoteTransportNotConfigured
		}
		return slf.remoteTransport.RequestAsync(actorremotes.ActorRef{
			NodeID:   target.NodeID(),
			ActorKey: target.ActorKey(),
		}, msg, timeout, callback)
	}
	pid, err := slf.locator.GetActor(target)
	if err != nil {
		return err
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				callback(nil, fmt.Errorf("panic: %v", r))
			}
		}()
		future := slf.actorSys.Root.RequestFuture(pid, msg, timeout)
		result, err := future.Result()
		callback(result, err)
	}()
	return nil
}

func (slf *ActorFramework) HandleRemoteSend(targetRef actorremotes.ActorRef, msg any) error {
	target, err := NewLucencyActorID(targetRef.NodeID, targetRef.ActorKey)
	if err != nil {
		return err
	}
	pid, err := slf.locator.GetActor(target)
	if err != nil {
		return err
	}
	slf.actorSys.Root.Send(pid, msg)
	return nil
}

func (slf *ActorFramework) HandleRemoteRequest(targetRef actorremotes.ActorRef, msg any, timeout time.Duration) (any, error) {
	target, err := NewLucencyActorID(targetRef.NodeID, targetRef.ActorKey)
	if err != nil {
		return nil, err
	}
	pid, err := slf.locator.GetActor(target)
	if err != nil {
		return nil, err
	}
	future := slf.actorSys.Root.RequestFuture(pid, msg, timeout)
	return future.Result()
}

//-------------------------------------------------------------------------

// 发送消息到指定actor
func Send[T any](af *ActorFramework, target LucencyActorID, msg T) error {
	return af.Send(target, msg)
}

// 同步请求指定actor
func Request[REQ any, RSP any](af *ActorFramework, target LucencyActorID, msg *REQ, timeout time.Duration) (*RSP, error) {
	result, err := af.Request(target, msg, timeout)
	if err != nil {
		return nil, err
	}
	rsp, ok := result.(*RSP)
	if !ok {
		return nil, fmt.Errorf("invalid result type: %T", result)
	}
	return rsp, nil
}

// 异步请求指定actor
func RequestAsync[REQ any, RSP any](af *ActorFramework, target LucencyActorID, msg *REQ, timeout time.Duration, callback func(result *RSP, err error)) error {
	return af.RequestAsync(target, msg, timeout, func(result any, err error) {
		if err != nil {
			callback(nil, err)
			return
		}
		rsp, ok := result.(*RSP)
		if !ok {
			callback(nil, fmt.Errorf("invalid result type: %T", result))
			return
		}
		callback(rsp, nil)
	})
}

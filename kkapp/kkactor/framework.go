package kkactor

import (
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kkerrors"
)

// IActorFramework 是 Actor 框架门面。
type IActorFramework interface {
	GetLocator() *ActorLocator
	GetActorSystem() *actor.ActorSystem
	SetRemoteTransport(transport actorremotes.IRemoteActorTransport) error
	GetRemoteTransport() actorremotes.IRemoteActorTransport
	Send(target LucencyID, msg any) error
	Request(target LucencyID, msg any, timeout time.Duration) (any, error)
	RequestAsync(target LucencyID, msg any, timeout time.Duration, callback func(result any, err error)) error
}

//-------------------------------------------------------------------------

// NewActorSystem 创建一个 ActorSystem，主要用于生产环境。
func NewActorSystem(options ...actor.ConfigOption) *actor.ActorSystem {
	return actor.NewActorSystem(options...)
}

// NewSilentActorSystem 创建一个关闭 protoactor-go 日志输出的 ActorSystem，主要用于测试/压测场景。
func NewSilentActorSystem(options ...actor.ConfigOption) *actor.ActorSystem {
	silentLogger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))
	options = append(options, actor.WithLoggerFactory(
		func(system *actor.ActorSystem) *slog.Logger {
			return silentLogger
		},
	))
	return actor.NewActorSystem(options...)
}

//-------------------------------------------------------------------------

// NewActorFramework 创建一套与彼此绑定的 Locator + ActorSystem。
//
// 不提供「单独注入 Locator」的构造方式：Locator 里存放的 *actor.PID 必须与某一 ActorSystem 中
// Spawn 出来的 PID 同源。若允许外部分别传入 locator1 与 system2，极易出现「往 locator1 里
// AddActor 的却是 system2.Root.Spawn 的 PID」——Send/Request 仍走本 framework 的 actorSys，
// 会造成找不到 PID 或与错误进程通信。因此二者只在工厂方法内成对创建，保证一一对应。
func NewActorFramework(options ...actor.ConfigOption) *ActorFramework {
	return &ActorFramework{
		locator:  NewActorLocator(),
		actorSys: NewActorSystem(options...),
	}
}

// NewSilentActorFramework 与 NewActorFramework 相同配对关系，仅 ActorSystem 使用静默日志，便于测试/压测。
func NewSilentActorFramework(options ...actor.ConfigOption) *ActorFramework {
	return &ActorFramework{
		locator:  NewActorLocator(),
		actorSys: NewSilentActorSystem(options...),
	}
}

// ActorFramework 是 Actor 框架门面。
type ActorFramework struct {
	// locator 与 actorSys 由同一工厂创建并绑定：仅寻址本 ActorSystem 下注册的 PID。
	// 本地命中时向 actorSys.Root 发消息；否则走 remoteTransport。
	// 业务 Spawn 后须 GetLocator().AddActor(...) 登记，否则本地无法解析。
	locator *ActorLocator
	// Actor系统
	actorSys *actor.ActorSystem
	// 远程Actor传输层
	remoteTransport actorremotes.IRemoteActorTransport
}

func (slf *ActorFramework) GetLocator() *ActorLocator {
	return slf.locator
}

func (slf *ActorFramework) GetActorSystem() *actor.ActorSystem {
	return slf.actorSys
}

func (slf *ActorFramework) SetRemoteTransport(transport actorremotes.IRemoteActorTransport) error {
	if transport == nil {
		if slf.remoteTransport != nil {
			_ = slf.remoteTransport.Close()
		}
		slf.remoteTransport = nil
		return nil
	}
	transport.SetReceiver(slf)
	if err := transport.Start(); err != nil {
		return err
	}
	oldTransport := slf.remoteTransport
	slf.remoteTransport = transport
	if oldTransport != nil && oldTransport != transport {
		_ = oldTransport.Close()
	}
	return nil
}

func (slf *ActorFramework) GetRemoteTransport() actorremotes.IRemoteActorTransport {
	return slf.remoteTransport
}

// 向指定actor发送消息
func (slf *ActorFramework) Send(target LucencyID, msg any) error {
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
func (slf *ActorFramework) Request(target LucencyID, msg any, timeout time.Duration) (any, error) {
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
func (slf *ActorFramework) RequestAsync(target LucencyID, msg any, timeout time.Duration, callback func(result any, err error)) error {
	if callback == nil {
		return kkerrors.ErrActorAsyncCallbackNil
	}
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
	target, err := NewLucencyID(targetRef.NodeID, targetRef.ActorKey)
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
	target, err := NewLucencyID(targetRef.NodeID, targetRef.ActorKey)
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

package kkactor

import (
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// IActorFramework 是 Actor 框架门面。
type IActorFramework interface {
	GetLocator() *ActorLocator
	GetActorSystem() *actor.ActorSystem
	SetRemoteTransport(transport actorremotes.IRemoteActorTransport) error
	GetRemoteTransport() actorremotes.IRemoteActorTransport
	Send(target LucencyActorID, msg any) error
	Request(target LucencyActorID, msg any, timeout time.Duration) (any, error)
	RequestAsync(target LucencyActorID, msg any, timeout time.Duration, callback func(result any, err error)) error
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

func NewActorFramework(locator *ActorLocator, actorSys *actor.ActorSystem) *ActorFramework {
	if locator == nil {
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicLog("locator is nil")
	}
	if actorSys == nil {
		// 启动期间的异常装配直接panic，不然反而将隐含问题带到了运行期间，造成不可预测的错误
		kklog.PanicLog("actorSys is nil")
	}
	return &ActorFramework{
		locator:  locator,
		actorSys: actorSys,
	}
}

// ActorFramework 是 Actor 框架门面。
type ActorFramework struct {
	locator         *ActorLocator                      // Actor寻址系统
	actorSys        *actor.ActorSystem                 // Actor系统
	remoteTransport actorremotes.IRemoteActorTransport // 远程Actor传输层
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

package kkactor

import (
	"encoding/json"

	"github.com/vvisun/kkdg/utils/kklog"
)

// RemotePID 标识远程 Actor（所在地址 + ActorID）。
type RemotePID struct {
	Addr    string // 远程节点地址（由上层传输定义，如 tcp://host:port 或自定义 key）
	ActorID string // 本地 ActorSystem 内的 actor id（即 PID.id）
}

// Transport 抽象远程传输层，由上层使用 kknet/kktcp/kkudp 或其它实现。
type Transport interface {
	// Send 向远程地址发送一段原始字节数据。
	Send(addr string, data []byte) error
}

// RemoteSystem 为本地 ActorSystem 提供远程通信能力。
type RemoteSystem struct {
	local     *ActorSystem
	transport Transport
}

// NewRemoteSystem 创建一个 RemoteSystem。
func NewRemoteSystem(local *ActorSystem, transport Transport) *RemoteSystem {
	if local == nil || transport == nil {
		return nil
	}
	return &RemoteSystem{
		local:     local,
		transport: transport,
	}
}

// remoteEnvelope 是远程消息的线协议封装。
type remoteEnvelope struct {
	ActorID string `json:"actorId"`
	Data    []byte `json:"data"`
}

// TellBytes 向远程 Actor 发送一个二进制消息（fire-and-forget）。
// 上层可以在 Data 中自行使用 JSON / ProtoBuf 等编码业务消息。
func (rs *RemoteSystem) TellBytes(pid RemotePID, data []byte) error {
	if rs == nil || rs.transport == nil {
		return nil
	}
	env := remoteEnvelope{
		ActorID: pid.ActorID,
		Data:    data,
	}
	raw, err := json.Marshal(&env)
	if err != nil {
		return err
	}
	return rs.transport.Send(pid.Addr, raw)
}

// HandleIncoming 处理从远程传输层收到的数据，解包后投递到本地 ActorSystem。
// 需要由上层在网络接收回调中调用，例如 kknet handler 中。
func (rs *RemoteSystem) HandleIncoming(addr string, raw []byte) {
	if rs == nil || rs.local == nil {
		return
	}
	var env remoteEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		kklog.Errorf("RemoteSystem decode error from %s: %v", addr, err)
		return
	}
	if env.ActorID == "" {
		kklog.Warnf("RemoteSystem got message without actorId from %s", addr)
		return
	}
	// 将 Data 作为消息体（[]byte）投递到对应本地 actor。
	rs.local.sendByID(env.ActorID, env.Data, nil)
}

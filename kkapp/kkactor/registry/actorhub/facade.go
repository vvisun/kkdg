package actorhub

import (
	"github.com/vvisun/kkdg/kkapp/kkactor"
)

// IHubServer 注册中心服务端接口。(实现方式可以是tcp服务器集群， nats, etcd等)
// 收到XXXReq协议，处理后返回XXXResp协议。
type IHubServer interface {
	// 启动
	Start() error
	// 停止
	Stop() error
	// 获取地址
	Addr() string
}

// IHubClient 注册中心客户端接口。
// 发送请求与接收响应是异步的，所有XXXReq协议都有对应的XXXResp协议，客户端监听XXXResp协议并处理响应。
type IHubClient interface {
	// 启动
	Start() error
	// 停止
	Stop() error

	// 获取远程Actor管理器
	GetRemoteActorMgr() IClientRemoteActorMgr

	// 请求注册actor
	ReqRegisterActor(actorID kkactor.LucencyID) error
	// 请求注销actor
	ReqUnregisterActor(actorID kkactor.LucencyID) error
	// 请求寻找actor
	ReqFindActor(actorID kkactor.LucencyID) error
	// 请求获取某个节点上的所有actor列表
	ReqGetAllActorsOfNode(nodeID string) error
}

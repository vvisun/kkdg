package actorhub

import (
	"github.com/vvisun/kkdg/kkapp/kkactor/actorhub/hubproto"
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
	// 注册节点
	RegisterNode(req *hubproto.RegisterNodeReq)
	// 注销节点
	UnregisterNode(req *hubproto.RegisterNodeReq)
	// 更新节点
	UpdateNode(req *hubproto.RegisterNodeReq)

	// 注册actor
	RegisterActor(req *hubproto.RegisterActorReq)
	// 注销actor
	UnregisterActor(req *hubproto.RegisterActorReq)
	// 寻找actor
	FindActor(req *hubproto.FindActorReq)
	// 某个节点上的所有actor列表
	GetAllActorsOfNode(req *hubproto.GetAllActorsOfNodeReq)
}

package kkapp

const (
	NodeTypeGate    = "gate"    // 网关服
	NodeTypeLogic   = "logic"   // 业务服|游戏服
	NodeTypeLogin   = "login"   // 登录服
	NodeTypeChat    = "chat"    // 聊天服
	NodeTypeWorld   = "world"   // 世界服
	NodeTypeMatch   = "match"   // 匹配服
	NodeTypeBattle  = "battle"  // 战斗服
	NodeTypeSocial  = "social"  // 社交服
	NodeTypeEconomy = "economy" // 经济服
	NodeTypeAdmin   = "admin"   // 管理服|后台
)

// 传输层类型。用于网关与业务服之间的消息转发。
type TransType = string

const (
	TransTypeNats  TransType = "nats"  // 使用nats集群转发消息
	TransTypeRpc   TransType = "rpc"   // 使用rpc转发消息
	TransTypeShard TransType = "shard" // 使用shard转发消息
)

const BackendShardCnt = 8

package kkapp

import "github.com/vvisun/kkdg/kknet"

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

type GameErrorCode = uint8

const (
	// 解码错误
	GameErrorCodeDecodeError GameErrorCode = 1 + iota
	// 消息ID不存在
	GameErrorCodeMsgIDNotFound
)

// 会话消息解码错误回调。解码失败或消息ID不存在时回调。用于gametrans.ISessionMsgReceiver.OnSession实现。
type DecodeErrorCallback func(sessionID string, errCode GameErrorCode)

// 原始数据解码错误回调。解码失败或消息ID不存在时回调。用于kknet.IRawHandler实现。
type DecodeErrorCallbackOnRaw func(connID kknet.CONN_ID, errCode GameErrorCode)

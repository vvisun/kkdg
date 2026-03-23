package msgreceiver

import "github.com/vvisun/kkdg/kknet"

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

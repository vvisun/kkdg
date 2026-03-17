package ccgate

import "github.com/vvisun/kkdg/kknet"

type GateErrorCode int

type ErrCallback func(conn kknet.IConn, errCode GateErrorCode)

const (
	// 收到来自客户端的异常数据包时回调。一般可能是客户端版本不匹配，也可能是异常流量攻击。
	// 建议反馈一个错误码给客户端，提示客户端稍后再试。然后掐断连接，既能通知正常客户端，又能防御攻击。
	ERR_CLIENT_INVALID_PACKET GateErrorCode = 1 + iota
	// 接收队列满回调。
	// 可以考虑限流/提示服务器繁忙等。如：限流则通知客户端，提示服务器繁忙则提示客户端稍后再试。
	ERR_RECV_QUEUE_FULL
	// 分配逻辑服失败回调。如：分配失败则通知客户端，提示服务器繁忙则提示客户端稍后再试。
	ERR_ALLOC_LOGIC_NODE_FAILED
	// 用户被顶号/被踢出会话回调。可以在这时发送顶号消息给被踢的连接。
	ERR_USER_KICKED
)

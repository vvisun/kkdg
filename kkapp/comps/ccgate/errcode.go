package ccgate

import "github.com/vvisun/kkdg/kknet"

type GateErrorCode = uint8

type ErrCallback func(conn kknet.IConn, errCode GateErrorCode)

const (
	err_code_min GateErrorCode = 1
	// 收到来自客户端的异常数据包时回调。一般可能是客户端版本不匹配，也可能是异常流量攻击。
	// 建议反馈一个错误码给客户端，提示客户端稍后再试。然后掐断连接，既能通知正常客户端，又能防御攻击。
	ERR_CLIENT_INVALID_PACKET GateErrorCode = 1 + iota
	// 接收队列满回调。
	// 可以考虑限流/提示等。可以通知客户端，提示“服务器繁忙”或提示“稍后再试”。
	ERR_RECV_QUEUE_FULL
	// 分配逻辑服失败回调。
	// 分配失败可以通知客户端，提示“服务器繁忙”或提示“稍后再试”。
	ERR_ALLOC_LOGIC_NODE_FAILED
	// 转发逻辑服失败回调。一般是逻辑服短暂断线或逻辑服已下线。
	// 可以通知客户端，提示“服务器繁忙”或提示“稍后再试”。
	ERR_FORWARD_LOGIC_NODE_FAILED
	// 用户被顶号/被踢出会话回调。可以在这时发送顶号消息给被踢的连接。
	ERR_USER_KICKED
	//
	err_code_count
)

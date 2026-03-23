package internal

import (
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
)

// counter for connection ID. unique id for the connection.
var connIDCounter atomic.Uint64

// NextConnID returns a unique connection ID.
// 进程内全局唯一，这样可以保证即使多个服务器和客户端，连接的connId也不会重复。
func NextConnID() kknet.CONN_ID {
	// 基本上达到uint64最大值，即使每秒100万个连接，也需要几十年，
	// 这时候前面的1~几亿的connId基本上必然已经断开逻辑也已经清理了，不存在逻辑向死亡的connId发消息的情况。
	return connIDCounter.Add(1)
}

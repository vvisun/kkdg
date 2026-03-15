package kknet

import "sync/atomic"

// CONN_ID is the type of connection ID.
type CONN_ID = uint64

const NULL_CONN_ID CONN_ID = 0

// counter for connection ID. unique id for the connection.
var connIDCounter atomic.Uint64

const maxUint64 = ^uint64(0)

// NextConnID returns a unique connection ID.
func NextConnID() CONN_ID {
	// 如果超过uint64最大值，则重置为0。理论上不可能，但以防万一。
	// 基本上达到uint64最大值，即使每秒1000万个连接，也需要几十年，
	// 这时候1~几亿的connId基本上必然已经断开逻辑也已经清理了，不存在逻辑向死亡的connId发消息的情况。
	if connIDCounter.Load() >= maxUint64 {
		connIDCounter.Store(0)
	}
	return connIDCounter.Add(1)
}

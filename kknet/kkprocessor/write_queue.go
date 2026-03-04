package kkprocessor

import "github.com/vvisun/kkdg/kknet"

type QueueWriteProcessor struct {
	conn   kknet.IConn   //连接(用于 flush 超时回调传参)
	connID kknet.CONN_ID //连接ID，记录下来，方便conn关闭导致conn为空时，消费携程可以继续消费。
	userID kknet.USER_ID //用户ID，记录下来，方便业务逻辑层使用。记录conn绑定的用户ID。
	opts   kknet.WriteOptions
}

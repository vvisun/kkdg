package pbgate

import (
	"sync"

	"github.com/vvisun/kkdg/proto/pb"
	"google.golang.org/protobuf/proto"
)

// 分片数=CPU核心数，建议8/16，与gw_bpchan.go保持一致
const shardCount = 8

// 全局通用缓冲区池（小消息8K，大消息64K）
var (
	bufPool8K  = sync.Pool{New: func() any { return make([]byte, 8*1024) }}
	bufPool64K = sync.Pool{New: func() any { return make([]byte, 64*1024) }}
)

// 分片专用缓冲区池，避免多核消费时的内存竞争
var (
	shardBufPool8K  [shardCount]sync.Pool
	shardBufPool64K [shardCount]sync.Pool
)

// 初始化分片缓冲区池
func init() {
	for i := 0; i < shardCount; i++ {
		shardBufPool8K[i] = sync.Pool{New: func() any { return make([]byte, 8*1024) }}
		shardBufPool64K[i] = sync.Pool{New: func() any { return make([]byte, 64*1024) }}
	}
}

// GetBuf 全局通用缓冲区获取
func GetBuf(size int) []byte {
	if size <= 8*1024 {
		return bufPool8K.Get().([]byte)[:0]
	}
	return bufPool64K.Get().([]byte)[:0]
}

// PutBuf 全局通用缓冲区归还
func PutBuf(buf []byte) {
	c := cap(buf)
	switch c {
	case 8 * 1024:
		bufPool8K.Put(buf)
	case 64 * 1024:
		bufPool64K.Put(buf)
	}
}

// GetShardBuf 分片专用缓冲区获取，传入分片ID
func GetShardBuf(shardID int, size int) []byte {
	if shardID < 0 || shardID >= shardCount {
		shardID = 0
	}
	if size <= 8*1024 {
		return shardBufPool8K[shardID].Get().([]byte)[:0]
	}
	return shardBufPool64K[shardID].Get().([]byte)[:0]
}

// PutShardBuf 分片专用缓冲区归还，传入分片ID
func PutShardBuf(shardID int, buf []byte) {
	if shardID < 0 || shardID >= shardCount {
		shardID = 0
	}
	c := cap(buf)
	switch c {
	case 8 * 1024:
		shardBufPool8K[shardID].Put(buf)
	case 64 * 1024:
		shardBufPool64K[shardID].Put(buf)
	}
}

// -------------------------- Protobuf对象池 --------------------------
type PbObjPool struct {
	pool sync.Pool
}

func NewPbObjPool(newFunc func() proto.Message) *PbObjPool {
	return &PbObjPool{
		pool: sync.Pool{New: func() any { return newFunc() }},
	}
}

func (p *PbObjPool) Get() proto.Message {
	return p.pool.Get().(proto.Message)
}

func (p *PbObjPool) Put(msg proto.Message) {
	proto.Reset(msg)
	p.pool.Put(msg)
}

// 业务消息池，与pb/game.proto一一对应（根据实际业务扩展）
var (
	HeartbeatReqPool  = NewPbObjPool(func() proto.Message { return &pb.HeartbeatReq{} })
	PlayerMoveReqPool = NewPbObjPool(func() proto.Message { return &pb.PlayerMoveReq{} })
	ChatReqPool       = NewPbObjPool(func() proto.Message { return &pb.ChatReq{} })
)

// GetPbObjByMsgID 根据消息ID从池获取Protobuf对象
func GetPbObjByMsgID(msgID uint32) proto.Message {
	switch msgID {
	case 1001: // 心跳
		return HeartbeatReqPool.Get()
	case 2001: // 玩家移动
		return PlayerMoveReqPool.Get()
	case 3001: // 聊天
		return ChatReqPool.Get()
	default:
		return nil
	}
}

// PutPbObjByMsgID 根据消息ID归还Protobuf对象到对应池
func PutPbObjByMsgID(msgID uint32, msg proto.Message) {
	if msg == nil {
		return
	}
	switch msgID {
	case 1001:
		HeartbeatReqPool.Put(msg)
	case 2001:
		PlayerMoveReqPool.Put(msg)
	case 3001:
		ChatReqPool.Put(msg)
	}
}

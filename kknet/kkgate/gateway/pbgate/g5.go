package pbgate

import (
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// 协议常量
const (
	MagicNum      = 0x6699                                            // 魔数
	MagicNumLen   = 2                                                 // 魔数字节数
	TotalLenLen   = 2                                                 // 消息体总长度字节数
	MsgIDLen      = 4                                                 // 消息ID字节数
	TraceIDLen    = 16                                                // TraceID字节数（全局唯一）
	HeaderLen     = MagicNumLen + TotalLenLen + MsgIDLen + TraceIDLen // 协议头总长度
	MaxMsgBodyLen = 65535                                             // 最大消息体长度
)

// 协议错误定义
var (
	ErrInvalidMagic = errors.New("invalid protocol magic num")
	ErrMsgTooLarge  = errors.New("message body too large")
	ErrInvalidLen   = errors.New("invalid message length")
	ErrProtoDecode  = errors.New("protobuf decode failed")
)

// Message 协议层通用消息结构
type Message struct {
	MsgID   uint32
	TraceID []byte
	Data    proto.Message
}

// Encode 单条消息编码：魔数+总长度+消息ID+TraceID+Protobuf体
func Encode(msgID uint32, traceID []byte, pbMsg proto.Message) ([]byte, error) {
	pbData, err := proto.Marshal(pbMsg)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrProtoDecode, err)
	}
	if len(pbData) > MaxMsgBodyLen {
		return nil, ErrMsgTooLarge
	}
	if len(traceID) != TraceIDLen {
		return nil, errors.New("invalid traceID length, must 16 bytes")
	}

	totalLen := MsgIDLen + TraceIDLen + len(pbData)
	buf := GetBuf(HeaderLen + len(pbData))
	buf = append(buf, make([]byte, HeaderLen+len(pbData))...)[:HeaderLen+len(pbData)]

	offset := 0
	// 魔数
	binary.BigEndian.PutUint16(buf[offset:offset+MagicNumLen], MagicNum)
	offset += MagicNumLen
	// 总长度
	binary.BigEndian.PutUint16(buf[offset:offset+TotalLenLen], uint16(totalLen))
	offset += TotalLenLen
	// 消息ID
	binary.BigEndian.PutUint32(buf[offset:offset+MsgIDLen], msgID)
	offset += MsgIDLen
	// TraceID
	copy(buf[offset:offset+TraceIDLen], traceID)
	offset += TraceIDLen
	// Protobuf体
	copy(buf[offset:], pbData)

	return buf, nil
}

// Decode 单条消息解码：从二进制包解析出消息ID+TraceID+Protobuf体
func Decode(data []byte, pbMsg proto.Message) (uint32, []byte, error) {
	if len(data) < HeaderLen {
		return 0, nil, ErrInvalidLen
	}

	offset := 0
	// 校验魔数
	magic := binary.BigEndian.Uint16(data[offset : offset+MagicNumLen])
	if magic != MagicNum {
		return 0, nil, ErrInvalidMagic
	}
	offset += MagicNumLen

	// 解析总长度
	totalLen := binary.BigEndian.Uint16(data[offset : offset+TotalLenLen])
	if int(totalLen) > len(data)-MagicNumLen-TotalLenLen {
		return 0, nil, ErrInvalidLen
	}
	offset += TotalLenLen

	// 解析消息ID
	msgID := binary.BigEndian.Uint32(data[offset : offset+MsgIDLen])
	offset += MsgIDLen

	// 解析TraceID
	traceID := make([]byte, TraceIDLen)
	copy(traceID, data[offset:offset+TraceIDLen])
	offset += TraceIDLen

	// 解析Protobuf体
	pbData := data[offset : offset+int(totalLen)-MsgIDLen-TraceIDLen]
	if err := proto.Unmarshal(pbData, pbMsg); err != nil {
		return 0, nil, fmt.Errorf("%w: %v", ErrProtoDecode, err)
	}

	return msgID, traceID, nil
}

// EncodeBatch 通用批量编码
func EncodeBatch(msgs []Message) ([]byte, error) {
	totalSize := 0
	for _, msg := range msgs {
		encData, _ := Encode(msg.MsgID, msg.TraceID, msg.Data)
		totalSize += len(encData)
	}
	batchBuf := GetBuf(totalSize)
	for _, msg := range msgs {
		encData, err := Encode(msg.MsgID, msg.TraceID, msg.Data)
		if err != nil {
			PutBuf(batchBuf)
			return nil, err
		}
		batchBuf = append(batchBuf, encData...)
		PutBuf(encData)
	}
	return batchBuf, nil
}

// EncodeBatchWithShard 分片专用批量编码，使用分片缓冲区池
func EncodeBatchWithShard(msgs []*Message, shardID int) ([]byte, error) {
	totalSize := 0
	for _, msg := range msgs {
		encData, _ := Encode(msg.MsgID, msg.TraceID, msg.Data)
		totalSize += len(encData)
	}
	// 从分片缓冲区池获取
	batchBuf := GetShardBuf(shardID, totalSize)
	for _, msg := range msgs {
		encData, err := Encode(msg.MsgID, msg.TraceID, msg.Data)
		if err != nil {
			PutShardBuf(shardID, batchBuf)
			return nil, err
		}
		batchBuf = append(batchBuf, encData...)
		PutBuf(encData)
	}
	return batchBuf, nil
}

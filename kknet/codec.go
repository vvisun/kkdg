package kknet

import (
	"encoding/binary"
	"errors"
)

// 1k
const default_max_message_size = 1024

// LengthPrefixCodec 长度前缀编解码器
// 消息格式: [4字节长度][消息体]
type LengthPrefixCodec struct {
	maxMessageSize int // 最大消息大小，0表示不限制
}

// NewLengthPrefixCodec 创建长度前缀编解码器
func NewLengthPrefixCodec(maxMessageSize int) (*LengthPrefixCodec, error) {
	// 检查maxMessageSize是否合理
	// 理论上的最大长度，由于长度字段是4个字节，所以理论上的最大长度是: 4294967295
	if maxMessageSize <= 0 {
		maxMessageSize = default_max_message_size
	}
	if maxMessageSize > default_max_message_size {
		return nil, ErrMaxMessageSize
	}
	return &LengthPrefixCodec{
		maxMessageSize: maxMessageSize,
	}, nil
}

// Encode 编码消息
func (c *LengthPrefixCodec) Encode(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("data is empty")
	}

	// 4字节长度 + 消息体
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(data)))
	copy(buf[4:], data)

	return buf, nil
}

// Decode 解码消息
func (c *LengthPrefixCodec) Decode(data []byte) ([]byte, []byte, error) {
	if len(data) < 4 {
		// 数据不足，等待更多数据
		return nil, data, nil
	}

	// 读取消息长度
	msgLen := binary.BigEndian.Uint32(data[0:4])

	// 检查消息长度是否合理
	if c.maxMessageSize > 0 && int(msgLen) > c.maxMessageSize {
		return nil, nil, errors.New("message size exceeds maximum")
	}

	// 检查是否有足够的数据
	if len(data) < 4+int(msgLen) {
		// 数据不足，等待更多数据
		return nil, data, nil
	}

	// 提取消息体
	msg := data[4 : 4+msgLen]
	// 返回剩余数据
	remaining := data[4+msgLen:]

	return msg, remaining, nil
}

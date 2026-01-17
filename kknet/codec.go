package kknet

import (
	"encoding/binary"
	"errors"
)

const default_max_message_size = 4096
const default_length_size = 4

// LengthPrefixCodec 长度前缀编解码器
// 消息格式: [4字节长度][消息体]
type LengthPrefixCodec struct {
	order          binary.ByteOrder // 字节序
	maxMessageSize int              // 最大消息大小
}

// NewLengthPrefixCodec 创建长度前缀编解码器
func NewLengthPrefixCodec(order binary.ByteOrder, maxMessageSize int) (*LengthPrefixCodec, error) {
	if maxMessageSize <= 0 {
		maxMessageSize = default_max_message_size
	}
	if maxMessageSize > default_max_message_size {
		return nil, ErrMaxMessageSize
	}
	return &LengthPrefixCodec{
		order:          order,
		maxMessageSize: maxMessageSize,
	}, nil
}

// Encode 编码消息
func (c *LengthPrefixCodec) Encode(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("data is empty")
	}

	// 4字节长度 + 消息体
	buf := make([]byte, default_length_size+len(data))
	c.order.PutUint32(buf[0:default_length_size], uint32(len(data)))
	copy(buf[default_length_size:], data)

	return buf, nil
}

// Decode 解码消息
func (c *LengthPrefixCodec) Decode(data []byte) ([]byte, []byte, error) {
	if len(data) < default_length_size {
		// 数据不足，等待更多数据
		return nil, data, nil
	}

	// 读取消息长度
	msgLen := c.order.Uint32(data[0:default_length_size])

	// 检查消息长度是否合理
	if c.maxMessageSize > 0 && int(msgLen) > c.maxMessageSize {
		return nil, nil, ErrMaxMessageSize
	}

	// 检查是否有足够的数据
	if len(data) < default_length_size+int(msgLen) {
		// 数据不足，等待更多数据
		return nil, data, nil
	}

	// 提取消息体
	msg := data[default_length_size : default_length_size+int(msgLen)]
	// 返回剩余数据
	remaining := data[default_length_size+int(msgLen):]

	return msg, remaining, nil
}

package kknet

import (
	"sync"
)

// MessageBuffer 消息缓冲区，用于处理粘包和拆包
type MessageBuffer struct {
	buffer []byte
	codec  ICodec
	mu     sync.Mutex
}

// NewMessageBuffer 创建消息缓冲区
func NewMessageBuffer(codec ICodec) *MessageBuffer {
	return &MessageBuffer{
		buffer: make([]byte, 0),
		codec:  codec,
	}
}

// Append 追加数据到缓冲区
func (mb *MessageBuffer) Append(data []byte) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.buffer = append(mb.buffer, data...)
}

// ReadMessages 从缓冲区读取完整的消息
func (mb *MessageBuffer) ReadMessages() ([][]byte, error) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if len(mb.buffer) == 0 {
		return nil, nil
	}

	var messages [][]byte
	remaining := mb.buffer

	for len(remaining) > 0 {
		msg, rest, err := mb.codec.Decode(remaining)
		if err != nil {
			return nil, err
		}

		if msg == nil {
			// 数据不足，等待更多数据
			break
		}

		messages = append(messages, msg)
		remaining = rest
	}

	// 更新缓冲区
	mb.buffer = remaining

	return messages, nil
}

// Clear 清空缓冲区
func (mb *MessageBuffer) Clear() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.buffer = mb.buffer[:0]
}

// Size 返回缓冲区大小
func (mb *MessageBuffer) Size() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return len(mb.buffer)
}

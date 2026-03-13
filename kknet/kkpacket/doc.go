// Package kkpacket 定义流式封包格式与粘包拆包。
//
// 整包格式：packet = [length, head, body]
//
//   - [length]：存放 [message] 的长度，占 2 或 4 个字节，默认 4 字节
//   - [head]：消息头（mid, seq, ...）
//   - [body]：消息体（对象二进制数据）
//
// stream = [length, message]，message = [head, body]
package kkpacket

package kkerrors

import "errors"

var (
	// ErrMemberNotFound 成员未找到
	ErrMemberNotFound = errors.New("member not found")
	// ErrInvalidPacket 无效的消息包
	ErrInvalidPacket = errors.New("invalid packet")
	// ErrNoMemberOfType 没有该类型的成员
	ErrNoMemberOfType = errors.New("no member of type")
)

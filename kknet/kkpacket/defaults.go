package kkpacket

var defaultStreamPacket *LengthFieldStreamPacket

const defaultMaxMessageSize = 4 * 1024 //默认MaxMessageSize

func DefaultStreamPacket() IStreamPacket {
	return defaultStreamPacket
}

// 整包最大长度，包括长度字段。[length,message]
func DefaultMaxMessageSize() int {
	return defaultMaxMessageSize
}

package kkpacket

var defaultStreamPacket = NewLengthFieldStreamPacket(nil)

const defaultMaxMessageSize = 8 * 1024 //默认MaxMessageSize为16KB

func DefaultStreamPacket() IStreamPacket {
	return defaultStreamPacket
}

func DefaultMaxMessageSize() int {
	return defaultMaxMessageSize
}

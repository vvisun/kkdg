package kkprocessor

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
)

type PacketSpliter struct {
	streamTool kkpacket.IPacket
	shrinkCap  int
	recvBuf    []byte                        //残包缓冲区。初始化为nil，避免永远没残包还一直占内存。有残包再分配即可。
	splitBuf   [kknet.BatchPacketSize][]byte //拆分缓冲区，用于拆分数据包时复用，避免分配新的内存
}

func NewPacketSpliter(streamTool kkpacket.IPacket, shrinkCap int) PacketSpliter {
	if streamTool == nil {
		panic("streamTool is required")
	}
	if shrinkCap <= 0 {
		shrinkCap = defaultRecvBufSize
	}
	return PacketSpliter{
		streamTool: streamTool,
		shrinkCap:  shrinkCap,
	}
}

func (ps *PacketSpliter) reRecvBuf(capacity int) {
	if ps.recvBuf == nil {
		ps.recvBuf = byteslice.GetZero(capacity)
		return
	}
	ps.recvBuf = ps.recvBuf[:0]
	byteslice.Put(ps.recvBuf)
	ps.recvBuf = byteslice.GetZero(capacity)
}

// 粘包拆包。注意：返回的packets是本类的私有成员，外部只读，如需更改请自行拷贝。
func (ps *PacketSpliter) Split(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// 拆包（单生产者，无锁）
	buf := data
	if len(ps.recvBuf) > 0 {
		ps.recvBuf = append(ps.recvBuf, data...)
		buf = ps.recvBuf
	}

	packets, leftData, err := ps.streamTool.Split(buf, ps.splitBuf[:0])
	if err != nil {
		if ps.recvBuf != nil {
			ps.recvBuf = ps.recvBuf[:0]
		}
		return nil, err
	}

	if len(leftData) > 0 {
		leftLen := len(leftData)
		ps.reRecvBuf(defaultRecvBufSize + leftLen)
		ps.recvBuf = ps.recvBuf[:leftLen]
		copy(ps.recvBuf, leftData)
	} else if ps.recvBuf != nil {
		ps.recvBuf = ps.recvBuf[:0]
	}

	if len(ps.recvBuf) == 0 && cap(ps.recvBuf) > ps.shrinkCap {
		byteslice.Put(ps.recvBuf)
		ps.recvBuf = nil
	}

	if len(packets) == 0 {
		return nil, nil
	}
	return packets, nil
}

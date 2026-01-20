package kkpacket

import (
	"errors"
	"io"

	"github.com/vvisun/kkdg/kkerrors"
)

// IStreamReader provides buffered stream access for unpacking.
type IStreamReader interface {
	InboundBuffered() int
	Peek(n int) ([]byte, error)
	Discard(n int) (discarded int, err error)
	Next(n int) (buf []byte, err error)
}

// IStreamUnpacker extracts messages from a stream.
type IStreamUnpacker interface {
	Unpack(r IStreamReader) (data []byte, ok bool, err error)
}

// LengthFieldUnpacker parses 4-byte length-prefixed frames.
type LengthFieldUnpacker struct {
	maxSize   int
	msgPacket *packer
}

// NewLengthFieldUnpacker creates a length-field unpacker.
func NewLengthFieldUnpacker(maxSize int, msgPacket *packer) *LengthFieldUnpacker {
	return &LengthFieldUnpacker{maxSize: maxSize, msgPacket: msgPacket}
}

// Unpack implements StreamUnpacker.
func (u *LengthFieldUnpacker) Unpack(r IStreamReader) ([]byte, bool, error) {
	if r.InboundBuffered() < 4 {
		return nil, false, nil
	}
	header, err := r.Peek(4)
	if err != nil {
		if errors.Is(err, io.ErrShortBuffer) {
			return nil, false, nil
		}
		return nil, false, err
	}
	size := int(GetByteOrder().Uint32(header))
	if size < 0 || size > u.maxSize {
		return nil, false, kkerrors.ErrMaxMessageSize
	}
	if r.InboundBuffered() < 4+size {
		return nil, false, nil
	}
	_, _ = r.Discard(4)
	data, err := r.Next(size)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func (u *LengthFieldUnpacker) GetMaxSize() int {
	return u.maxSize
}

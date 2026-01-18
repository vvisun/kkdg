package kknet

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/vvisun/kkdg/kkerrors"
)

var defaultEndian = binary.BigEndian // 默认大端序(网络字节序)

func GetDefaultEndian() binary.ByteOrder {
	return defaultEndian
}

// StreamReader provides buffered stream access for unpacking.
type StreamReader interface {
	InboundBuffered() int
	Peek(n int) ([]byte, error)
	Discard(n int) (discarded int, err error)
	Next(n int) (buf []byte, err error)
}

// StreamUnpacker extracts messages from a stream.
type StreamUnpacker interface {
	Unpack(r StreamReader) (data []byte, ok bool, err error)
}

// LengthFieldUnpacker parses 4-byte length-prefixed frames.
type LengthFieldUnpacker struct {
	MaxSize int
}

// NewLengthFieldUnpacker creates a length-field unpacker.
func NewLengthFieldUnpacker(maxSize int) *LengthFieldUnpacker {
	return &LengthFieldUnpacker{MaxSize: maxSize}
}

// Unpack implements StreamUnpacker.
func (u *LengthFieldUnpacker) Unpack(r StreamReader) ([]byte, bool, error) {
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
	size := int(defaultEndian.Uint32(header))
	if size < 0 || size > u.MaxSize {
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

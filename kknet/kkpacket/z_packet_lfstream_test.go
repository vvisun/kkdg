package kkpacket

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func TestSetByteOrder(t *testing.T) {
	ConfigDefaults(nil, binary.LittleEndian)
	if GetByteOrder() != binary.LittleEndian {
		t.Errorf("GetByteOrder() = %v, want %v", GetByteOrder(), binary.LittleEndian)
	}
	// 再次设置，应该不会修改
	setByteOrder(binary.BigEndian)
	if GetByteOrder() != binary.LittleEndian {
		t.Errorf("GetByteOrder() = %v, want %v", GetByteOrder(), binary.LittleEndian)
	}
}

func TestSetDefaultStreamPacket(t *testing.T) {
	ConfigDefaults(NewLengthFieldStreamPacket(4, 8*1024), nil)
	if DefaultStreamPacket().LengthFieldByteCount() != 4 || DefaultStreamPacket().MaxPacketSize() != 8*1024 {
		t.Errorf("DefaultStreamPacket() = %v, want %v", DefaultStreamPacket(), NewLengthFieldStreamPacket(4, 4*1024))
	}
	// 再次设置，应该不会修改
	setDefaultStreamPacket(NewLengthFieldStreamPacket(2, 2*1024))
	if DefaultStreamPacket().LengthFieldByteCount() != 4 || DefaultStreamPacket().MaxPacketSize() != 8*1024 {
		t.Errorf("DefaultStreamPacket() = %v, want %v", DefaultStreamPacket(), NewLengthFieldStreamPacket(4, 8*1024))
	}
}

func TestNewLengthFieldStreamPacket_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewLengthFieldStreamPacket(1) should panic")
		}
	}()
	NewLengthFieldStreamPacket(1, 4*1024)
}

func TestNewLengthFieldStreamPacket_Valid(t *testing.T) {
	for _, lfb := range []int{2, 4} {
		p := NewLengthFieldStreamPacket(lfb, 4*1024)
		if p.LengthFieldByteCount() != lfb {
			t.Errorf("LengthFieldByteCount() = %d, want %d", p.LengthFieldByteCount(), lfb)
		}
	}
}

func TestLengthFieldStreamPacket_Pack_Unpack(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	msg := []byte("hello")
	bb, err := p.Pack(msg)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	defer kkbuffer.Put(bb)

	got, err := p.Unpack(bb.B)
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if string(got) != string(msg) {
		t.Errorf("Unpack() = %q, want %q", got, msg)
	}
}

func TestLengthFieldStreamPacket_Pack_Empty(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	bb, err := p.Pack(nil)
	if err != nil {
		t.Fatalf("Pack(nil): %v", err)
	}
	defer kkbuffer.Put(bb)
	got, err := p.Unpack(bb.B)
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Unpack() len = %d, want 0", len(got))
	}
}

func TestLengthFieldStreamPacket_Pack_TooLarge(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	big := make([]byte, p.MaxPacketSize()+1)
	_, err := p.Pack(big)
	if err == nil || !errors.Is(err, kkerrors.ErrPktMaxMessageSize) {
		t.Errorf("Pack(too large) = %v, want ErrMaxMessageSize", err)
	}
}

func TestLengthFieldStreamPacket_ReadMessageSize_TooShort(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	_, err := p.ReadMessageSize([]byte{1, 2})
	if err == nil || !errors.Is(err, kkerrors.ErrPktDataTooShortToDecode) {
		t.Errorf("ReadMessageSize(short) = %v, want ErrDataTooShortToDecode", err)
	}
}

func TestLengthFieldStreamPacket_MessageBytes_TooShort(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	_, err := p.MessageBytes([]byte{1, 2})
	if err == nil || !errors.Is(err, kkerrors.ErrPktDataTooShortToDecode) {
		t.Errorf("MessageBytes(short) = %v, want ErrDataTooShortToDecode", err)
	}
}

func TestLengthFieldStreamPacket_CheckPacket_TooShort(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	err := p.CheckPacket([]byte{1, 2})
	if err == nil || !errors.Is(err, kkerrors.ErrPktDataTooShortToDecode) {
		t.Errorf("CheckPacket(short) = %v, want ErrDataTooShortToDecode", err)
	}
}

func TestLengthFieldStreamPacket_CheckPacket_InvalidLenMismatch(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	// length field says 10, but total len is 4+3=7
	packet := []byte{0, 0, 0, 10, 'a', 'b', 'c'}
	err := p.CheckPacket(packet)
	if err == nil || !errors.Is(err, kkerrors.ErrClusterInvalidPacket) {
		t.Errorf("CheckPacket(mismatch) = %v, want ErrInvalidPacket", err)
	}
}

func TestLengthFieldStreamPacket_CheckPacket_NilBuffer(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	err := p.CheckPacketBuffer(nil)
	if err == nil || !errors.Is(err, kkerrors.ErrClusterInvalidPacket) {
		t.Errorf("CheckPacketBuffer(nil) = %v, want ErrInvalidPacket", err)
	}
}

func TestLengthFieldStreamPacket_Unpack_TooShort(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	// len=10 but only 6 bytes
	packet := []byte{0, 0, 0, 10, 'a', 'b'}
	_, err := p.Unpack(packet)
	if err == nil || !errors.Is(err, kkerrors.ErrClusterInvalidPacket) {
		t.Errorf("Unpack(short) = %v, want ErrInvalidPacket", err)
	}
}

func TestLengthFieldStreamPacket_Split_Empty(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	recvs, left, err := p.Split(nil, nil)
	if err != nil {
		t.Errorf("Split(nil) err = %v", err)
	}
	if len(recvs) != 0 {
		t.Errorf("Split recvs len = %d, want 0", len(recvs))
	}
	if left != nil {
		t.Errorf("Split left = %v, want nil", left)
	}
}

func TestLengthFieldStreamPacket_Split_OnePacket(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	bb, _ := p.Pack([]byte("hi"))
	defer kkbuffer.Put(bb)
	data := append([]byte(nil), bb.B...)

	recvs, left, err := p.Split(data, nil)
	if err != nil {
		t.Errorf("Split: %v", err)
	}
	if len(recvs) != 1 {
		t.Fatalf("Split recvs len = %d, want 1", len(recvs))
	}
	if len(left) != 0 {
		t.Errorf("Split left len = %d, want 0", len(left))
	}
	msg, _ := p.Unpack(recvs[0])
	if string(msg) != "hi" {
		t.Errorf("Unpack(recvs[0]) = %q, want hi", msg)
	}
}

func TestLengthFieldStreamPacket_Split_Partial(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	// only 2 bytes - partial length field
	data := []byte{0, 0}
	recvs, left, err := p.Split(data, nil)
	if err != nil {
		t.Errorf("Split(partial) err = %v", err)
	}
	if len(recvs) != 0 {
		t.Errorf("Split recvs len = %d, want 0", len(recvs))
	}
	if string(left) != string(data) {
		t.Errorf("Split left = %v, want %v", left, data)
	}
}

func TestLengthFieldStreamPacket_SplitSR_NotEnough(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	r := &mockStreamReader{buf: []byte{0, 0}, inbound: 2}
	_, ok, err := p.SplitSR(r)
	if err != nil {
		t.Errorf("SplitSR: %v", err)
	}
	if ok {
		t.Error("SplitSR should return ok=false when incomplete")
	}
}

func TestLengthFieldStreamPacket_SplitSR_Complete(t *testing.T) {
	p := NewLengthFieldStreamPacket(4, 4*1024)
	bb, _ := p.Pack([]byte("x"))
	defer kkbuffer.Put(bb)
	r := &mockStreamReader{buf: bb.B, inbound: len(bb.B)}
	data, ok, err := p.SplitSR(r)
	if err != nil {
		t.Errorf("SplitSR: %v", err)
	}
	if !ok {
		t.Error("SplitSR should return ok=true")
	}
	if string(data) != string(bb.B) {
		t.Errorf("SplitSR data = %q, want %q", data, bb.B)
	}
}

func TestLengthFieldStreamPacket_2Byte(t *testing.T) {
	p := NewLengthFieldStreamPacket(2, 4*1024)
	msg := []byte("ab")
	bb, err := p.Pack(msg)
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	defer kkbuffer.Put(bb)
	if len(bb.B) != 2+2 {
		t.Errorf("packet len = %d, want 4", len(bb.B))
	}
	got, err := p.Unpack(bb.B)
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if string(got) != "ab" {
		t.Errorf("Unpack = %q, want ab", got)
	}
}

type mockStreamReader struct {
	buf     []byte
	inbound int
	pos     int
}

func (m *mockStreamReader) InboundBuffered() int {
	return m.inbound - m.pos
}

func (m *mockStreamReader) Peek(n int) ([]byte, error) {
	if m.pos+n > len(m.buf) {
		return nil, io.ErrShortBuffer
	}
	return m.buf[m.pos : m.pos+n], nil
}

func (m *mockStreamReader) Discard(n int) (int, error) {
	m.pos += n
	return n, nil
}

func (m *mockStreamReader) Next(n int) ([]byte, error) {
	if m.pos+n > len(m.buf) {
		return nil, io.ErrShortBuffer
	}
	b := make([]byte, n)
	copy(b, m.buf[m.pos:m.pos+n])
	m.pos += n
	return b, nil
}

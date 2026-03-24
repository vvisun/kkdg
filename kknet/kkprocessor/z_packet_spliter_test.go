package kkprocessor

import (
	"errors"
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func mustPackPacket(t *testing.T, stream kkpacket.IPacket, msg string) []byte {
	t.Helper()
	bb, err := stream.Pack([]byte(msg))
	if err != nil {
		t.Fatalf("Pack(%q): %v", msg, err)
	}
	defer kkbuffer.Put(bb)
	out := make([]byte, len(bb.B))
	copy(out, bb.B)
	return out
}

func TestPacketSpliter_Split_EmptyInput(t *testing.T) {
	ps := NewPacketSpliter(kkpacket.DefaultStreamPacket(), 2048)

	packets, err := ps.Split(nil)
	if err != nil {
		t.Fatalf("Split(nil): %v", err)
	}
	if len(packets) != 0 {
		t.Fatalf("Split(nil) packets=%d, want 0", len(packets))
	}

	packets, err = ps.Split([]byte{})
	if err != nil {
		t.Fatalf("Split([]): %v", err)
	}
	if len(packets) != 0 {
		t.Fatalf("Split([]) packets=%d, want 0", len(packets))
	}
}

func TestPacketSpliter_Split_SinglePacket(t *testing.T) {
	stream := kkpacket.DefaultStreamPacket()
	ps := NewPacketSpliter(stream, 2048)

	p := mustPackPacket(t, stream, "hello")
	packets, err := ps.Split(p)
	if err != nil {
		t.Fatalf("Split(single): %v", err)
	}
	if len(packets) != 1 {
		t.Fatalf("packets=%d, want 1", len(packets))
	}
	msg, err := stream.Unpack(packets[0])
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if got := string(msg); got != "hello" {
		t.Fatalf("msg=%q, want hello", got)
	}
}

func TestPacketSpliter_Split_MultiPacket(t *testing.T) {
	stream := kkpacket.DefaultStreamPacket()
	ps := NewPacketSpliter(stream, 2048)

	var buf []byte
	for _, m := range []string{"a", "bb", "ccc"} {
		buf = append(buf, mustPackPacket(t, stream, m)...)
	}

	packets, err := ps.Split(buf)
	if err != nil {
		t.Fatalf("Split(multi): %v", err)
	}
	if len(packets) != 3 {
		t.Fatalf("packets=%d, want 3", len(packets))
	}
	for i, want := range []string{"a", "bb", "ccc"} {
		msg, err := stream.Unpack(packets[i])
		if err != nil {
			t.Fatalf("Unpack[%d]: %v", i, err)
		}
		if got := string(msg); got != want {
			t.Fatalf("msg[%d]=%q, want %q", i, got, want)
		}
	}
}

func TestPacketSpliter_Split_PartialThenComplete(t *testing.T) {
	stream := kkpacket.DefaultStreamPacket()
	ps := NewPacketSpliter(stream, 2048)

	p := mustPackPacket(t, stream, "full")
	if len(p) < 4 {
		t.Fatalf("packed size=%d, want >=4", len(p))
	}

	packets, err := ps.Split(p[:2])
	if err != nil {
		t.Fatalf("Split(partial): %v", err)
	}
	if len(packets) != 0 {
		t.Fatalf("partial packets=%d, want 0", len(packets))
	}

	packets, err = ps.Split(p[2:])
	if err != nil {
		t.Fatalf("Split(complete): %v", err)
	}
	if len(packets) != 1 {
		t.Fatalf("complete packets=%d, want 1", len(packets))
	}
	msg, err := stream.Unpack(packets[0])
	if err != nil {
		t.Fatalf("Unpack: %v", err)
	}
	if got := string(msg); got != "full" {
		t.Fatalf("msg=%q, want full", got)
	}
}

func TestPacketSpliter_Split_ErrorClearsResidual(t *testing.T) {
	stream := kkpacket.DefaultStreamPacket()
	ps := NewPacketSpliter(stream, 2048)

	p := mustPackPacket(t, stream, "abc")
	_, _ = ps.Split(p[:2]) // leave residual bytes

	// Malformed length field (very large). default stream packet should reject this.
	_, err := ps.Split([]byte{0xff, 0xff, 0xff, 0xff})
	if err == nil {
		t.Fatalf("Split(malformed): want error, got nil")
	}

	// After error, residual should have been cleared and normal packet should pass.
	okPacket := mustPackPacket(t, stream, "ok")
	packets, err := ps.Split(okPacket)
	if err != nil {
		t.Fatalf("Split(after error): %v", err)
	}
	if len(packets) != 1 {
		t.Fatalf("after error packets=%d, want 1", len(packets))
	}
	msg, err := stream.Unpack(packets[0])
	if err != nil {
		t.Fatalf("Unpack(after error): %v", err)
	}
	if got := string(msg); got != "ok" {
		t.Fatalf("msg=%q, want ok", got)
	}
}

type errPacket struct{}

func (errPacket) LengthFieldByteCount() int                                           { return 4 }
func (errPacket) MaxPacketSize() int                                                  { return 1024 }
func (errPacket) LengthFieldBytes(packet []byte) ([]byte, error)                      { return nil, nil }
func (errPacket) MessageBytes(packet []byte) ([]byte, error)                          { return nil, nil }
func (errPacket) ReadMessageSize(packet []byte) (int, error)                          { return 0, nil }
func (errPacket) WriteMessageSize(packet []byte, size int) error                      { return nil }
func (errPacket) CheckPacket(packet []byte) error                                     { return nil }
func (errPacket) CheckPacketBuffer(packetBB *kkbuffer.ByteBuffer) error               { return nil }
func (errPacket) Pack(messageBytes []byte) (*kkbuffer.ByteBuffer, error)              { return nil, nil }
func (errPacket) Unpack(packet []byte) ([]byte, error)                                { return nil, nil }
func (errPacket) Split(packets []byte, recvs [][]byte) ([][]byte, []byte, error)      { return nil, nil, errors.New("split failed") }
func (errPacket) SplitSR(r kkpacket.IStreamReader) ([]byte, bool, error)              { return nil, false, errors.New("split failed") }

func TestPacketSpliter_Split_PropagatesStreamError(t *testing.T) {
	ps := NewPacketSpliter(errPacket{}, 2048)
	_, err := ps.Split([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("Split: want propagated error, got nil")
	}
}


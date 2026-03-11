package kkpacket

import (
	"reflect"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// testMsg is a simple struct used in msgMeta tests.
type testMsg struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// newTestMessagePacket builds a MessagePacket suitable for msgMeta tests.
func newTestMessagePacket(t *testing.T, router *MsgRouter) *MessagePacket {
	t.Helper()
	head := NewPacketHead(&PartUint32{})             // first part: msgID (uint32)
	codec := kkcodec.GetCodec(kkcodec.CodecTypeJson) // use json for simplicity
	if codec == nil {                                // should never happen
		t.Fatalf("json codec is nil")
	}
	return NewMessagePacket(head, codec, router)
}

func TestMsgMeta_BasicGetters(t *testing.T) {
	router := NewMsgRouter()
	const msgID MSGID = 100
	const route = "/test"
	mp := newTestMessagePacket(t, router)

	m := NewMsgMeta[testMsg](msgID, route, mp)
	if m == nil {
		t.Fatalf("NewMsgMeta returned nil")
	}

	if got := m.GetMsgID(); got != msgID {
		t.Fatalf("GetMsgID() = %d, want %d", got, msgID)
	}
	if got := m.GetMsgType(); got != reflect.TypeFor[*testMsg]() {
		t.Fatalf("GetMsgType() = %v, want %v", got, reflect.TypeFor[*testMsg]())
	}
	if got := m.GetMsgRoute(); got != route {
		t.Fatalf("GetMsgRoute() = %q, want %q", got, route)
	}
	if got := m.GetCodec(); got != mp.GetBodyCodec() {
		t.Fatalf("GetCodec() returned unexpected codec")
	}
}

func TestMsgMeta_MarshalUnmarshal(t *testing.T) {
	router := NewMsgRouter()
	const msgID MSGID = 101
	mp := newTestMessagePacket(t, router)
	m := NewMsgMeta[testMsg](msgID, "/m", mp)
	if m == nil {
		t.Fatalf("NewMsgMeta returned nil")
	}

	// Marshal should fail on nil
	if _, err := m.Marshal(nil); err != kkerrors.ErrPktInvalidMessage {
		t.Fatalf("Marshal(nil) error = %v, want ErrInvalidMessage", err)
	}

	orig := &testMsg{ID: 7, Name: "alice"}
	data, err := m.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoded, err := m.Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded == nil || decoded.ID != orig.ID || decoded.Name != orig.Name {
		t.Fatalf("Unmarshal result = %#v, want %#v", decoded, orig)
	}
}

func TestMsgMeta_EncodeDecodeStream_RoundTrip(t *testing.T) {
	router := NewMsgRouter()
	const msgID MSGID = 200
	const route = "/encode"
	mp := newTestMessagePacket(t, router)
	m := NewMsgMeta[testMsg](msgID, route, mp)
	if m == nil {
		t.Fatalf("NewMsgMeta returned nil")
	}
	stream := NewLengthFieldStreamPacket(4, 4*1024) // default stream: [length][message]

	// EncodeStream should fail with nil
	if _, err := m.EncodeStream(nil, stream); err != kkerrors.ErrPktInvalidMessage {
		t.Fatalf("EncodeStream(nil) error = %v, want ErrInvalidMessage", err)
	}

	// Round-trip: EncodeStream + DecodeStream
	orig := &testMsg{ID: 42, Name: "bob"}
	bb, err := m.EncodeStream(orig, stream)
	if err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}
	defer kkbuffer.Put(bb)

	// sanity check: packet must be valid and contain the correct message ID in head
	if err := stream.CheckPacketBuffer(bb); err != nil {
		t.Fatalf("CheckPacketBuffer: %v", err)
	}
	msgBytes, err := stream.MessageBytes(bb.B)
	if err != nil {
		t.Fatalf("MessageBytes: %v", err)
	}
	gotID, err := mp.GetMsgID(msgBytes)
	if err != nil {
		t.Fatalf("GetMsgID: %v", err)
	}
	if gotID != msgID {
		t.Fatalf("packet head msgID = %d, want %d", gotID, msgID)
	}

	decoded, err := m.DecodeStream(bb, stream)
	if err != nil {
		t.Fatalf("DecodeStream: %v", err)
	}
	if decoded == nil || decoded.ID != orig.ID || decoded.Name != orig.Name {
		t.Fatalf("DecodeStream result = %#v, want %#v", decoded, orig)
	}

	// DecodeStream should fail on nil buffer
	if v, err := m.DecodeStream(nil, stream); err != kkerrors.ErrPktInvalidMessage || v != nil {
		t.Fatalf("DecodeStream(nil) = (%v, %v), want (nil, ErrInvalidMessage)", v, err)
	}
}

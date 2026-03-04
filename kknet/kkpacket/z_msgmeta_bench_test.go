package kkpacket

import (
	"strconv"
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Benchmark msgMeta.EncodeStream / DecodeStream end-to-end,
// including json 编解码 + length-field stream 封包/拆包 + 头部写入/解析。

func benchmarkMsgMetaEncodeStream(b *testing.B, nameSize int) {
	router := NewMsgRouter()
	const msgID MSGID = 300
	mp := newTestMessagePacket(&testing.T{}, router)
	m := NewMsgMeta[testMsg](msgID, "/bench", mp)
	if m == nil {
		b.Fatalf("NewMsgMeta returned nil")
	}
	stream := NewLengthFieldStreamPacket(4)

	// 构造一定长度的字符串，避免过于理想的压缩
	payload := &testMsg{ID: 123, Name: makeString(nameSize)}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bb, err := m.EncodeStream(payload, stream)
		if err != nil {
			b.Fatalf("EncodeStream error: %v", err)
		}
		kkbuffer.Put(bb)
	}
}

func BenchmarkMsgMetaEncodeStream_Small(b *testing.B) {
	benchmarkMsgMetaEncodeStream(b, 16)
}

func BenchmarkMsgMetaEncodeStream_Medium(b *testing.B) {
	benchmarkMsgMetaEncodeStream(b, 128)
}

func BenchmarkMsgMetaEncodeStream_Large(b *testing.B) {
	benchmarkMsgMetaEncodeStream(b, 1024)
}

func BenchmarkMsgMetaDecodeStream(b *testing.B) {
	router := NewMsgRouter()
	const msgID MSGID = 301
	mp := newTestMessagePacket(&testing.T{}, router)
	m := NewMsgMeta[testMsg](msgID, "/bench/dec", mp)
	if m == nil {
		b.Fatalf("NewMsgMeta returned nil")
	}
	stream := NewLengthFieldStreamPacket(4)

	orig := &testMsg{ID: 456, Name: makeString(128)}
	bb, err := m.EncodeStream(orig, stream)
	if err != nil {
		b.Fatalf("EncodeStream (setup) error: %v", err)
	}
	defer kkbuffer.Put(bb)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v, err := m.DecodeStream(bb, stream)
		if err != nil {
			b.Fatalf("DecodeStream error: %v", err)
		}
		if v == nil || v.ID != orig.ID {
			b.Fatalf("DecodeStream got %#v, want ID=%d", v, orig.ID)
		}
	}
}

// Benchmark 针对仅头部解析（GetMsgID），测量在高 QPS 下的开销。
func BenchmarkMessagePacket_GetMsgID(b *testing.B) {
	router := NewMsgRouter()
	const msgID MSGID = 400
	mp := newTestMessagePacket(&testing.T{}, router)
	m := NewMsgMeta[testMsg](msgID, "/bench/head", mp)
	if m == nil {
		b.Fatalf("NewMsgMeta returned nil")
	}
	stream := NewLengthFieldStreamPacket(4)

	orig := &testMsg{ID: 789, Name: "head-only"}
	bb, err := m.EncodeStream(orig, stream)
	if err != nil {
		b.Fatalf("EncodeStream (setup) error: %v", err)
	}
	defer kkbuffer.Put(bb)

	msgBytes, err := stream.MessageBytes(bb.B)
	if err != nil {
		b.Fatalf("MessageBytes (setup): %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id, err := mp.GetMsgID(msgBytes)
		if err != nil {
			b.Fatalf("GetMsgID error: %v", err)
		}
		if id != msgID {
			b.Fatalf("GetMsgID = %d, want %d", id, msgID)
		}
	}
}

// makeString 返回长度大约为 n 的字符串。
func makeString(n int) string {
	if n <= 0 {
		return ""
	}
	// 简单重复 pattern 填充
	pattern := "abcdefghijklmnopqrstuvwxyz0123456789"
	var s string
	for len(s) < n {
		s += pattern + strconv.Itoa(len(s))
	}
	return s[:n]
}


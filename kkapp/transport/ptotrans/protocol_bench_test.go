package ptotrans

import (
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

var useCodec = kkcodec.GetCodec(kkcodec.CodecTypeJson)

var testMsg = RpcS2Clients{
	ClientIds: []string{
		"1234567890", "1234567891", "1234567892", "1234567893", "1234567894",
		"1234567895", "1234567896", "1234567897", "1234567898", "1234567899",
	},
	Payload: []byte("test message"),
}

func BenchmarkProtocol_Marshal_Codec(b *testing.B) {
	msg := testMsg
	si := structInfo{}
	si.AddField(dataTypeStringList, "ClientIds")
	si.AddField(dataTypeBytes, "Payload")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bb, err := si.Marshal([]any{msg.ClientIds, msg.Payload}, 0)
		if err != nil {
			b.Fatal(err)
		}
		kkbuffer.Put(bb)
	}
}

func BenchmarkProtocol_UnMarshal_Codec(b *testing.B) {
	msg := testMsg
	si := structInfo{}
	si.AddField(dataTypeStringList, "ClientIds")
	si.AddField(dataTypeBytes, "Payload")

	bb, err := si.Marshal([]any{msg.ClientIds, msg.Payload}, 0)
	if err != nil {
		b.Fatal(err)
	}
	encoded := append([]byte(nil), bb.B...)
	kkbuffer.Put(bb)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		valueList, err := si.Unmarshal(encoded)
		if err != nil {
			b.Fatal(err)
		}
		var got RpcS2Clients
		got.ClientIds = valueList[0].([]string)
		got.Payload = valueList[1].([]byte)
	}
}

func BenchmarkProtocol_Marshal(b *testing.B) {
	msg := testMsg
	for i := 0; i < b.N; i++ {
		useCodec.Marshal(&msg)
	}
}

func BenchmarkProtocol_Unmarshal(b *testing.B) {
	msg := testMsg
	data, err := useCodec.Marshal(&msg)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		useCodec.Unmarshal(data, &msg)
	}
}

func BenchmarkProtocol_MarshalAppend(b *testing.B) {
	msg := testMsg
	for i := 0; i < b.N; i++ {
		bb, err := useCodec.MarshalAppend(&msg, 0)
		kkbuffer.Put(bb)
		if err != nil {
			b.Fatal(err)
		}
	}
}

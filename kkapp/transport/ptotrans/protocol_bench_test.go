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
		"1234567890", "1234567891", "1234567892", "1234567893", "1234567894",
	},
	Payload: []byte("test message 123456789612345678961234567896123456789612345678961234567896123456789612345678961234567896123456789612345678961234567896123456789612345678961234567896"),
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
		bb, err := useCodec.MarshalAppend(&msg, 8)
		kkbuffer.Put(bb)
		if err != nil {
			b.Fatal(err)
		}
	}
}

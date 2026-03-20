package actorshard

import (
	"fmt"

	"github.com/shamaton/msgpack/v2"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// 与 kknet kktcp 默认一致的[length,message]流；message = [wireType : 1][msgpack(body)]。
const (
	wireRegister byte = 1
	wireRelay    byte = 2
	wireReply    byte = 3
	wireErr      byte = 4
)

type regBody struct {
	NodeId   string `msgpack:"i"`
	NodeType string `msgpack:"t,omitempty"`
}

// relayOut 节点发往中心服（不含 Src，防伪造）。
type relayOut struct {
	DestNodeId string `msgpack:"d"`
	ReplyTag   string `msgpack:"r,omitempty"`
	Payload    []byte `msgpack:"p"`
}

// relayIn 中心服下发到节点（含真实 SrcNodeId）。
type relayIn struct {
	SrcNodeId  string `msgpack:"s"`
	DestNodeId string `msgpack:"d"`
	ReplyTag   string `msgpack:"r,omitempty"`
	Payload    []byte `msgpack:"p"`
}

type replyBody struct {
	DestNodeId string `msgpack:"d"`
	ReplyTag   string `msgpack:"r"`
	Payload    []byte `msgpack:"p"`
}

type errBody struct {
	ReplyTag string `msgpack:"r,omitempty"`
	Message  string `msgpack:"m"`
}

func packFrame(stream kkpacket.IPacket, wireType byte, body []byte) (*kkbuffer.ByteBuffer, error) {
	if len(body) > 1<<20 {
		return nil, fmt.Errorf("actorshard wire: body too large")
	}
	inner := make([]byte, 1+len(body))
	inner[0] = wireType
	copy(inner[1:], body)
	return stream.Pack(inner)
}

func unpackFrame(stream kkpacket.IPacket, packet []byte) (wireType byte, body []byte, err error) {
	inner, err := stream.Unpack(packet)
	if err != nil {
		return 0, nil, err
	}
	if len(inner) < 1 {
		return 0, nil, fmt.Errorf("actorshard wire: empty message")
	}
	return inner[0], inner[1:], nil
}

func marshalBody(v any) ([]byte, error) {
	return msgpack.Marshal(v)
}

func unmarshalBody(data []byte, v any) error {
	return msgpack.Unmarshal(data, v)
}

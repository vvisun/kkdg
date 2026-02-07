package msgrouter

import "google.golang.org/protobuf/proto"

// protobuf解码优化，复用同一个buff
type PBBuff struct {
	buff []byte
}

func (slf *PBBuff) Reset() {
	slf.buff = slf.buff[:0]
}

func (slf *PBBuff) Marshal(message proto.Message) ([]byte, error) {
	var err error
	slf.buff, err = proto.MarshalOptions{}.MarshalAppend(slf.buff, message)
	return slf.buff, err
}

package kkrpc

type FrameType = uint32

const (
	FrameTypeUnknown  FrameType = 0
	FrameTypeRequest  FrameType = 1
	FrameTypeResponse FrameType = 2
	FrameTypeOneway   FrameType = 3
)

type Frame struct {
	T    FrameType `json:"t" msgpack:"t"`             // FrameType
	ID   uint64    `json:"id" msgpack:"id"`           // request id. 0 means no oneway
	DL   int64     `json:"dl,omitempty" msgpack:"dl"` // deadline unix ms (0 means no deadline)
	M    string    `json:"m,omitempty" msgpack:"m"`   // method（远程方法名）
	P    []byte    `json:"p,omitempty" msgpack:"p"`   // payload（远程方法参数）
	Code int32     `json:"c,omitempty" msgpack:"c"`   // status code (0 ok) 错误码
	Err  string    `json:"e,omitempty" msgpack:"e"`   // error message 错误信息
}

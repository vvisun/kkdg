package kkrpc

type FrameType uint8

const (
	FrameTypeUnknown       FrameType = 0
	FrameTypeRequest       FrameType = 1
	FrameTypeResponse      FrameType = 2
	FrameTypeOneway        FrameType = 3
	FrameTypeTransMsg      FrameType = 4
	FrameTypeTransBroadMsg FrameType = 5
)

type Frame struct {
	T    FrameType `json:"t" msgpack:"t"`             // FrameType
	ID   uint64    `json:"id" msgpack:"id"`           // request id. 0 means no response required
	DL   int64     `json:"dl,omitempty" msgpack:"dl"` // deadline unix ms (0 means no deadline)
	M    string    `json:"m,omitempty" msgpack:"m"`   // method（远程方法名）
	P    []byte    `json:"p,omitempty" msgpack:"p"`   // payload（远程方法参数）
	Code int32     `json:"c,omitempty" msgpack:"c"`   // status code (0 ok) 错误码
	Err  string    `json:"e,omitempty" msgpack:"e"`   // error message 错误信息
}

// NilResponse 占位，注册单向调用时用来占位，实际不会返回。方便统一method注册逻辑
type NilResponse struct{}

type (
	// TransMsg 网关转发消息
	TransMsg struct {
		ClientId int64  // 客户端id(connID)
		Data     []byte // 转发数据
	}

	// TransBroadcast 网关转发群发消息
	TransBroadMsg struct {
		Clients []int64 // 客户端id列表(connID列表)
		Data    []byte  // 转发数据
	}
)

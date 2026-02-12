package kkrpc

// RegistReqRspHandler 注册请求响应方法 handler
func RegistReqRspHandler[T any, R any](router *RpcReceiver, method string, call ReqRspHandlerFunc[T, R]) {
	h := newReqRspHandler(method, call)
	router.hdMap[method] = h
}

// RegistOneWayHandler 注册单向消息方法 handler
func RegistOneWayHandler[T any](router *RpcReceiver, method string, call OneWayHandlerFunc[T]) {
	h := newOneWayHandler(method, call)
	router.oneWayMap[method] = h
}

// RegisterReqRspMethod 注册请求响应方法。method的参数类型和返回类型必须为REQ和RSP。
// 相当于函数签名: func method(REQ) RSP
func RegisterReqRspMethod[REQ any, RSP any](method string) {
	newReqResp[REQ, RSP](method)
}

// RegisterOneWayMethod 注册单向方法。method的参数类型必须为REQ。
// 相当于函数签名: func method(REQ)
func RegisterOneWayMethod[REQ any](method string) {
	newOneWay[REQ](method)
}

func newReqRspHandler[T any, R any](method string, call ReqRspHandlerFunc[T, R]) *ReqRspHandler[T, R] {
	return &ReqRspHandler[T, R]{
		call:   call,
		method: method,
	}
}

func newOneWayHandler[T any](method string, call OneWayHandlerFunc[T]) *OneWayHandler[T] {
	return &OneWayHandler[T]{
		call:   call,
		method: method,
	}
}

func NewRpcReceiver() *RpcReceiver {
	return &RpcReceiver{
		hdMap:     make(map[string]IRpcHandler),
		oneWayMap: make(map[string]IOneWayHandler),
	}
}

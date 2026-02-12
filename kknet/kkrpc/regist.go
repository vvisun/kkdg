package kkrpc

// RegistRpcHandler 注册RPC方法 handler
func RegistRpcHandler[T any, R any](router *RpcReceiver, method string, call RpcHandlerFunc[T, R]) {
	h := newRpcHandler(method, call)
	router.hdMap[method] = h
}

// RegisterReqResp 注册请求响应方法。method的参数类型和返回类型必须为REQ和RSP。
// 相当于函数签名: func method(REQ) RSP
func RegisterReqResp[REQ any, RSP any](method string) {
	newReqResp[REQ, RSP](method)
}

// RegisterOneWay 注册单向方法。method的参数类型必须为REQ。
// 相当于函数签名: func method(REQ)
func RegisterOneWay[REQ any](method string) {
	newOneWay[REQ](method)
}

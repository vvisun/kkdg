package kkrpc

type testReq struct {
	ID   int
	Data string
}

type testRsp struct {
	Code int
	Msg  string
}

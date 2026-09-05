package gametrans

// ITransportor 数据转发器接口。
// 抽象化接口，方便切换实现逻辑（如：使用Actor、使用Nats、使用RPC等）。
type ITransportor interface {
	Stop() error
	// forwards a message to a client.
	// 发消息到单个客户端。
	//  @param packet is a full stream packet [length,message]
	ForwardToClient(sessionID string, packet []byte) error
	// forwards a message to multiple clients.
	// 发消息到多个客户端。
	//  @param packet is a full stream packet [length,message]
	ForwardToClients(sessionIDs []string, packet []byte) error
	// sends a message to a client.
	// 发消息到单个客户端。
	SendToClient(sessionID string, msg any) error
	// sends a message to multiple clients.
	// 发消息到多个客户端。
	SendToClients(sessionIDs []string, msg any) error
	// notifies a client login or logout. tell gateway and feedback login success to client.
	// 客户端登录或登出通知。一是为了告知网关，二是为了反馈登录成功给客户端
	NotifyClientLoginLogout(sessionID string, userId int64, isLogin bool) error
	// closes a client.
	// 关闭客户端连接。
	CloseClient(sessionID string, reason string) error
	// gets the session manager.
	// 获取会话管理器。
	GetSessionManager() *SessionManager
}

// 消息接收器接口
type ISessionMsgReceiver interface {
	// receives a message from a session.
	// 接收来自会话的消息。
	//  @param sessionID 会话ID
	//  @param packet 整包数据[length,message]。不得保存 packet 引用，如需保存，请自行拷贝。
	//  @param threadIdx 线程索引。gametrans.ITransportor会将sessionID映射到固定的threadIdx（见session.go中的sessionIdToThreadIdx函数）。
	//
	// 注意：
	// 1. OnSession里处理消息时，应该保证相同的threadIdx在同一个工作线程中处理，否则会出现同一sessionID的消息不能保证顺序性。
	// 2. 若实现 IThreadWorkerCount（例如 decodeWorkers），创建 transportor 时会与 SessionManager.GetWorkersCount() 校验；OnSession 仍应拒绝越界 threadIdx。
	OnSession(sessionID string, packet []byte, threadIdx int)
}

// IThreadWorkerCount 可选。按 threadIdx 取工作队列的接收器应实现它，供启动期校验。
type IThreadWorkerCount interface {
	ThreadWorkerCount() int
}

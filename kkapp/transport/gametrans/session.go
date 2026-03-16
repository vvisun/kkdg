package gametrans

import (
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/user"
	"github.com/vvisun/kkdg/utils/kklog"
)

const workers_count = 4096

// 使用稳定哈希将任意 sessionID 均匀映射到 [0, workersCount) 区间。
func sessionIdToThreadIdx(sessionID string, workersCount int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(sessionID))
	idx := int(h.Sum32() % uint32(workersCount))
	return idx
}

// SessionInfo 客户端会话信息
type SessionInfo struct {
	isInPool   atomic.Bool  //标志是否在池中，防止重复入池
	sessionID  string       // 会话ID
	gateNodeID string       // 网关节点ID
	shardIdx   int          // 分片索引
	userID     user.USER_ID // 用户ID
	threadIdx  int          // 线程索引
}

func (si *SessionInfo) GetSessionID() string {
	return si.sessionID
}

func (si *SessionInfo) GetGateNodeID() string {
	return si.gateNodeID
}

func (si *SessionInfo) GetShardIdx() int {
	return si.shardIdx
}

func (si *SessionInfo) GetUserID() user.USER_ID {
	return si.userID
}

func (si *SessionInfo) GetThreadIdx() int {
	return si.threadIdx
}

var sessionInfoPool = sync.Pool{
	New: func() interface{} {
		return &SessionInfo{}
	},
}

func newSessionInfo() *SessionInfo {
	si := sessionInfoPool.Get().(*SessionInfo)
	si.isInPool.Store(false)
	return si
}

func putSessionInfo(si *SessionInfo) {
	if si == nil {
		return
	}
	if !si.isInPool.CompareAndSwap(false, true) {
		return // 已经在池中，不再放回池中, 避免重复放入池中
	}
	si.sessionID = ""
	si.gateNodeID = ""
	si.shardIdx = -1
	si.userID = user.NULL_USER_ID
	sessionInfoPool.Put(si)
}

//--------------------------------------------------

var autoShardIdx int64 = 0 // 自动分配的shardIdx

func getAutoShardIdx() int {
	return int(atomic.AddInt64(&autoShardIdx, 1) % transport.BackendShardCnt)
}

// SessionManager 客户端会话管理器。
// 用于管理客户端会话信息【会话ID、用户ID、网关节点ID、分片索引】
type SessionManager struct {
	sessionMap  sync.Map // map[sessionID]*SessionInfo
	userMap     sync.Map // map[userID]*SessionInfo
	onlineCount int32
	userCount   int32
}

func NewSessionManager() *SessionManager {
	return &SessionManager{}
}

func (slf *SessionManager) AddSession(sessionID string, gateNodeID string) *SessionInfo {
	return slf.AddSessionWithShard(sessionID, gateNodeID, -1)
}

// AddSessionWithShard 与 AddSession 相同，但可指定 shardIdx（用于 shard 模式绑定到收到 C2S 的那条连接）。
// shardIdx < 0 或 >= BackendShardCnt 时使用轮询分配。
// 实际上并不需要接收shardIdx和发送shardIdx必须一致，
// 只需要关心同一个客户端(sessionId)分配到同一个shardIdx即可，因为这样就能保证同一个客户端的接收和发送是顺序性的。
func (slf *SessionManager) AddSessionWithShard(sessionID string, gateNodeID string, shardIdx int) *SessionInfo {
	oldInfo := slf.GetSession(sessionID)
	if oldInfo != nil {
		return oldInfo
	}
	if shardIdx < 0 || shardIdx >= transport.BackendShardCnt {
		shardIdx = getAutoShardIdx()
	}
	atomic.AddInt32(&slf.onlineCount, 1)
	si := newSessionInfo()
	si.sessionID = sessionID
	si.gateNodeID = gateNodeID
	si.shardIdx = shardIdx
	si.threadIdx = sessionIdToThreadIdx(si.sessionID, workers_count)
	slf.sessionMap.Store(sessionID, si)
	kklog.Debugf("newSessionInfo: sessionID=%s, threadIdx=%d", si.sessionID, si.threadIdx)
	return si
}

func (slf *SessionManager) RemoveSession(sessionID string) {
	si, ok := slf.sessionMap.LoadAndDelete(sessionID)
	if !ok {
		return
	}
	atomic.AddInt32(&slf.onlineCount, -1)
	userID := si.(*SessionInfo).userID
	if userID != user.NULL_USER_ID {
		slf.userMap.Delete(userID)
		atomic.AddInt32(&slf.userCount, -1)
	}
	putSessionInfo(si.(*SessionInfo))
}

func (slf *SessionManager) RemoveSessionByUserID(userID user.USER_ID) {
	si, ok := slf.userMap.Load(userID)
	if ok {
		slf.RemoveSession(si.(*SessionInfo).sessionID)
	}
}

func (slf *SessionManager) GetSession(sessionID string) *SessionInfo {
	si, ok := slf.sessionMap.Load(sessionID)
	if !ok {
		return nil
	}
	return si.(*SessionInfo)
}

func (slf *SessionManager) GetSessionByUserID(userID user.USER_ID) *SessionInfo {
	si, ok := slf.userMap.Load(userID)
	if !ok {
		return nil
	}
	return si.(*SessionInfo)
}

// CheckKickOutUser 检查是否需要踢出旧用户。
//
//	如果需要踢出，则返回需要踢出的会话ID。
//	如果不需要踢出，则返回空字符串。
func (slf *SessionManager) CheckKickOutUser(sessionID string, userID user.USER_ID) string {
	si := slf.GetSession(sessionID)
	if si != nil {
		if si.userID != user.NULL_USER_ID && (si.userID != userID || si.sessionID != sessionID) {
			return si.sessionID
		}
	}
	otherSi := slf.GetSessionByUserID(userID)
	if otherSi != nil {
		if otherSi.userID != user.NULL_USER_ID && (otherSi.userID != userID || otherSi.sessionID != sessionID) {
			return otherSi.sessionID
		}
	}
	return ""
}

// Login 登录客户端。
//
//	如果登录成功，则返回 true。
//	如果登录失败，则返回 false。
func (slf *SessionManager) Login(sessionID string, userID user.USER_ID, kickFunc func(sessionID string)) bool {
	if userID == user.NULL_USER_ID {
		return false
	}
	si := slf.GetSession(sessionID)
	if si == nil {
		return false
	}
	kickOutSessionID := slf.CheckKickOutUser(sessionID, userID)
	if kickOutSessionID != "" {
		if kickFunc != nil {
			kickFunc(kickOutSessionID)
		} else {
			kklog.Warnf("kick out user %v from session %v, but kickFunc is nil", userID, kickOutSessionID)
		}
		slf.RemoveSession(kickOutSessionID) // 踢出旧用户
	}
	si.userID = userID
	slf.userMap.Store(userID, si)
	atomic.AddInt32(&slf.userCount, 1)
	return true
}

func (slf *SessionManager) OnlineCount() int {
	return int(atomic.LoadInt32(&slf.onlineCount))
}

func (slf *SessionManager) UserCount() int {
	return int(atomic.LoadInt32(&slf.userCount))
}

package gametrans

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kknet"
)

type SessionInfo struct {
	isInPool   atomic.Bool //标志是否在池中，防止重复入池
	UserID     kknet.USER_ID
	SessionID  string
	GateNodeID string
	ShardIdx   int
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
	si.SessionID = ""
	si.GateNodeID = ""
	si.UserID = kknet.NULL_USER_ID
	sessionInfoPool.Put(si)
}

//--------------------------------------------------

var autoShardIdx int64 = 0 // 自动分配的shardIdx

// SessionManager 会话管理器
type SessionManager struct {
	sessionMap  sync.Map // map[sessionID]*SessionInfo
	userMap     sync.Map // map[userID]*SessionInfo
	onlineCount int32
	userCount   int32
}

func NewSessionManager() *SessionManager {
	return &SessionManager{}
}

func (slf *SessionManager) AddSession(sessionID string, gateNodeID string) {
	slf.AddSessionWithShard(sessionID, gateNodeID, -1)
}

// AddSessionWithShard 与 AddSession 相同，但可指定 shardIdx（用于 shard 模式绑定到收到 C2S 的那条连接）。
// shardIdx < 0 或 >= BackendShardCnt 时使用轮询分配。
func (slf *SessionManager) AddSessionWithShard(sessionID string, gateNodeID string, shardIdx int) {
	oldInfo := slf.GetSession(sessionID)
	if oldInfo != nil {
		return
	}
	if shardIdx < 0 || shardIdx >= kkapp.BackendShardCnt {
		shardIdx = int(atomic.AddInt64(&autoShardIdx, 1) % kkapp.BackendShardCnt)
	}
	si := newSessionInfo()
	si.SessionID = sessionID
	si.GateNodeID = gateNodeID
	si.ShardIdx = shardIdx
	slf.sessionMap.Store(sessionID, si)
	atomic.AddInt32(&slf.onlineCount, 1)
}

func (slf *SessionManager) RemoveSession(sessionID string) {
	si, ok := slf.sessionMap.LoadAndDelete(sessionID)
	if !ok {
		return
	}
	atomic.AddInt32(&slf.onlineCount, -1)
	userID := si.(*SessionInfo).UserID
	if userID != kknet.NULL_USER_ID {
		slf.userMap.Delete(userID)
		atomic.AddInt32(&slf.userCount, -1)
	}
	putSessionInfo(si.(*SessionInfo))
}

func (slf *SessionManager) RemoveSessionByUserID(userID kknet.USER_ID) {
	si, ok := slf.userMap.Load(userID)
	if ok {
		slf.RemoveSession(si.(*SessionInfo).SessionID)
	}
}

func (slf *SessionManager) GetSession(sessionID string) *SessionInfo {
	si, ok := slf.sessionMap.Load(sessionID)
	if !ok {
		return nil
	}
	return si.(*SessionInfo)
}

func (slf *SessionManager) GetSessionByUserID(userID kknet.USER_ID) *SessionInfo {
	si, ok := slf.userMap.Load(userID)
	if !ok {
		return nil
	}
	return si.(*SessionInfo)
}

func (slf *SessionManager) CheckKickOutUser(sessionID string, userID kknet.USER_ID) string {
	si := slf.GetSession(sessionID)
	if si != nil {
		if si.UserID != kknet.NULL_USER_ID && (si.UserID != userID || si.SessionID != sessionID) {
			return si.SessionID
		}
	}
	otherSi := slf.GetSessionByUserID(userID)
	if otherSi != nil {
		if otherSi.UserID != kknet.NULL_USER_ID && (otherSi.UserID != userID || otherSi.SessionID != sessionID) {
			return otherSi.SessionID
		}
	}
	return ""
}

func (slf *SessionManager) Login(sessionID string, userID kknet.USER_ID) bool {
	if userID == kknet.NULL_USER_ID {
		return false
	}
	si := slf.GetSession(sessionID)
	if si == nil {
		return false
	}
	kickOutSessionID := slf.CheckKickOutUser(sessionID, userID)
	if kickOutSessionID != "" {
		slf.RemoveSession(kickOutSessionID) // 踢出旧用户
	}
	si.UserID = userID
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

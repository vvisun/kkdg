package ccgame

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
)

type SessionInfo struct {
	isInPool   atomic.Bool //标志是否在池中，防止重复入池
	UserID     kknet.USER_ID
	SessionID  string
	GateNodeID string
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

// sessionManager 会话管理器
type sessionManager struct {
	sessionMap sync.Map // map[sessionID]*SessionInfo
	userMap    sync.Map // map[userID]*SessionInfo
}

func newSessionManager() *sessionManager {
	return &sessionManager{}
}

func (slf *sessionManager) AddSession(sessionID string, gateNodeID string) {
	oldInfo := slf.GetSession(sessionID)
	if oldInfo != nil {
		return
	}
	si := newSessionInfo()
	si.SessionID = sessionID
	si.GateNodeID = gateNodeID
	slf.sessionMap.Store(sessionID, si)
}

func (slf *sessionManager) RemoveSession(sessionID string) {
	si, ok := slf.sessionMap.LoadAndDelete(sessionID)
	if !ok {
		return
	}
	slf.userMap.Delete(si.(*SessionInfo).UserID)
	putSessionInfo(si.(*SessionInfo))
}

func (slf *sessionManager) RemoveSessionByUserID(userID kknet.USER_ID) {
	si, ok := slf.userMap.LoadAndDelete(userID)
	if ok && si != nil {
		slf.RemoveSession(si.(*SessionInfo).SessionID)
	}
}

func (slf *sessionManager) GetSession(sessionID string) *SessionInfo {
	si, ok := slf.sessionMap.Load(sessionID)
	if !ok {
		return nil
	}
	return si.(*SessionInfo)
}

func (slf *sessionManager) GetSessionByUserID(userID kknet.USER_ID) *SessionInfo {
	si, ok := slf.userMap.Load(userID)
	if !ok {
		return nil
	}
	return si.(*SessionInfo)
}

func (slf *sessionManager) CheckKickOutUser(sessionID string, userID kknet.USER_ID) string {
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

func (slf *sessionManager) Login(sessionID string, userID kknet.USER_ID) bool {
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
	return true
}

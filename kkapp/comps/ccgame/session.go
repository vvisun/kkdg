package ccgame

import "sync"

type SessionInfo struct {
	SessionID  string
	GateNodeID string
}

var sessionInfoPool = sync.Pool{
	New: func() interface{} {
		return &SessionInfo{}
	},
}

func getSessionInfo() *SessionInfo {
	return sessionInfoPool.Get().(*SessionInfo)
}
func putSessionInfo(si *SessionInfo) {
	si.SessionID = ""
	si.GateNodeID = ""
	sessionInfoPool.Put(si)
}

type sessionManager struct {
	sessionMap sync.Map // map[string]SessionInfo
}

func newSessionManager() *sessionManager {
	return &sessionManager{}
}

func (slf *sessionManager) AddSession(sessionID string, sessionInfo SessionInfo) {
	si := getSessionInfo()
	si.SessionID = sessionID
	si.GateNodeID = sessionInfo.GateNodeID
	slf.sessionMap.Store(sessionID, si)
}

func (slf *sessionManager) RemoveSession(sessionID string) {
	sessionInfo, ok := slf.sessionMap.LoadAndDelete(sessionID)
	if ok {
		putSessionInfo(sessionInfo.(*SessionInfo))
	}
}

func (slf *sessionManager) GetSession(sessionID string) (*SessionInfo, bool) {
	sessionInfo, ok := slf.sessionMap.Load(sessionID)
	if !ok {
		return nil, false
	}
	return sessionInfo.(*SessionInfo), true
}

package ccgame

type SessionInfo struct {
	SessionID  string
	GateNodeID string
}

type sessionManager struct {
	sessionMap map[string]SessionInfo
}

func newSessionManager() *sessionManager {
	return &sessionManager{
		sessionMap: make(map[string]SessionInfo),
	}
}

func (slf *sessionManager) AddSession(sessionID string, sessionInfo SessionInfo) {
	slf.sessionMap[sessionID] = sessionInfo
}

func (slf *sessionManager) RemoveSession(sessionID string) {
	delete(slf.sessionMap, sessionID)
}

func (slf *sessionManager) GetSession(sessionID string) (*SessionInfo, bool) {
	sessionInfo, ok := slf.sessionMap[sessionID]
	if !ok {
		return nil, false
	}
	return &sessionInfo, true
}

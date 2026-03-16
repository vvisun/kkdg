package ccgate

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/user"
	"github.com/vvisun/kkdg/utils/kklog"
)

type kickInfo struct {
	userId    user.USER_ID
	sessionId string
}

type userManager struct {
	mu      sync.Mutex
	uid2sid map[user.USER_ID]string //user.USER_ID -> sessionId
	sid2uid map[string]user.USER_ID //sessionId -> user.USER_ID
}

func newUserManager() *userManager {
	return &userManager{
		uid2sid: make(map[user.USER_ID]string),
		sid2uid: make(map[string]user.USER_ID),
	}
}

// 用户登录时，记录用户与会话的绑定关系
// 返回需要踢出的会话ID列表。需要投递给业务回调，发送顶号消息给被踢的连接。
//
//	踢出逻辑：
//	1. 如果userId已经登录了其他会话，需踢出旧的会话。
//	2. 如果当前会话已经登录了其他用户，需踢出该其他用户。
func (m *userManager) addUser(userId user.USER_ID, curSessionId string) []kickInfo {
	if curSessionId == "" {
		kklog.Errorf("addUser: sessionId is empty, userId: %d", userId)
		return nil
	}

	kickList := make([]kickInfo, 0)

	m.mu.Lock()

	// 如果userId已经登录了其他会话，需踢出旧的会话
	if oldSid, ok := m.uid2sid[userId]; ok {
		if oldSid != "" && oldSid != curSessionId {
			oldUid, ok := m.sid2uid[oldSid]
			if ok {
				delete(m.uid2sid, oldUid)
			}
			delete(m.sid2uid, oldSid)
			kklog.Debugf("kick out old user %d, sessionId: %s", oldUid, oldSid)
			kickList = append(kickList, kickInfo{userId: oldUid, sessionId: oldSid})
		}
	}

	// 如果当前会话已经登录了其他用户，需踢出该其他用户。理论上不可能，但是依旧防御性检查
	if otherUid, ok := m.sid2uid[curSessionId]; ok {
		if otherUid != user.NULL_USER_ID && otherUid != userId {
			otherSid, ok := m.uid2sid[otherUid]
			if ok {
				delete(m.sid2uid, otherSid)
				kickList = append(kickList, kickInfo{userId: otherUid, sessionId: otherSid})
			}
			delete(m.uid2sid, otherUid)
			kklog.Debugf("kick out other user %d, sessionId: %s", otherUid, otherSid)
		}
	}

	m.uid2sid[userId] = curSessionId
	m.sid2uid[curSessionId] = userId
	m.mu.Unlock()

	return kickList
}

// 用户登出时，清除所有记录
func (m *userManager) removeUser(userId user.USER_ID) {
	m.mu.Lock()
	if sid, ok := m.uid2sid[userId]; ok {
		delete(m.sid2uid, sid)
	}
	delete(m.uid2sid, userId)
	m.mu.Unlock()
}

// 会话断开时，移除【userId - sessionId】绑定关系。
func (m *userManager) onSessionDisconnect(sessionId string) {
	m.mu.Lock()
	if userId, ok := m.sid2uid[sessionId]; ok {
		delete(m.uid2sid, userId)
	}
	delete(m.sid2uid, sessionId)
	m.mu.Unlock()
}

// 获取会话ID对应的用户ID。
func (m *userManager) getUserIdBySessionId(sessionId string) (user.USER_ID, bool) {
	m.mu.Lock()
	v, ok := m.sid2uid[sessionId]
	m.mu.Unlock()
	if !ok {
		return user.NULL_USER_ID, false
	}
	return v, true
}

// 获取用户ID对应的会话ID。
func (m *userManager) getSessionIdByUserId(userId user.USER_ID) (string, bool) {
	m.mu.Lock()
	v, ok := m.uid2sid[userId]
	m.mu.Unlock()
	if !ok {
		return "", false
	}
	return v, true
}

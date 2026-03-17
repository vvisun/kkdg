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

// 用户管理。
//  1. 用户登入时加入，登出时移除。
//  2. 管理用户ID与sessionId的映射关系。
type userManager struct {
	mu      sync.RWMutex
	uid2sid map[user.USER_ID]string //user.USER_ID -> sessionId
	sid2uid map[string]user.USER_ID //sessionId -> user.USER_ID
}

func newUserManager() *userManager {
	return &userManager{
		uid2sid: make(map[user.USER_ID]string),
		sid2uid: make(map[string]user.USER_ID),
	}
}

// 连接curSessionId处，用户登入账号userId时，检查是否需要踢出
// 返回需要踢出的会话ID列表。需要投递给业务回调，发送顶号消息给被踢的连接。
//
//	踢出逻辑：
//	1. 如果userId已经登录了其他会话，需踢出旧的会话。
//	2. 如果当前会话已经登录了其他用户，需踢出该其他用户。
func (m *userManager) checkKick(userId user.USER_ID, curSessionId string) []kickInfo {
	var kickList []kickInfo = nil

	// 如果userId已经登录了其他会话，需踢出旧的会话
	if oldSid, ok := m.uid2sid[userId]; ok {
		if oldSid != "" && oldSid != curSessionId {
			oldUid, ok := m.sid2uid[oldSid]
			if ok {
				if m.uid2sid[oldUid] != "" && m.uid2sid[oldUid] != oldSid {
					// 旧的uid和sid的绑定关系不一致，说明出bug了。
					kklog.Errorf("checkKick: userId: %d, curSessionId: %s, oldUid: %d, oldSid: %s",
						userId, curSessionId, oldUid, oldSid,
					)
				}
				delete(m.uid2sid, oldUid)
			}
			delete(m.sid2uid, oldSid)
			kklog.Debugf("kick out old user %d, sessionId: %s", oldUid, oldSid)
			kickList = append(kickList, kickInfo{userId: oldUid, sessionId: oldSid})
		}
	}

	// 如果当前会话已经登录了其他用户，需踢出该其他用户。
	if otherUid, ok := m.sid2uid[curSessionId]; ok {
		if otherUid != user.NULL_USER_ID && otherUid != userId {
			otherSid, ok := m.uid2sid[otherUid]
			if ok {
				if m.sid2uid[otherSid] != user.NULL_USER_ID && m.sid2uid[otherSid] != otherUid {
					// 其他用户的uid和sid的绑定关系不一致，说明出bug了。
					kklog.Errorf("checkKick: userId: %d, curSessionId: %s, otherUid: %d, otherSid: %s",
						userId, curSessionId, otherUid, otherSid,
					)
				}
				delete(m.sid2uid, otherSid)
				kickList = append(kickList, kickInfo{userId: otherUid, sessionId: otherSid})
			}
			delete(m.uid2sid, otherUid)
			kklog.Debugf("kick out other user %d, sessionId: %s", otherUid, otherSid)
		}
	}

	return kickList
}

// 用户登录某逻辑服时
//
//	返回需要踢出的会话ID列表。需要投递给业务回调，发送顶号消息给被踢的连接。
//	踢出逻辑详见checkKick
func (m *userManager) onUserLogin(userId user.USER_ID, curSessionId string) []kickInfo {
	if curSessionId == "" {
		kklog.Errorf("onUserLogin: sessionId is empty, userId: %d", userId)
		return nil
	}
	if userId == user.NULL_USER_ID {
		kklog.Errorf("onUserLogin: userId is empty, curSessionId: %s", curSessionId)
		return nil
	}

	m.mu.Lock()
	kickList := m.checkKick(userId, curSessionId)
	m.uid2sid[userId] = curSessionId
	m.sid2uid[curSessionId] = userId
	m.mu.Unlock()

	return kickList
}

// 用户登出某逻辑服时
func (m *userManager) onUserLogout(userId user.USER_ID) {
	m.mu.Lock()
	if sid, ok := m.uid2sid[userId]; ok {
		// 这里不删sessionId, 因为只是登出，不是断线。单纯将值置为空值，表示该连接处已无用户登录。
		m.sid2uid[sid] = user.NULL_USER_ID
	}
	delete(m.uid2sid, userId)
	m.mu.Unlock()
}

// 会话断开时，移除【userId - sessionId】绑定关系。
func (m *userManager) onSessionDisconnect(sessionId string) {
	m.mu.Lock()
	if userId, ok := m.sid2uid[sessionId]; ok {
		// 这里不删userId, 因为只是断线，不是退出登录。单纯将值置为空值，表示离线。
		m.uid2sid[userId] = ""
	}
	delete(m.sid2uid, sessionId)
	m.mu.Unlock()
}

// 获取会话ID对应的用户ID。
func (m *userManager) getUserIdBySessionId(sessionId string) (user.USER_ID, bool) {
	m.mu.RLock()
	v, ok := m.sid2uid[sessionId]
	m.mu.RUnlock()
	if !ok {
		return user.NULL_USER_ID, false
	}
	return v, true
}

// 获取用户ID对应的会话ID。
func (m *userManager) getSessionIdByUserId(userId user.USER_ID) (string, bool) {
	m.mu.RLock()
	v, ok := m.uid2sid[userId]
	m.mu.RUnlock()
	if !ok {
		return "", false
	}
	return v, true
}

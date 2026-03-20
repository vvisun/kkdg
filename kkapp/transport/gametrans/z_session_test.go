package gametrans

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vvisun/kkdg/kkapp/user"
)

func TestSessionManager_AddGetRemove(t *testing.T) {
	mgr := NewSessionManager(4)

	if mgr.OnlineCount() != 0 || mgr.UserCount() != 0 {
		t.Fatalf("initial counts = online=%d user=%d, want 0,0", mgr.OnlineCount(), mgr.UserCount())
	}

	// 空时 Get 返回 nil
	if got := mgr.GetSession("s1"); got != nil {
		t.Fatalf("GetSession empty = %v, want nil", got)
	}

	// Add 后 Get 能取到，且字段正确
	mgr.AddSession("s1", "gate1")
	si := mgr.GetSession("s1")
	if si == nil {
		t.Fatal("GetSession(s1) = nil after AddSession")
	}
	if si.sessionID != "s1" || si.gateNodeID != "gate1" {
		t.Errorf("GetSession(s1) = SessionID=%q GateNodeID=%q, want s1, gate1", si.sessionID, si.gateNodeID)
	}
	if si.userID != user.NULL_USER_ID {
		t.Errorf("GetSession(s1).UserID = %v, want NULL_USER_ID", si.userID)
	}

	if mgr.OnlineCount() != 1 || mgr.UserCount() != 0 {
		t.Fatalf("after AddSession: online=%d user=%d, want 1,0", mgr.OnlineCount(), mgr.UserCount())
	}

	// Remove 后 Get 返回 nil
	mgr.RemoveSession("s1")
	if got := mgr.GetSession("s1"); got != nil {
		t.Fatalf("GetSession(s1) after Remove = %v, want nil", got)
	}
	if mgr.OnlineCount() != 0 || mgr.UserCount() != 0 {
		t.Fatalf("after RemoveSession: online=%d user=%d, want 0,0", mgr.OnlineCount(), mgr.UserCount())
	}
}

func TestSessionManager_AddSession_Idempotent(t *testing.T) {
	mgr := NewSessionManager(4)

	mgr.AddSession("s1", "gate1")
	mgr.AddSession("s1", "gate2") // 已存在，应忽略，不覆盖
	si := mgr.GetSession("s1")
	if si == nil {
		t.Fatal("GetSession(s1) = nil")
	}
	if si.gateNodeID != "gate1" {
		t.Errorf("AddSession idempotent: GateNodeID = %q, want gate1", si.gateNodeID)
	}
}

func TestSessionManager_GetSessionByUserID_AfterLogin(t *testing.T) {
	mgr := NewSessionManager(4)

	mgr.AddSession("s1", "gate1")
	if mgr.GetSessionByUserID(100) != nil {
		t.Error("GetSessionByUserID before Login should be nil")
	}

	ok := mgr.Login("s1", 100, nil)
	if !ok {
		t.Fatal("Login(s1, 100) = false")
	}

	si := mgr.GetSessionByUserID(100)
	if si == nil {
		t.Fatal("GetSessionByUserID(100) = nil after Login")
	}
	if si.sessionID != "s1" || si.userID != 100 {
		t.Errorf("GetSessionByUserID(100) = SessionID=%q UserID=%v, want s1, 100", si.sessionID, si.userID)
	}
}

func TestSessionManager_Login_NullUserID_ReturnsFalse(t *testing.T) {
	mgr := NewSessionManager(4)
	mgr.AddSession("s1", "gate1")

	if mgr.Login("s1", user.NULL_USER_ID, nil) {
		t.Error("Login(s1, NULL_USER_ID) = true, want false")
	}
	if mgr.UserCount() != 0 {
		t.Errorf("UserCount after Login with NULL_USER_ID = %d, want 0", mgr.UserCount())
	}
	if mgr.OnlineCount() != 1 {
		t.Errorf("OnlineCount after Login with NULL_USER_ID = %d, want 1", mgr.OnlineCount())
	}
}

func TestSessionManager_Login_NoSession_ReturnsFalse(t *testing.T) {
	mgr := NewSessionManager(4)

	if mgr.Login("nonexist", 100, nil) {
		t.Error("Login(nonexist, 100) = true, want false")
	}
	if mgr.UserCount() != 0 || mgr.OnlineCount() != 0 {
		t.Errorf("counts after Login(nonexist): online=%d user=%d, want 0,0", mgr.OnlineCount(), mgr.UserCount())
	}
}

func TestSessionManager_CheckKickOutUser_SameSessionSameUser_NoKick(t *testing.T) {
	mgr := NewSessionManager(4)
	mgr.AddSession("s1", "gate1")
	mgr.Login("s1", 100, nil)

	kick := mgr.CheckKickOutUser("s1", 100)
	if kick != "" {
		t.Errorf("CheckKickOutUser(s1, 100) = %q, want \"\" (no kick)", kick)
	}
}

func TestSessionManager_CheckKickOutUser_OtherSessionSameUser_ReturnsOtherSessionID(t *testing.T) {
	mgr := NewSessionManager(4)
	mgr.AddSession("s1", "gate1")
	mgr.AddSession("s2", "gate1")
	mgr.Login("s1", 100, nil)

	// 同一用户从 s2 登录，应踢出 s1
	kick := mgr.CheckKickOutUser("s2", 100)
	if kick != "s1" {
		t.Errorf("CheckKickOutUser(s2, 100) = %q, want s1", kick)
	}
}

func TestSessionManager_Login_KicksOtherSessionWithSameUser(t *testing.T) {
	mgr := NewSessionManager(4)
	mgr.AddSession("s1", "gate1")
	mgr.AddSession("s2", "gate1")
	mgr.Login("s1", 100, nil)
	if mgr.OnlineCount() != 2 || mgr.UserCount() != 1 {
		t.Fatalf("after first login: online=%d user=%d, want 2,1", mgr.OnlineCount(), mgr.UserCount())
	}

	// s2 登录同一用户，应踢掉 s1，然后 s2 绑定 100
	ok := mgr.Login("s2", 100, nil)
	if !ok {
		t.Fatal("Login(s2, 100) = false")
	}

	if mgr.GetSession("s1") != nil {
		t.Error("session s1 should be removed after kick")
	}
	si := mgr.GetSessionByUserID(100)
	if si == nil || si.sessionID != "s2" {
		t.Errorf("GetSessionByUserID(100) = %v, want session s2", si)
	}
	if mgr.OnlineCount() != 1 || mgr.UserCount() != 1 {
		t.Fatalf("after second login (kick): online=%d user=%d, want 1,1", mgr.OnlineCount(), mgr.UserCount())
	}
}

func TestSessionManager_RemoveSession_CleansUserMap(t *testing.T) {
	mgr := NewSessionManager(4)
	mgr.AddSession("s1", "gate1")
	mgr.Login("s1", 100, nil)
	if mgr.OnlineCount() != 1 || mgr.UserCount() != 1 {
		t.Fatalf("before RemoveSession: online=%d user=%d, want 1,1", mgr.OnlineCount(), mgr.UserCount())
	}

	mgr.RemoveSession("s1")
	if mgr.GetSessionByUserID(100) != nil {
		t.Error("GetSessionByUserID(100) after RemoveSession(s1) should be nil")
	}
	if mgr.OnlineCount() != 0 || mgr.UserCount() != 0 {
		t.Fatalf("after RemoveSession: online=%d user=%d, want 0,0", mgr.OnlineCount(), mgr.UserCount())
	}
}

func TestSessionManager_RemoveSessionByUserID(t *testing.T) {
	mgr := NewSessionManager(4)
	mgr.AddSession("s1", "gate1")
	mgr.AddSession("s2", "gate2")
	if !mgr.Login("s1", 100, nil) || !mgr.Login("s2", 200, nil) {
		t.Fatal("Login failed")
	}
	if mgr.OnlineCount() != 2 || mgr.UserCount() != 2 {
		t.Fatalf("before RemoveSessionByUserID: online=%d user=%d, want 2,2", mgr.OnlineCount(), mgr.UserCount())
	}

	mgr.RemoveSessionByUserID(100)
	if mgr.GetSession("s1") != nil {
		t.Error("session s1 should be removed after RemoveSessionByUserID(100)")
	}
	if mgr.GetSessionByUserID(100) != nil {
		t.Error("user 100 should be removed from userMap after RemoveSessionByUserID")
	}
	if mgr.OnlineCount() != 1 || mgr.UserCount() != 1 {
		t.Fatalf("after RemoveSessionByUserID(100): online=%d user=%d, want 1,1", mgr.OnlineCount(), mgr.UserCount())
	}

	// 删除不存在的 userID 不应改变计数
	mgr.RemoveSessionByUserID(999)
	if mgr.OnlineCount() != 1 || mgr.UserCount() != 1 {
		t.Fatalf("after RemoveSessionByUserID(999): online=%d user=%d, want 1,1", mgr.OnlineCount(), mgr.UserCount())
	}
}

// 测试重复释放SessionInfo到池中
func TestSessionManager_PutSessionInfo_Duplicate(t *testing.T) {
	si := newSessionInfo()
	si.sessionID = "s1"
	si.gateNodeID = "gate1"
	si.userID = 100
	if si.isInPool.Load() {
		t.Error("SessionInfo should not be in pool")
	}
	putSessionInfo(si)
	if !si.isInPool.Load() {
		t.Error("SessionInfo should be in pool after put")
	}
	putSessionInfo(si)
}

// 并发场景测试：不同会话/用户在多个 goroutine 中同时增删，确保计数最终一致且无 panic。
func TestSessionManager_ConcurrentAddLoginRemove_DisjointSessions(t *testing.T) {
	mgr := NewSessionManager(4)

	const goroutines = 8
	const perG = 256

	var wg sync.WaitGroup
	var loginFailCount atomic.Int32
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			base := g * perG
			for i := 0; i < perG; i++ {
				// 保证每个 goroutine 负责一段不重叠的 session/user 区间，避免同一 SessionInfo 被多 goroutine 同时操作。
				id := base + i
				sessionID := fmt.Sprintf("s-%d", id)
				gateID := fmt.Sprintf("gate-%d", g)
				userID := user.USER_ID(id + 1)

				mgr.AddSession(sessionID, gateID)

				ok := mgr.Login(sessionID, userID, nil)
				if !ok {
					loginFailCount.Add(1)
				}

				// 使用 RemoveSessionByUserID 删除，间接校验 userMap 与 sessionMap 的一致性。
				mgr.RemoveSessionByUserID(userID)
			}
		}()
	}

	wg.Wait()

	if n := loginFailCount.Load(); n != 0 {
		t.Errorf("Login failed %d times in concurrent test", n)
	}
	if gotOnline, gotUser := mgr.OnlineCount(), mgr.UserCount(); gotOnline != 0 || gotUser != 0 {
		t.Fatalf("after concurrent add/login/remove: online=%d user=%d, want 0,0", gotOnline, gotUser)
	}
}

package ccgate

import (
	"strconv"
	"testing"

	"github.com/vvisun/kkdg/kkapp/user"
)

func TestUserManager_OnUserLogin_FirstLogin(t *testing.T) {
	mgr := newUserManager()
	uid := user.USER_ID(1)
	sid := "session-1"

	kicks := mgr.onUserLogin(uid, sid)
	if len(kicks) != 0 {
		t.Fatalf("expected no kicks on first login, got %d", len(kicks))
	}

	if gotSid, ok := mgr.getSessionIdByUserId(uid); !ok || gotSid != sid {
		t.Fatalf("expected sessionId %q for user %d, got %q, ok=%v", sid, uid, gotSid, ok)
	}
	if gotUid, ok := mgr.getUserIdBySessionId(sid); !ok || gotUid != uid {
		t.Fatalf("expected userId %d for session %q, got %d, ok=%v", uid, sid, gotUid, ok)
	}
}

func TestUserManager_OnUserLogin_KickOldSession(t *testing.T) {
	mgr := newUserManager()
	uid := user.USER_ID(1)
	sid1 := "session-1"
	sid2 := "session-2"

	if len(mgr.onUserLogin(uid, sid1)) != 0 {
		t.Fatalf("expected no kicks on first login")
	}

	kicks := mgr.onUserLogin(uid, sid2)
	if len(kicks) != 1 {
		t.Fatalf("expected 1 kick when user relogin, got %d", len(kicks))
	}
	if kicks[0].userId != uid || kicks[0].sessionId != sid1 {
		t.Fatalf("unexpected kickInfo: %+v", kicks[0])
	}

	if gotSid, ok := mgr.getSessionIdByUserId(uid); !ok || gotSid != sid2 {
		t.Fatalf("expected sessionId %q for user %d, got %q, ok=%v", sid2, uid, gotSid, ok)
	}
	if _, ok := mgr.getUserIdBySessionId(sid1); ok {
		t.Fatalf("expected no user bound to old session %q", sid1)
	}
	if gotUid, ok := mgr.getUserIdBySessionId(sid2); !ok || gotUid != uid {
		t.Fatalf("expected userId %d for new session %q, got %d, ok=%v", uid, sid2, gotUid, ok)
	}
}

func TestUserManager_OnUserLogin_KickOtherUserOnSameSession(t *testing.T) {
	mgr := newUserManager()
	uid1 := user.USER_ID(1)
	uid2 := user.USER_ID(2)
	sid := "session-1"

	if len(mgr.onUserLogin(uid1, sid)) != 0 {
		t.Fatalf("expected no kicks on first login")
	}

	kicks := mgr.onUserLogin(uid2, sid)
	if len(kicks) != 1 {
		t.Fatalf("expected 1 kick when other user login same session, got %d", len(kicks))
	}
	if kicks[0].userId != uid1 || kicks[0].sessionId != sid {
		t.Fatalf("unexpected kickInfo: %+v", kicks[0])
	}

	if _, ok := mgr.getSessionIdByUserId(uid1); ok {
		t.Fatalf("expected no session bound for kicked user %d", uid1)
	}
	if gotSid, ok := mgr.getSessionIdByUserId(uid2); !ok || gotSid != sid {
		t.Fatalf("expected sessionId %q for user %d, got %q, ok=%v", sid, uid2, gotSid, ok)
	}
}

func TestUserManager_OnUserLogout(t *testing.T) {
	mgr := newUserManager()
	uid := user.USER_ID(1)
	sid := "session-1"

	if len(mgr.onUserLogin(uid, sid)) != 0 {
		t.Fatalf("expected no kicks on first login")
	}

	mgr.onUserLogout(uid)

	if _, ok := mgr.getSessionIdByUserId(uid); ok {
		t.Fatalf("expected no session for user after logout")
	}
	if gotUid, ok := mgr.getUserIdBySessionId(sid); !ok || gotUid != user.NULL_USER_ID {
		t.Fatalf("expected NULL_USER_ID for session %q after logout, got %d, ok=%v", sid, gotUid, ok)
	}
}

func TestUserManager_OnSessionDisconnect(t *testing.T) {
	mgr := newUserManager()
	uid := user.USER_ID(1)
	sid := "session-1"

	if len(mgr.onUserLogin(uid, sid)) != 0 {
		t.Fatalf("expected no kicks on first login")
	}

	mgr.onSessionDisconnect(sid)

	if gotUid, ok := mgr.getUserIdBySessionId(sid); ok {
		t.Fatalf("expected no user for disconnected session, got %d", gotUid)
	}
	if gotSid, ok := mgr.getSessionIdByUserId(uid); !ok || gotSid != "" {
		t.Fatalf("expected empty sessionId but present mapping for user %d, got %q, ok=%v", uid, gotSid, ok)
	}
}

// Benchmark: 连续登录同一用户（覆盖 checkKick 分支）
func BenchmarkUserManager_OnUserLogin_SameUser(b *testing.B) {
	mgr := newUserManager()
	uid := user.USER_ID(1)

	for i := 0; i < b.N; i++ {
		sid := "session-" + strconv.Itoa(i)
		mgr.onUserLogin(uid, sid)
	}
}

// Benchmark: 大量不同用户并发登录
func BenchmarkUserManager_OnUserLogin_MultiUsers(b *testing.B) {
	mgr := newUserManager()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			uid := user.USER_ID(i)
			sid := "session-" + strconv.Itoa(i)
			mgr.onUserLogin(uid, sid)
			i++
		}
	})
}

// Benchmark: 会话断开处理
func BenchmarkUserManager_OnSessionDisconnect(b *testing.B) {
	mgr := newUserManager()

	// 预先填充一定数量的用户与会话
	const preCount = 10000
	for i := 0; i < preCount; i++ {
		uid := user.USER_ID(i)
		sid := "session-" + strconv.Itoa(i)
		mgr.onUserLogin(uid, sid)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sid := "session-" + strconv.Itoa(i%preCount)
		mgr.onSessionDisconnect(sid)
	}
}



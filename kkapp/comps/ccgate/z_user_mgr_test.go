package ccgate

import (
	"strconv"
	"testing"

	"github.com/vvisun/kkdg/kkapp/user"
)

// Test basic add / remove / query behavior of userManager without kicks.
func Test_userManager_basic(t *testing.T) {
	m := newUserManager()

	uid := user.USER_ID(1)
	sid := "s1"

	// first login should not kick anyone
	if kicks := m.addUser(uid, sid); kicks != nil {
		t.Fatalf("addUser first login kicks = %#v, want none", kicks)
	}

	// query mappings
	if gotSid, ok := m.getSessionIdByUserId(uid); !ok || gotSid != sid {
		t.Fatalf("getSessionIdByUserId(%d) = (%q,%v), want (%q,true)", uid, gotSid, ok, sid)
	}
	if gotUid, ok := m.getUserIdBySessionId(sid); !ok || gotUid != uid {
		t.Fatalf("getUserIdBySessionId(%q) = (%d,%v), want (%d,true)", sid, gotUid, ok, uid)
	}

	// remove and verify
	m.removeUser(uid)
	if _, ok := m.getSessionIdByUserId(uid); ok {
		t.Fatalf("getSessionIdByUserId(%d) ok after remove, want false", uid)
	}
	if _, ok := m.getUserIdBySessionId(sid); ok {
		t.Fatalf("getUserIdBySessionId(%q) ok after remove, want false", sid)
	}
}

// Test kicking old session when same user logs in again.
func Test_userManager_addUser_kickOldSession(t *testing.T) {
	m := newUserManager()

	uid := user.USER_ID(10)
	oldSid := "s-old"
	newSid := "s-new"

	// first login
	_ = m.addUser(uid, oldSid)

	// second login with same user, different session
	kicks := m.addUser(uid, newSid)
	if len(kicks) != 1 {
		t.Fatalf("addUser second login kicks len = %d, want 1", len(kicks))
	}
	if kicks[0].userId != uid || kicks[0].sessionId != oldSid {
		t.Fatalf("kick info = %+v, want userId=%d, sessionId=%q", kicks[0], uid, oldSid)
	}

	// mapping should point to newSid
	if gotSid, ok := m.getSessionIdByUserId(uid); !ok || gotSid != newSid {
		t.Fatalf("getSessionIdByUserId(%d) = (%q,%v), want (%q,true)", uid, gotSid, ok, newSid)
	}
	if gotUid, ok := m.getUserIdBySessionId(newSid); !ok || gotUid != uid {
		t.Fatalf("getUserIdBySessionId(%q) = (%d,%v), want (%d,true)", newSid, gotUid, ok, uid)
	}
	// old session should be gone
	if _, ok := m.getUserIdBySessionId(oldSid); ok {
		t.Fatalf("getUserIdBySessionId(%q) ok after re-login, want false", oldSid)
	}
}

// Test kicking "other user on same session" defensive path.
func Test_userManager_addUser_kickOtherUserOnSameSession(t *testing.T) {
	m := newUserManager()

	uid1 := user.USER_ID(1)
	uid2 := user.USER_ID(2)
	sid := "s1"

	// first bind uid1 -> sid
	_ = m.addUser(uid1, sid)

	// force a conflicting mapping: sid -> uid1 already, now uid2 logs in on same sid
	kicks := m.addUser(uid2, sid)

	if len(kicks) != 1 {
		t.Fatalf("addUser conflicting login kicks len = %d, want 1", len(kicks))
	}
	if kicks[0].userId != uid1 || kicks[0].sessionId == "" {
		t.Fatalf("kick info = %+v, want userId=%d and non-empty sessionId", kicks[0], uid1)
	}

	// now mapping should point sid -> uid2
	if gotUid, ok := m.getUserIdBySessionId(sid); !ok || gotUid != uid2 {
		t.Fatalf("getUserIdBySessionId(%q) = (%d,%v), want (%d,true)", sid, gotUid, ok, uid2)
	}
}

// Test onSessionDisconnect clears mapping.
func Test_userManager_onSessionDisconnect(t *testing.T) {
	m := newUserManager()

	uid := user.USER_ID(100)
	sid := "s-100"

	_ = m.addUser(uid, sid)

	m.onSessionDisconnect(sid)

	if _, ok := m.getSessionIdByUserId(uid); ok {
		t.Fatalf("getSessionIdByUserId(%d) ok after onSessionDisconnect, want false", uid)
	}
	if _, ok := m.getUserIdBySessionId(sid); ok {
		t.Fatalf("getUserIdBySessionId(%q) ok after onSessionDisconnect, want false", sid)
	}
}

// Benchmark addUser under sequential usage.
func Benchmark_userManager_addUser(b *testing.B) {
	m := newUserManager()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		uid := user.USER_ID(i + 1)
		sid := "s-" + strconv.Itoa(i)
		_ = m.addUser(uid, sid)
	}
}

// Benchmark get / remove under sequential usage.
func Benchmark_userManager_get_remove(b *testing.B) {
	m := newUserManager()
	for i := 0; i < b.N; i++ {
		uid := user.USER_ID(i + 1)
		sid := "s-" + strconv.Itoa(i)
		_ = m.addUser(uid, sid)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		uid := user.USER_ID(i + 1)
		_, _ = m.getSessionIdByUserId(uid)
		m.removeUser(uid)
	}
}

// Benchmark add / get / remove in parallel to stress the mutex.
func Benchmark_userManager_add_get_remove_parallel(b *testing.B) {
	m := newUserManager()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			uid := user.USER_ID(i + 1)
			sid := "s-" + strconv.Itoa(i)
			_ = m.addUser(uid, sid)
			_, _ = m.getSessionIdByUserId(uid)
			m.removeUser(uid)
			i++
		}
	})
}

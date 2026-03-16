package ccgate

import (
	"strconv"
	"testing"

	"github.com/vvisun/kkdg/kkapp/user"
)

// Test clientBindTable basic bind / get / unbind / range behavior.
func Test_clientBindTable_basic(t *testing.T) {
	tbl := newClientBindTable()

	itemA1 := tbl.bindLogicItem("game", "node-game-1")
	if itemA1 == nil || itemA1.nodeId != "node-game-1" || itemA1.nodeType != "game" {
		t.Fatalf("bindLogicItem(game,node-game-1) = %+v, want nodeId=node-game-1,nodeType=game", itemA1)
	}

	// bind same nodeType should return existing item
	itemA2 := tbl.bindLogicItem("game", "node-game-2")
	if itemA2 != itemA1 {
		t.Fatalf("bindLogicItem for same nodeType should return same pointer, got %+v, want %+v", itemA2, itemA1)
	}

	// bind another nodeType
	itemB := tbl.bindLogicItem("chat", "node-chat-1")
	if itemB == nil || itemB.nodeId != "node-chat-1" || itemB.nodeType != "chat" {
		t.Fatalf("bindLogicItem(chat,node-chat-1) = %+v, want nodeId=node-chat-1,nodeType=chat", itemB)
	}

	// getLogicItem
	if got := tbl.getLogicItem("game"); got != itemA1 {
		t.Fatalf("getLogicItem(game) = %+v, want %+v", got, itemA1)
	}
	if got := tbl.getLogicItem("chat"); got != itemB {
		t.Fatalf("getLogicItem(chat) = %+v, want %+v", got, itemB)
	}
	if got := tbl.getLogicItem("not-exist"); got != nil {
		t.Fatalf("getLogicItem(not-exist) = %+v, want nil", got)
	}

	// rangeLogicItems should visit both entries
	visited := make(map[string]*clientLogicItem)
	tbl.rangeLogicItems(func(nodeType string, logicItem *clientLogicItem) bool {
		visited[nodeType] = logicItem
		return true
	})
	if len(visited) != 2 || visited["game"] != itemA1 || visited["chat"] != itemB {
		t.Fatalf("rangeLogicItems visited = %+v, want game and chat items", visited)
	}

	// unbind one nodeType
	tbl.unbindLogicItem("game")
	if got := tbl.getLogicItem("game"); got != nil {
		t.Fatalf("getLogicItem(game) after unbind = %+v, want nil", got)
	}
	if got := tbl.getLogicItem("chat"); got != itemB {
		t.Fatalf("getLogicItem(chat) after unbind(game) = %+v, want %+v", got, itemB)
	}
}

// Test logicBindManager session / user bind and queries.
func Test_logicBindManager_basic(t *testing.T) {
	m := newLogicBindManager()

	const sessionID = "s1"
	const nodeType = "game"
	const nodeID = "node-1"

	// sessionBind should create and return an item
	item := m.sessionBind(sessionID, nodeType, nodeID)
	if item == nil || item.nodeId != nodeID || item.nodeType != nodeType {
		t.Fatalf("sessionBind = %+v, want nodeId=%q,nodeType=%q", item, nodeID, nodeType)
	}

	// getLogicItemBySessionId should see the same item
	if got := m.getLogicItemBySessionId(sessionID, nodeType); got != item {
		t.Fatalf("getLogicItemBySessionId = %+v, want %+v", got, item)
	}

	// sessionBindTable should return the same bind table
	bindTbl := m.getSessionBindTable(sessionID)
	if bindTbl == nil {
		t.Fatalf("sessionBindTable(%q) = nil, want non-nil", sessionID)
	}
	if got := bindTbl.getLogicItem(nodeType); got != item {
		t.Fatalf("bindTbl.getLogicItem(%q) = %+v, want %+v", nodeType, got, item)
	}

	// userBind should create mapping for user
	uid := user.USER_ID(10)
	uitem := m.userBind(uid, nodeType, nodeID)
	if uitem == nil || uitem.nodeId != nodeID || uitem.nodeType != nodeType {
		t.Fatalf("userBind = %+v, want nodeId=%q,nodeType=%q", uitem, nodeID, nodeType)
	}

	if got := m.getLogicItemByUserId(uid, nodeType); got != uitem {
		t.Fatalf("getLogicItemByUserId = %+v, want %+v", got, uitem)
	}

	if utbl := m.getUserBindTable(uid); utbl == nil {
		t.Fatalf("userBindTable(%d) = nil, want non-nil", uid)
	}

	// unbind session and user
	m.sessionUnbind(sessionID, nodeType)
	if got := m.getLogicItemBySessionId(sessionID, nodeType); got != nil {
		t.Fatalf("getLogicItemBySessionId after sessionUnbind = %+v, want nil", got)
	}

	m.userUnbind(uid, nodeType)
	if got := m.getLogicItemByUserId(uid, nodeType); got != nil {
		t.Fatalf("getLogicItemByUserId after userUnbind = %+v, want nil", got)
	}
}

// Benchmark clientBindTable bind / get / unbind sequentially.
func Benchmark_clientBindTable_bind_get_unbind(b *testing.B) {
	tbl := newClientBindTable()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeType := "type-" + strconv.Itoa(i%8)
		nodeID := "node-" + strconv.Itoa(i%16)
		_ = tbl.bindLogicItem(nodeType, nodeID)
		_ = tbl.getLogicItem(nodeType)
		tbl.unbindLogicItem(nodeType)
	}
}

// Benchmark clientBindTable bind / get / unbind in parallel to stress mutex.
func Benchmark_clientBindTable_bind_get_unbind_parallel(b *testing.B) {
	tbl := newClientBindTable()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			nodeType := "type-" + strconv.Itoa(i%8)
			nodeID := "node-" + strconv.Itoa(i%16)
			_ = tbl.bindLogicItem(nodeType, nodeID)
			_ = tbl.getLogicItem(nodeType)
			tbl.unbindLogicItem(nodeType)
			i++
		}
	})
}

// Benchmark logicBindManager session / user bind / query in parallel.
func Benchmark_logicBindManager_bind_get_parallel(b *testing.B) {
	m := newLogicBindManager()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sessionID := "s-" + strconv.Itoa(i)
			nodeType := "type-" + strconv.Itoa(i%4)
			nodeID := "node-" + strconv.Itoa(i%8)
			uid := user.USER_ID(i)

			_ = m.sessionBind(sessionID, nodeType, nodeID)
			_ = m.getLogicItemBySessionId(sessionID, nodeType)

			_ = m.userBind(uid, nodeType, nodeID)
			_ = m.getLogicItemByUserId(uid, nodeType)

			m.sessionUnbind(sessionID, nodeType)
			m.userUnbind(uid, nodeType)
			i++
		}
	})
}

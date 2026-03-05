package kkpacket

import (
	"reflect"
	"testing"
)

// Benchmark 针对 MsgRouter 的典型使用场景：注册 + 按类型/ID/路由查询。

func BenchmarkMsgRouter_Register(b *testing.B) {
	router := NewMsgRouter()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := MSGID(1000 + i)
		if err := router.Register(id, &routerTestMsg{}, "/bench/"+itoa(i)); err != nil {
			b.Fatalf("Register error at i=%d: %v", i, err)
		}
	}
}

func BenchmarkMsgRouter_GetMsgID(b *testing.B) {
	router := NewMsgRouter()
	const id MSGID = 2001
	if err := router.Register(id, &routerTestMsg{}, "/bench/getid"); err != nil {
		b.Fatalf("Register: %v", err)
	}

	msg := &routerTestMsg{ID: 1}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if got := router.GetMsgID(msg); got != id {
			b.Fatalf("GetMsgID = %d, want %d", got, id)
		}
	}
}

func BenchmarkMsgRouter_GetMsgType(b *testing.B) {
	router := NewMsgRouter()
	const id MSGID = 2002
	if err := router.Register(id, &routerTestMsg{}, "/bench/gettype"); err != nil {
		b.Fatalf("Register: %v", err)
	}

	wantType := reflect.TypeOf(&routerTestMsg{})
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if tp := router.GetMsgType(id); tp != wantType {
			b.Fatalf("GetMsgType = %v, want %v", tp, wantType)
		}
	}
}

func BenchmarkMsgRouter_GetMsgRoute(b *testing.B) {
	router := NewMsgRouter()
	const id MSGID = 2003
	const route = "/bench/route"
	if err := router.Register(id, &routerTestMsg{}, route); err != nil {
		b.Fatalf("Register: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		got, err := router.GetMsgRoute(id)
		if err != nil {
			b.Fatalf("GetMsgRoute error: %v", err)
		}
		if got != route {
			b.Fatalf("GetMsgRoute = %q, want %q", got, route)
		}
	}
}

// itoa 是一个简单的整型转字符串实现，避免为基准引入 strconv 的额外噪音。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

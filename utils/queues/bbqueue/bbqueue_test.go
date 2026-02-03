package bbqueue

import (
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func makeBuf(s string) *kkbuffer.ByteBuffer {
	b := kkbuffer.Get()
	b.SetString(s)
	return b
}

func TestBBQueue_New(t *testing.T) {
	q := NewBBQueue(10, true)
	if q.Len() != 0 {
		t.Errorf("New queue Len() = %d, want 0", q.Len())
	}
}

func TestBBQueue_NewZeroOrNegative(t *testing.T) {
	// size <= 0 时使用默认 64
	for _, size := range []int{0, -1} {
		q := NewBBQueue(size, false)
		if q.Len() != 0 {
			t.Errorf("NewBBQueue(%d) queue Len() = %d, want 0", size, q.Len())
		}
		b := makeBuf("x")
		q.Push(b)
		if q.Len() != 1 {
			t.Errorf("After Push, Len() = %d, want 1", q.Len())
		}
		bb := q.Pop()
		if bb == nil || bb.String() != "x" {
			t.Errorf("Pop() = %v, want buffer with 'x'", bb)
		}
		kkbuffer.Put(bb)
	}
}

func TestBBQueue_PushPop(t *testing.T) {
	q := NewBBQueue(4, false)

	b := makeBuf("hello")
	q.Push(b)
	if q.Len() != 1 {
		t.Errorf("Len() = %d, want 1", q.Len())
	}

	bb := q.Pop()
	if bb == nil {
		t.Fatal("Pop() returned nil")
	}
	if bb.String() != "hello" {
		t.Errorf("Pop() = %q, want 'hello'", bb.String())
	}
	kkbuffer.Put(bb)

	if q.Len() != 0 {
		t.Errorf("Len() = %d, want 0", q.Len())
	}
}

func TestBBQueue_PushPopMultiple(t *testing.T) {
	q := NewBBQueue(4, false)

	for i := 0; i < 10; i++ {
		q.Push(makeBuf(fmt.Sprintf("%d", i)))
	}
	if q.Len() != 10 {
		t.Errorf("Len() = %d, want 10", q.Len())
	}

	for i := 0; i < 10; i++ {
		bb := q.Pop()
		if bb == nil {
			t.Fatalf("Pop() at %d returned nil", i)
		}
		want := fmt.Sprintf("%d", i)
		if bb.String() != want {
			t.Errorf("Pop() = %q, want %q", bb.String(), want)
		}
		kkbuffer.Put(bb)
	}
	if q.Len() != 0 {
		t.Errorf("Len() = %d, want 0", q.Len())
	}
}

func TestBBQueue_PopEmpty(t *testing.T) {
	q := NewBBQueue(4, false)
	bb := q.Pop()
	if bb != nil {
		t.Errorf("Pop() on empty queue = %v, want nil", bb)
	}
}

func TestBBQueue_Grow(t *testing.T) {
	q := NewBBQueue(4, false)
	// 超过初始容量以触发 grow
	for i := 0; i < 20; i++ {
		q.Push(makeBuf(fmt.Sprintf("%d", i)))
	}
	if q.Len() != 20 {
		t.Errorf("Len() = %d, want 20", q.Len())
	}
	for i := 0; i < 20; i++ {
		bb := q.Pop()
		if bb == nil {
			t.Fatalf("Pop() at %d returned nil", i)
		}
		if bb.String() != fmt.Sprintf("%d", i) {
			t.Errorf("Pop() = %q, want %q", bb.String(), fmt.Sprintf("%d", i))
		}
		kkbuffer.Put(bb)
	}
	if q.Len() != 0 {
		t.Errorf("Len() = %d, want 0", q.Len())
	}
}

func TestBBQueue_Shrink_AfterPop(t *testing.T) {
	if !enableShrink {
		t.Skip("skipping shrink test")
		return
	}
	q := NewBBQueue(4096, false)
	if q.Cap() != 4096 {
		t.Fatalf("Cap()=%d want=4096", q.Cap())
	}

	// Push 3000 buffers.
	for i := 0; i < 3000; i++ {
		if !q.Push(makeBuf(fmt.Sprintf("%d", i))) {
			t.Fatalf("Push(%d) failed", i)
		}
	}
	// Pop 2000, leaving 1000 (< 4096/2), should shrink to 2048.
	for i := 0; i < 2000; i++ {
		bb := q.Pop()
		if bb == nil || bb.String() != fmt.Sprintf("%d", i) {
			t.Fatalf("Pop(%d)=%v want=%d", i, bb, i)
		}
		kkbuffer.Put(bb)
	}

	if q.Cap() != 2048 {
		t.Fatalf("Cap() after shrink=%d want=2048", q.Cap())
	}
	if q.Len() != 1000 {
		t.Fatalf("Len()=%d want=1000", q.Len())
	}

	// Remaining order should be preserved.
	for i := 2000; i < 3000; i++ {
		bb := q.Pop()
		if bb == nil || bb.String() != fmt.Sprintf("%d", i) {
			t.Fatalf("Pop remaining(%d)=%v want=%d", i, bb, i)
		}
		kkbuffer.Put(bb)
	}
	if q.Len() != 0 {
		t.Fatalf("Len()=%d want=0", q.Len())
	}
	// Empty queue should keep minimal shrink size (2048) once it had grown big.
	if q.Cap() != 2048 {
		t.Fatalf("Cap() empty=%d want=2048", q.Cap())
	}
}

func TestBBQueue_HeadTailReset(t *testing.T) {
	q := NewBBQueue(4, false)
	// 先填满再弹空，触发 head/tail 回零
	for i := 0; i < 6; i++ {
		q.Push(makeBuf(fmt.Sprintf("%d", i)))
	}
	for i := 0; i < 6; i++ {
		bb := q.Pop()
		if bb == nil || bb.String() != fmt.Sprintf("%d", i) {
			t.Errorf("Pop() = %v, want %d", bb, i)
		}
		if bb != nil {
			kkbuffer.Put(bb)
		}
	}
	if q.Len() != 0 {
		t.Errorf("after pop all, Len() = %d, want 0", q.Len())
	}
	// 再次 Push/Pop 应仍正常
	q.Push(makeBuf("again"))
	if q.Len() != 1 {
		t.Errorf("Len() = %d, want 1", q.Len())
	}
	bb := q.Pop()
	if bb == nil || bb.String() != "again" {
		t.Errorf("Pop() = %v, want 'again'", bb)
	}
	if bb != nil {
		kkbuffer.Put(bb)
	}
}

func TestBBQueue_ConcurrentPush(t *testing.T) {
	// BBQueue 非并发安全，本测试仅验证并发 Push 不 panic；最终 Len 可能因竞态小于 1000
	q := NewBBQueue(100, false)
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				q.Push(makeBuf(fmt.Sprintf("%d-%d", id, j)))
			}
			done <- true
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	// 清空并归还 buffer，避免池泄漏
	for q.Len() > 0 {
		bb := q.Pop()
		if bb != nil {
			kkbuffer.Put(bb)
		}
	}
}

func TestBBQueue_LenConsistency(t *testing.T) {
	q := NewBBQueue(4, false)
	for i := 0; i < 20; i++ {
		q.Push(makeBuf(fmt.Sprintf("%d", i)))
		if q.Len() != i+1 {
			t.Errorf("After Push(%d), Len() = %d, want %d", i, q.Len(), i+1)
		}
	}
	for i := 0; i < 20; i++ {
		bb := q.Pop()
		if bb != nil {
			kkbuffer.Put(bb)
		}
		if q.Len() != 19-i {
			t.Errorf("After Pop(), Len() = %d, want %d", q.Len(), 19-i)
		}
	}
}

func TestBBQueue_PopMany_Basic(t *testing.T) {
	q := NewBBQueue(8, false)
	for i := 0; i < 10; i++ {
		q.Push(makeBuf(fmt.Sprintf("%d", i)))
	}

	recv := make([]*kkbuffer.ByteBuffer, 5)
	n := q.PopMany(5, recv, 0)
	if n != 5 {
		t.Fatalf("PopMany() = %d, want 5", n)
	}
	for i := 0; i < n; i++ {
		if recv[i] == nil {
			t.Fatalf("recv[%d] is nil", i)
		}
		if recv[i].String() != fmt.Sprintf("%d", i) {
			t.Fatalf("recv[%d] = %q, want %q", i, recv[i].String(), fmt.Sprintf("%d", i))
		}
		kkbuffer.Put(recv[i])
		recv[i] = nil
	}
	if q.Len() != 5 {
		t.Fatalf("Len() after PopMany = %d, want 5", q.Len())
	}
}

func TestBBQueue_PopMany_Limits(t *testing.T) {
	q := NewBBQueue(8, false)
	for i := 0; i < 3; i++ {
		q.Push(makeBuf(fmt.Sprintf("%d", i)))
	}

	recv := make([]*kkbuffer.ByteBuffer, 2)
	n := q.PopMany(10, recv, 0) // count > len(recv) and > Len()
	if n != 2 {
		t.Fatalf("PopMany(10, len=2) = %d, want 2", n)
	}
	for i := 0; i < n; i++ {
		if recv[i] == nil {
			t.Fatalf("recv[%d] is nil", i)
		}
		if recv[i].String() != fmt.Sprintf("%d", i) {
			t.Fatalf("recv[%d] = %q, want %q", i, recv[i].String(), fmt.Sprintf("%d", i))
		}
		kkbuffer.Put(recv[i])
	}
	if q.Len() != 1 {
		t.Fatalf("Len() after PopMany = %d, want 1", q.Len())
	}
	// 清空剩余
	if bb := q.Pop(); bb != nil {
		kkbuffer.Put(bb)
	}
}

func TestBBQueue_PopMany_WrapAroundOrder(t *testing.T) {
	q := NewBBQueue(8, false)
	// 先填满
	for i := 0; i < 8; i++ {
		q.Push(makeBuf(fmt.Sprintf("%d", i)))
	}
	// 弹出6个，留下 6,7
	for i := 0; i < 6; i++ {
		bb := q.Pop()
		if bb == nil || bb.String() != fmt.Sprintf("%d", i) {
			t.Fatalf("Pop() = %v, want %d", bb, i)
		}
		kkbuffer.Put(bb)
	}
	// 再入6个，触发 tail 回绕，但不扩容
	for i := 8; i < 14; i++ {
		q.Push(makeBuf(fmt.Sprintf("%d", i)))
	}
	if q.Len() != 8 {
		t.Fatalf("Len() = %d, want 8", q.Len())
	}

	recv := make([]*kkbuffer.ByteBuffer, 8)
	n := q.PopMany(8, recv, 0)
	if n != 8 {
		t.Fatalf("PopMany() = %d, want 8", n)
	}
	for i, want := 0, 6; i < n; i, want = i+1, want+1 {
		if recv[i] == nil {
			t.Fatalf("recv[%d] is nil", i)
		}
		if recv[i].String() != fmt.Sprintf("%d", want) {
			t.Fatalf("recv[%d] = %q, want %q", i, recv[i].String(), fmt.Sprintf("%d", want))
		}
		kkbuffer.Put(recv[i])
	}
	if q.Len() != 0 {
		t.Fatalf("Len() after PopMany = %d, want 0", q.Len())
	}

	// 空队列后 head/tail 应复位，继续可用
	q.Push(makeBuf("again"))
	bb := q.Pop()
	if bb == nil || bb.String() != "again" {
		t.Fatalf("after reset, Pop() = %v, want 'again'", bb)
	}
	kkbuffer.Put(bb)
}

func TestBBQueue_PopMany_LimitBytes(t *testing.T) {
	q := NewBBQueue(8, false)

	// push three buffers: 2,2,10 bytes
	b0 := kkbuffer.GetWithCapacity(2)
	b0.B = append(b0.B, []byte("aa")...)
	b1 := kkbuffer.GetWithCapacity(2)
	b1.B = append(b1.B, []byte("bb")...)
	b2 := kkbuffer.GetWithCapacity(10)
	b2.B = append(b2.B, []byte("cccccccccc")...)

	q.Push(b0)
	q.Push(b1)
	q.Push(b2)

	recv := make([]*kkbuffer.ByteBuffer, 10)

	// limitBytes=3 should pop at least 1, and should not exceed limit for subsequent items.
	n := q.PopMany(10, recv, 3)
	if n != 1 {
		t.Fatalf("PopMany(limitBytes=3) = %d, want 1", n)
	}
	if recv[0] == nil || string(recv[0].B) != "aa" {
		t.Fatalf("recv[0] = %v, want 'aa'", recv[0])
	}
	kkbuffer.Put(recv[0])
	recv[0] = nil

	// Now queue has "bb"(2) + "cccc..."(10). limitBytes=3 should pop only "bb".
	n = q.PopMany(10, recv, 3)
	if n != 1 {
		t.Fatalf("PopMany(limitBytes=3) second = %d, want 1", n)
	}
	if recv[0] == nil || string(recv[0].B) != "bb" {
		t.Fatalf("recv[0] = %v, want 'bb'", recv[0])
	}
	kkbuffer.Put(recv[0])
	recv[0] = nil

	// Now only big one remains; limitBytes=3 still should pop 1 (even if it exceeds).
	n = q.PopMany(10, recv, 3)
	if n != 1 {
		t.Fatalf("PopMany(limitBytes=3) big = %d, want 1", n)
	}
	if recv[0] == nil || string(recv[0].B) != "cccccccccc" {
		t.Fatalf("recv[0] = %v, want 'cccccccccc'", recv[0])
	}
	kkbuffer.Put(recv[0])
	recv[0] = nil

	if q.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", q.Len())
	}
}

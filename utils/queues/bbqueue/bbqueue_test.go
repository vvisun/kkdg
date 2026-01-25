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

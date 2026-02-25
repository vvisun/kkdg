package bbqueue

import (
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func makeBufNN(s string) *kkbuffer.ByteBuffer {
	b := kkbuffer.Get()
	b.SetString(s)
	return b
}

func TestNNQueue_New(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](10, true)
	if q.Len() != 0 {
		t.Errorf("New queue Len() = %d, want 0", q.Len())
	}
	if !q.IsEmpty() {
		t.Errorf("New queue IsEmpty() = false, want true")
	}
	if q.IsFull() {
		t.Errorf("New queue IsFull() = true, want false")
	}
}

func TestNNQueue_PushPop(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](4, false)

	b := makeBufNN("hello")
	ok := q.Push(b)
	if !ok {
		t.Fatal("Push() = false, want true")
	}
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
	if !q.IsEmpty() {
		t.Errorf("IsEmpty() = false, want true")
	}
}

func TestNNQueue_PushPopMultiple(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](4, false)

	for i := 0; i < 10; i++ {
		ok := q.Push(makeBufNN(fmt.Sprintf("%d", i)))
		if !ok {
			t.Fatalf("Push(%d) = false", i)
		}
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

func TestNNQueue_PopEmpty(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](4, false)
	bb := q.Pop()
	if bb != nil {
		t.Errorf("Pop() on empty queue = %v, want nil", bb)
	}
}

func TestNNQueue_IsFull_Strict(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](2, true)

	if q.IsFull() {
		t.Error("empty queue IsFull() = true, want false")
	}
	q.Push(makeBufNN("a"))
	if q.IsFull() {
		t.Error("Len=1 IsFull() = true, want false")
	}
	q.Push(makeBufNN("b"))
	if !q.IsFull() {
		t.Error("Len=2 (maxCount=2) IsFull() = false, want true")
	}
	ok := q.Push(makeBufNN("c"))
	if ok {
		t.Error("Push when full (strict) = true, want false")
	}
	if q.Len() != 2 {
		t.Errorf("Len() = %d, want 2", q.Len())
	}
	// 归还已入队的
	for q.Len() > 0 {
		bb := q.Pop()
		if bb != nil {
			kkbuffer.Put(bb)
		}
	}
}

func TestNNQueue_IsFull_NonStrict(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](2, false)
	for i := 0; i < 5; i++ {
		q.Push(makeBufNN(fmt.Sprintf("%d", i)))
	}
	// 非严格模式不应报告 IsFull（按实现：isStrict && count >= maxCount）
	if q.IsFull() {
		t.Error("non-strict queue IsFull() = true, want false")
	}
	if q.Len() != 5 {
		t.Errorf("Len() = %d, want 5", q.Len())
	}
	for i := 0; i < 5; i++ {
		bb := q.Pop()
		if bb == nil || bb.String() != fmt.Sprintf("%d", i) {
			t.Errorf("Pop() = %v, want %d", bb, i)
		}
		if bb != nil {
			kkbuffer.Put(bb)
		}
	}
}

func TestNNQueue_LenConsistency(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](4, false)
	for i := 0; i < 20; i++ {
		q.Push(makeBufNN(fmt.Sprintf("%d", i)))
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

func TestNNQueue_PopMany_Basic(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](16, false)
	for i := 0; i < 10; i++ {
		q.Push(makeBufNN(fmt.Sprintf("%d", i)))
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

func TestNNQueue_PopMany_Limits(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](8, false)
	for i := 0; i < 3; i++ {
		q.Push(makeBufNN(fmt.Sprintf("%d", i)))
	}

	recv := make([]*kkbuffer.ByteBuffer, 2)
	n := q.PopMany(10, recv, 0)
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
	bb := q.Pop()
	if bb == nil || bb.String() != "2" {
		t.Fatalf("remaining Pop() = %v, want '2'", bb)
	}
	kkbuffer.Put(bb)
}

func TestNNQueue_PopMany_EmptyRecvPanic(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](4, false)
	q.Push(makeBufNN("x"))
	recv := make([]*kkbuffer.ByteBuffer, 0)
	defer func() {
		if r := recover(); r == nil {
			t.Error("PopMany with empty recv should panic")
		}
	}()
	q.PopMany(1, recv, 0)
}

func TestNNQueue_PopMany_LimitBytes(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](8, false)

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

	n := q.PopMany(10, recv, 3)
	if n != 1 {
		t.Fatalf("PopMany(limitBytes=3) = %d, want 1", n)
	}
	if recv[0] == nil || string(recv[0].B) != "aa" {
		t.Fatalf("recv[0] = %v, want 'aa'", recv[0])
	}
	kkbuffer.Put(recv[0])
	recv[0] = nil

	n = q.PopMany(10, recv, 3)
	if n != 1 {
		t.Fatalf("PopMany(limitBytes=3) second = %d, want 1", n)
	}
	if recv[0] == nil || string(recv[0].B) != "bb" {
		t.Fatalf("recv[0] = %v, want 'bb'", recv[0])
	}
	kkbuffer.Put(recv[0])
	recv[0] = nil

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

func TestNNQueue_PopMany_CountZero(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](4, false)
	q.Push(makeBufNN("a"))
	recv := make([]*kkbuffer.ByteBuffer, 2)
	n := q.PopMany(0, recv, 0)
	if n != 1 {
		t.Fatalf("PopMany(0, ...) should treat count as 1, got n=%d", n)
	}
	if recv[0] == nil || recv[0].String() != "a" {
		t.Fatalf("recv[0] = %v, want 'a'", recv[0])
	}
	kkbuffer.Put(recv[0])
}

func TestNNQueue_EmptyPopMany(t *testing.T) {
	q := NewNNQueue[*kkbuffer.ByteBuffer](4, false)
	recv := make([]*kkbuffer.ByteBuffer, 2)
	n := q.PopMany(2, recv, 0)
	if n != 0 {
		t.Fatalf("PopMany on empty = %d, want 0", n)
	}
}

package kkbuffer

import (
	"io"
	"sync/atomic"
)

// 使用原则: 在哪里终止使用，就在哪里用kkbuffer.Put()释放。
//  1. 在任何函数中，如果不再使用(不作为参数传递给其他函数，或不作为返回值时)则kkbuffer.Put()释放。
//  2. 如果传递给其他函数使用，不要释放，因为其他函数可能是异步使用，释放会引起数据错乱。
type ByteBuffer struct {
	//是否已释放标志，防止重复释放
	released atomic.Bool
	// B is a byte buffer to use in append-like workloads.
	// See example code for details.
	B []byte
}

// ReadFrom implements io.ReaderFrom.
//
// The function appends all the data read from r to b.
func (b *ByteBuffer) ReadFrom(r io.Reader) (int64, error) {
	p := b.B
	nStart := int64(len(p))
	nMax := int64(cap(p))
	n := nStart
	if nMax == 0 {
		nMax = 64
		p = make([]byte, nMax)
	} else {
		p = p[:nMax]
	}
	for {
		if n == nMax {
			nMax *= 2
			bNew := make([]byte, nMax)
			copy(bNew, p)
			p = bNew
		}
		nn, err := r.Read(p[n:])
		n += int64(nn)
		if err != nil {
			b.B = p[:n]
			n -= nStart
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
	}
}

// WriteTo implements io.WriterTo.
func (b *ByteBuffer) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b.B)
	return int64(n), err
}

//----------------------------------------------------------

func (b *ByteBuffer) Cap() int {
	return cap(b.B)
}

// Len returns the size of the byte buffer.
func (b *ByteBuffer) Len() int {
	return len(b.B)
}

// Bytes returns b.B, i.e. all the bytes accumulated in the buffer.
//
// The purpose of this function is bytes.Buffer compatibility.
func (b *ByteBuffer) Bytes() []byte {
	return b.B
}

// Reset makes ByteBuffer.B empty.
func (b *ByteBuffer) Reset() {
	b.B = b.B[:0]
}

// SetBytes sets ByteBuffer.B to p.
//
// If cap(b.B) >= len(p), it uses copy (no alloc). Use GetWithCapacity
// or Grow before Reset+SetBytes when repeatedly setting similar-sized data.
func (b *ByteBuffer) SetBytes(p []byte) {
	cnt := len(p)
	b.grow(cnt)
	b.B = b.B[:cnt]
	copy(b.B, p)
}

func (b *ByteBuffer) WriteBytes(p []byte) {
	cnt := len(p)
	b.B = b.B[:cnt]
	copy(b.B, p)
}

// SetString sets ByteBuffer.B to s.
//
// If cap(b.B) >= len(s), it uses copy (no alloc). Use GetWithCapacity
// or Grow before Reset+SetString when repeatedly setting similar-sized data.
func (b *ByteBuffer) SetString(s string) {
	cnt := len(s)
	b.grow(cnt)
	b.B = b.B[:cnt]
	copy(b.B, s)
}

// grow ensures the buffer has at least n bytes capacity.
// If the current capacity is less than n, it grows the buffer.
//
// Call grow before a batch of Write/WriteByte/WriteString when the total
// size is known (e.g. grow(len(b.B)+total) before a loop) to reduce reallocations.
func (b *ByteBuffer) grow(n int) {
	if cap(b.B) < n {
		newCap := n
		if cap(b.B) > 0 {
			newCap = cap(b.B) * 2
			if newCap < n {
				newCap = n
			}
		}
		newBuf := make([]byte, len(b.B), newCap)
		copy(newBuf, b.B)
		b.B = newBuf
	}
}

// String returns string representation of ByteBuffer.B.
func (b *ByteBuffer) String() string {
	return string(b.B)
}

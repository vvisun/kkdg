package kkbuffer

import (
	"io"
	"sync/atomic"
)

// ByteBuffer provides byte buffer, which can be used for minimizing
// memory allocations.
//
// ByteBuffer may be used with functions appending data to the given []byte
// slice. See example code for details.
//
// Use Get for obtaining an empty byte buffer.
type ByteBuffer struct {

	// B is a byte buffer to use in append-like workloads.
	// See example code for details.
	B []byte

	released atomic.Bool //防止重复释放
}

// Len returns the size of the byte buffer.
func (b *ByteBuffer) Len() int {
	return len(b.B)
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

// Bytes returns b.B, i.e. all the bytes accumulated in the buffer.
//
// The purpose of this function is bytes.Buffer compatibility.
func (b *ByteBuffer) Bytes() []byte {
	return b.B
}

// Write implements io.Writer - it appends p to ByteBuffer.B
func (b *ByteBuffer) Write(p []byte) (int, error) {
	b.B = append(b.B, p...)
	return len(p), nil
}

// WriteByte appends the byte c to the buffer.
//
// The purpose of this function is bytes.Buffer compatibility.
//
// The function always returns nil.
func (b *ByteBuffer) WriteByte(c byte) error {
	b.B = append(b.B, c)
	return nil
}

// WriteString appends s to ByteBuffer.B.
func (b *ByteBuffer) WriteString(s string) (int, error) {
	b.B = append(b.B, s...)
	return len(s), nil
}

// Set sets ByteBuffer.B to p.
// If the buffer has sufficient capacity, it uses copy for better performance.
func (b *ByteBuffer) Set(p []byte) {
	if cap(b.B) >= len(p) {
		b.B = b.B[:len(p)]
		copy(b.B, p)
	} else {
		b.B = append(b.B[:0], p...)
	}
}

// SetString sets ByteBuffer.B to s.
// If the buffer has sufficient capacity, it uses copy for better performance.
func (b *ByteBuffer) SetString(s string) {
	if cap(b.B) >= len(s) {
		b.B = b.B[:len(s)]
		copy(b.B, s)
	} else {
		b.B = append(b.B[:0], s...)
	}
}

// SetWithCapacity sets ByteBuffer.B to p, ensuring capacity >= len(p).
// This method always ensures sufficient capacity, potentially allocating new memory.
func (b *ByteBuffer) SetWithCapacity(p []byte) {
	if cap(b.B) < len(p) {
		b.B = make([]byte, len(p))
	} else {
		b.B = b.B[:len(p)]
	}
	copy(b.B, p)
}

// Grow ensures the buffer has at least n bytes capacity.
// If the current capacity is less than n, it grows the buffer.
func (b *ByteBuffer) Grow(n int) {
	if cap(b.B) < n {
		newCap := n
		if cap(b.B) > 0 {
			// Double the capacity, but ensure it's at least n
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

// Reset makes ByteBuffer.B empty.
func (b *ByteBuffer) Reset() {
	b.B = b.B[:0]
}

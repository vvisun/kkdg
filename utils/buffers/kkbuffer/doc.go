// Package kkbuffer provides byte buffer and pool for minimizing allocations.
//
// Quick start:
//
//	buf := kkbuffer.Get()
//	buf.WriteString("hello")
//	defer kkbuffer.Put(buf)
//
// Performance tips:
//
//   - Known size: use GetWithCapacity(n), or call Grow(n) before a batch of
//     Write/WriteByte/WriteString to avoid repeated reallocations.
//   - Many WriteByte in a loop: call Grow(b.Len()+n) first, then write n bytes.
//   - Repeated Set/SetString: GetWithCapacity or Grow before Reset+Set keeps
//     cap >= len(data), so Set uses copy instead of allocating.
package kkbuffer

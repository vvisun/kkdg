// Copyright (c) 2021 The Gnet Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package byteslice implements a pool of byte slices consisting of sync.Pool's
// that collect byte slices with different length sizes from 0 to 32 in powers of 2.
package byteslice

import (
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	minBitSize = 0                      // 2**0=1 是1B
	step0      = 1 << minBitSize        // 1 = 1B
	step1      = 1 << (minBitSize + 1)  // 2 = 2B
	step2      = 1 << (minBitSize + 2)  // 4 = 4B
	step3      = 1 << (minBitSize + 3)  // 8 = 8B
	step4      = 1 << (minBitSize + 4)  // 16 = 16B
	step5      = 1 << (minBitSize + 5)  // 32 = 32B
	step6      = 1 << (minBitSize + 6)  // 64 = 64B
	step7      = 1 << (minBitSize + 7)  // 128 = 128B
	step8      = 1 << (minBitSize + 8)  // 256 = 256B
	step9      = 1 << (minBitSize + 9)  // 512 = 512B
	step10     = 1 << (minBitSize + 10) // 1024 = 1KB
	step11     = 1 << (minBitSize + 11) // 2048 = 2KB
	step12     = 1 << (minBitSize + 12) // 4096 = 4KB
	step13     = 1 << (minBitSize + 13) // 8192 = 8KB
	step14     = 1 << (minBitSize + 14) // 16384 = 16KB
	step15     = 1 << (minBitSize + 15) // 32768 = 32KB
	step16     = 1 << (minBitSize + 16) // 65536 = 64KB
	step17     = 1 << (minBitSize + 17) // 131072 = 128KB
	step18     = 1 << (minBitSize + 18) // 262144 = 256KB
	step19     = 1 << (minBitSize + 19) // 524288 = 512KB
	step20     = 1 << (minBitSize + 20) // 1048576 = 1MB
	step21     = 1 << (minBitSize + 21) // 2097152 = 2MB
	step22     = 1 << (minBitSize + 22) // 4194304 = 4MB
	step23     = 1 << (minBitSize + 23) // 8388608 = 8MB
	step24     = 1 << (minBitSize + 24) // 16777216 = 16MB
	step25     = 1 << (minBitSize + 25) // 33554432 = 32MB
	step26     = 1 << (minBitSize + 26) // 67108864 = 64MB
	step27     = 1 << (minBitSize + 27) // 134217728 = 128MB
	step28     = 1 << (minBitSize + 28) // 268435456 = 256MB
	step29     = 1 << (minBitSize + 29) // 536870912 = 512MB
	step30     = 1 << (minBitSize + 30) // 1073741824 = 1GB
	step31     = 1 << (minBitSize + 31) // 2147483648 = 2GB
)

const max_size = step16

// 必须做限制，防止池过大耗尽内存
var max_caps [32]int64 = [32]int64{
	1024 * 1024,      // step0, 1B, 1M里有1024K个byte slice
	1024 * 1024,      // step1, 2B, 1M里有512K个byte slice
	1024 * 1024,      // step2, 4B, 1M里有256K个byte slice
	1024 * 1024,      // step3, 8B, 1M里有128K个byte slice
	1024 * 1024 * 2,  // step4, 16B, 1M里有64K个byte slice
	1024 * 1024 * 4,  // step5, 32B, 1M里有32K个byte slice
	1024 * 1024 * 8,  // step6, 64B, 1M里有16K个byte slice
	1024 * 1024 * 16, // step7, 128B, 1M里有8K个byte slice
	1024 * 1024 * 20, // step8, 256B, 1M里有4096个byte slice
	1024 * 1024 * 20, // step9, 512B, 1M里有2048个byte slice
	1024 * 1024 * 20, // step10, 1KB, 1M里有1024个byte slice
	1024 * 1024 * 20, // step11, 2KB, 1M里有512个byte slice
	1024 * 1024 * 20, // step12, 4KB, 1M里有256个byte slice
	1024 * 1024 * 10, // step13, 8KB, 1M里有128个byte slice
	1024 * 1024 * 8,  // step14, 16KB, 1M里有64个byte slice
	1024 * 1024 * 8,  // step15, 32KB, 1M里有32个byte slice
	1024 * 1024 * 8,  // step16, 64KB, 1M里有16个byte slice
	// 后面基本不会用到
	1024 * 1024, // step17, 128KB, 1M里有8个byte slice
	1024 * 1024, // step18, 256KB, 1M里有4个byte slice
	1024 * 1024, // step19, 512KB, 1M里有2个byte slice
	1024 * 1024, // step20, 1MB, 1M里有1个byte slice
}

// Pool consists of 32 sync.Pool, representing byte slices of length from 0 to 32 in powers of 2.
type Pool struct {
	pools  [32]sync.Pool
	totals [32]int64 // 每个步长对应的容量，单位：字节数
}

var builtinPool Pool

// Get returns a byte slice with given length from the built-in pool.
func Get(size int) []byte {
	return builtinPool.Get(size)
}

// Get returns a byte slice with 0 length and given capacity from the built-in pool.
func GetZero(capacity int) []byte {
	ret := builtinPool.Get(capacity)
	ret = ret[:0]
	return ret
}

func GetWithLenCap(len int, cap int) []byte {
	ret := builtinPool.Get(cap)
	ret = ret[:len]
	return ret
}

// Put returns the byte slice to the built-in pool.
func Put(buf []byte) {
	builtinPool.Put(buf)
}

// Get retrieves a byte slice of the length requested by the caller from pool or allocates a new one.
func (p *Pool) Get(size int) []byte {
	if size <= 0 {
		return make([]byte, 0)
	}
	if size > max_size {
		return make([]byte, size)
	}
	idx := index(uint32(size))
	ptr, _ := p.pools[idx].Get().(*byte)
	if ptr == nil {
		return make([]byte, size, 1<<idx)
	}
	ret := unsafe.Slice(ptr, 1<<idx)[:size]
	if atomic.LoadInt64(&p.totals[idx]) > 0 {
		atomic.AddInt64(&p.totals[idx], -int64(cap(ret)))
	}
	return ret
}

// Put returns the byte slice to the pool.
func (p *Pool) Put(buf []byte) {
	size := cap(buf)
	if size < 1 || size > max_size {
		return // 超大 buffer 丢弃，不参与校准统计
	}

	idx := index(uint32(size))
	if size != 1<<idx { // this byte slice is not from Pool.Get(), put it into the previous interval of idx
		idx--
	}

	if atomic.AddInt64(&p.totals[idx], int64(size)) > max_caps[idx] {
		return //直接丢弃，防止池过大耗尽内存
	}

	// Store the pointer to the underlying array instead of the pointer to the slice itself,
	// which circumvents the escape of buf from the stack to the heap.
	p.pools[idx].Put(unsafe.SliceData(buf))
}

func index(n uint32) uint32 {
	return uint32(bits.Len32(n - 1))
}

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

var builtinPool Pool

// Pool consists of 32 sync.Pool, representing byte slices of length from 0 to 32 in powers of 2.
type Pool struct {
	pools [32]sync.Pool
}

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
	return unsafe.Slice(ptr, 1<<idx)[:size]
}

// Put returns the byte slice to the pool.
func (p *Pool) Put(buf []byte) {
	size := cap(buf)
	if size < 1 || size > max_size {
		return
	}
	idx := index(uint32(size))
	if size != 1<<idx { // this byte slice is not from Pool.Get(), put it into the previous interval of idx
		idx--
	}
	// Store the pointer to the underlying array instead of the pointer to the slice itself,
	// which circumvents the escape of buf from the stack to the heap.
	p.pools[idx].Put(unsafe.SliceData(buf))
}

func index(n uint32) uint32 {
	return uint32(bits.Len32(n - 1))
}

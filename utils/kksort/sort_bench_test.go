package kksort

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// BenchmarkSortBy_IntPointers 对int指针slice排序的性能测试
func BenchmarkSortBy_IntPointers(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 创建随机数据
				data := make([]*int, size)
				for j := range data {
					val := rand.Int()
					data[j] = &val
				}

				SortBy(data, func(p, q *int) bool {
					return *p < *q
				})
			}
		})
	}
}

// BenchmarkSort_Ints 对int slice排序的性能测试
func BenchmarkSort_Ints(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 创建随机数据
				data := make([]int, size)
				for j := range data {
					data[j] = rand.Int()
				}

				Sort(data, func(p, q int) bool {
					return p < q
				})
			}
		})
	}
}

// BenchmarkSort_Structs 对结构体slice排序的性能测试
func BenchmarkSort_Structs(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 创建随机数据
				data := make([]Person, size)
				for j := range data {
					data[j] = Person{
						Name: "Person" + string(rune(j)),
						Age:  rand.Intn(100),
					}
				}

				Sort(data, func(p, q Person) bool {
					return p.Age < q.Age
				})
			}
		})
	}
}

// BenchmarkSortBy_StructPointers 对结构体指针slice排序的性能测试
func BenchmarkSortBy_StructPointers(b *testing.B) {
	sizes := []int{10, 100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 创建随机数据
				data := make([]*Person, size)
				for j := range data {
					data[j] = &Person{
						Name: "Person" + string(rune(j)),
						Age:  rand.Intn(100),
					}
				}

				SortBy(data, func(p, q *Person) bool {
					return p.Age < q.Age
				})
			}
		})
	}
}

// BenchmarkCompareWithStdLib 对比标准库性能
func BenchmarkCompareWithStdLib(b *testing.B) {
	size := 1000

	b.Run("OurSort_Ints", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data := make([]int, size)
			for j := range data {
				data[j] = rand.Int()
			}

			Sort(data, func(p, q int) bool {
				return p < q
			})
		}
	})

	b.Run("StdLib_Ints", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data := make([]int, size)
			for j := range data {
				data[j] = rand.Int()
			}

			sort.Ints(data)
		}
	})

	b.Run("StdLib_Slice", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data := make([]int, size)
			for j := range data {
				data[j] = rand.Int()
			}

			sort.Slice(data, func(i, j int) bool {
				return data[i] < data[j]
			})
		}
	})
}

// BenchmarkSort_AlreadySorted 测试已排序数据的性能
func BenchmarkSort_AlreadySorted(b *testing.B) {
	size := 1000

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 创建已排序的数据
		data := make([]int, size)
		for j := range data {
			data[j] = j
		}

		Sort(data, func(p, q int) bool {
			return p < q
		})
	}
}

// BenchmarkSort_ReverseSorted 测试逆序数据的性能
func BenchmarkSort_ReverseSorted(b *testing.B) {
	size := 1000

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 创建逆序数据
		data := make([]int, size)
		for j := range data {
			data[j] = size - j
		}

		Sort(data, func(p, q int) bool {
			return p < q
		})
	}
}

// BenchmarkSort_SmallArrays 测试小数组的性能
func BenchmarkSort_SmallArrays(b *testing.B) {
	sizes := []int{2, 3, 4, 5, 8, 16}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				data := make([]int, size)
				for j := range data {
					data[j] = rand.Int()
				}

				Sort(data, func(p, q int) bool {
					return p < q
				})
			}
		})
	}
}

// BenchmarkSort_EdgeCases 测试边界情况的性能
func BenchmarkSort_EdgeCases(b *testing.B) {
	b.Run("EmptySlice", func(b *testing.B) {
		data := []int{}
		for i := 0; i < b.N; i++ {
			Sort(data, func(p, q int) bool { return p < q })
		}
	})

	b.Run("SingleElement", func(b *testing.B) {
		data := []int{42}
		for i := 0; i < b.N; i++ {
			Sort(data, func(p, q int) bool { return p < q })
		}
	})

	b.Run("TwoElements", func(b *testing.B) {
		data := []int{2, 1}
		for i := 0; i < b.N; i++ {
			Sort(data, func(p, q int) bool { return p < q })
		}
	})
}

// BenchmarkInsertionSortThreshold 测试不同插入排序阈值的性能
func BenchmarkInsertionSortThreshold(b *testing.B) {
	// 测试不同阈值的最佳性能
	thresholds := []int{8, 12, 16, 20, 24, 32}

	for _, threshold := range thresholds {
		b.Run(fmt.Sprintf("Threshold_%d", threshold), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// 使用接近阈值大小的数据来测试
				size := threshold + 4
				data := make([]int, size)
				for j := range data {
					data[j] = size - j // 逆序数据，最坏情况
				}

				// 手动实现阈值测试
				if size <= threshold {
					// 使用插入排序
					for i := 1; i < size; i++ {
						for j := i; j > 0 && data[j] < data[j-1]; j-- {
							data[j], data[j-1] = data[j-1], data[j]
						}
					}
				} else {
					// 使用我们的排序函数
					Sort(data, func(p, q int) bool { return p < q })
				}
			}
		})
	}
}

// BenchmarkSortBy_ComplexComparison 复杂比较函数的性能
func BenchmarkSortBy_ComplexComparison(b *testing.B) {
	size := 1000

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := make([]*Person, size)
		for j := range data {
			data[j] = &Person{
				Name: "Person" + string(rune(j%26+'A')),
				Age:  rand.Intn(100),
			}
		}

		// 复杂的比较：先按年龄，再按姓名
		SortBy(data, func(p, q *Person) bool {
			if p.Age != q.Age {
				return p.Age < q.Age
			}
			return p.Name < q.Name
		})
	}
}

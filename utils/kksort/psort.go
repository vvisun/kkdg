package kksort

import "sort"

type vsortWrapper[T any] struct {
	people []*T
	by     func(p, q *T) bool
}

func (pw vsortWrapper[T]) Len() int {
	return len(pw.people)
}
func (pw vsortWrapper[T]) Swap(i, j int) {
	pw.people[i], pw.people[j] = pw.people[j], pw.people[i]
}
func (pw vsortWrapper[T]) Less(i, j int) bool {
	return pw.by(pw.people[i], pw.people[j])
}

//go:inline
func newVWrapper[T any](arr []*T, call func(p, q *T) bool) *vsortWrapper[T] {
	return &vsortWrapper[T]{
		people: arr,
		by:     call,
	}
}

//go:inline
func SortBy[T any](arr []*T, call func(p, q *T) bool) {
	n := len(arr)
	if n <= 1 {
		return
	}

	// 自适应排序算法选择
	if n <= 32 {
		// 小数组：使用优化的插入排序
		sortByWithInsertion(arr, call)
	} else if n <= 256 {
		// 中等数组：快速检查是否接近排序
		isNearlySorted := true
		for i := 1; i < min(16, n); i++ { // 只检查前16个元素
			if call(arr[i], arr[i-1]) {
				isNearlySorted = false
				break
			}
		}
		if isNearlySorted {
			sortByWithInsertion(arr, call)
		} else {
			sort.Sort(newVWrapper(arr, func(p, q *T) bool {
				return call(p, q)
			}))
		}
	} else {
		// 大数组：使用标准排序
		sort.Sort(newVWrapper(arr, func(p, q *T) bool {
			return call(p, q)
		}))
	}
}

// sortByWithInsertion 优化的指针插入排序实现
func sortByWithInsertion[T any](arr []*T, call func(p, q *T) bool) {
	n := len(arr)

	// 快速检查是否已经排序
	isSorted := true
	for i := 1; i < n; i++ {
		if call(arr[i], arr[i-1]) {
			isSorted = false
			break
		}
	}
	if isSorted {
		return // 已经排序，直接返回
	}

	// 使用优化的插入排序：从右向左查找插入位置
	for i := 1; i < n; i++ {
		key := arr[i]
		j := i - 1

		// 从右向左查找插入位置
		for j >= 0 && call(key, arr[j]) {
			arr[j+1] = arr[j]
			j--
		}
		arr[j+1] = key
	}
}

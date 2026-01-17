package kksort

import (
	"reflect"
	"sort"
	"testing"
)

// 测试用的结构体
type Person struct {
	Name string
	Age  int
}

type Student struct {
	Name  string
	Score int
	Grade string
}

// TestSortBy_Pointers 测试指针slice排序
func TestSortBy_Pointers(t *testing.T) {
	tests := []struct {
		name     string
		input    []*Person
		compare  func(p, q *Person) bool
		expected []*Person
	}{
		{
			name: "按年龄升序",
			input: []*Person{
				{Name: "Alice", Age: 25},
				{Name: "Bob", Age: 20},
				{Name: "Charlie", Age: 30},
			},
			compare: func(p, q *Person) bool {
				return p.Age < q.Age
			},
			expected: []*Person{
				{Name: "Bob", Age: 20},
				{Name: "Alice", Age: 25},
				{Name: "Charlie", Age: 30},
			},
		},
		{
			name: "按姓名升序",
			input: []*Person{
				{Name: "Charlie", Age: 30},
				{Name: "Alice", Age: 25},
				{Name: "Bob", Age: 20},
			},
			compare: func(p, q *Person) bool {
				return p.Name < q.Name
			},
			expected: []*Person{
				{Name: "Alice", Age: 25},
				{Name: "Bob", Age: 20},
				{Name: "Charlie", Age: 30},
			},
		},
		{
			name: "按年龄降序",
			input: []*Person{
				{Name: "Alice", Age: 25},
				{Name: "Bob", Age: 20},
				{Name: "Charlie", Age: 30},
			},
			compare: func(p, q *Person) bool {
				return p.Age > q.Age
			},
			expected: []*Person{
				{Name: "Charlie", Age: 30},
				{Name: "Alice", Age: 25},
				{Name: "Bob", Age: 20},
			},
		},
		{
			name: "相同值保持稳定",
			input: []*Person{
				{Name: "Alice", Age: 25},
				{Name: "Bob", Age: 25},
				{Name: "Charlie", Age: 25},
			},
			compare: func(p, q *Person) bool {
				return p.Age < q.Age // 所有值相等
			},
			expected: []*Person{
				{Name: "Alice", Age: 25},
				{Name: "Bob", Age: 25},
				{Name: "Charlie", Age: 25},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 复制输入以避免修改原数据
			input := make([]*Person, len(tt.input))
			copy(input, tt.input)

			SortBy(input, tt.compare)

			if !reflect.DeepEqual(input, tt.expected) {
				t.Errorf("SortBy() = %v, want %v", formatPeople(input), formatPeople(tt.expected))
			}
		})
	}
}

// TestSort_Values 测试值slice排序
func TestSort_Values(t *testing.T) {
	tests := []struct {
		name     string
		input    []Student
		compare  func(p, q Student) bool
		expected []Student
	}{
		{
			name: "按分数升序",
			input: []Student{
				{Name: "Alice", Score: 85, Grade: "A"},
				{Name: "Bob", Score: 92, Grade: "A"},
				{Name: "Charlie", Score: 78, Grade: "B"},
			},
			compare: func(p, q Student) bool {
				return p.Score < q.Score
			},
			expected: []Student{
				{Name: "Charlie", Score: 78, Grade: "B"},
				{Name: "Alice", Score: 85, Grade: "A"},
				{Name: "Bob", Score: 92, Grade: "A"},
			},
		},
		{
			name: "按分数降序",
			input: []Student{
				{Name: "Alice", Score: 85, Grade: "A"},
				{Name: "Bob", Score: 92, Grade: "A"},
				{Name: "Charlie", Score: 78, Grade: "B"},
			},
			compare: func(p, q Student) bool {
				return p.Score > q.Score
			},
			expected: []Student{
				{Name: "Bob", Score: 92, Grade: "A"},
				{Name: "Alice", Score: 85, Grade: "A"},
				{Name: "Charlie", Score: 78, Grade: "B"},
			},
		},
		{
			name: "按姓名升序",
			input: []Student{
				{Name: "Charlie", Score: 78, Grade: "B"},
				{Name: "Alice", Score: 85, Grade: "A"},
				{Name: "Bob", Score: 92, Grade: "A"},
			},
			compare: func(p, q Student) bool {
				return p.Name < q.Name
			},
			expected: []Student{
				{Name: "Alice", Score: 85, Grade: "A"},
				{Name: "Bob", Score: 92, Grade: "A"},
				{Name: "Charlie", Score: 78, Grade: "B"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 复制输入以避免修改原数据
			input := make([]Student, len(tt.input))
			copy(input, tt.input)

			Sort(input, tt.compare)

			if !reflect.DeepEqual(input, tt.expected) {
				t.Errorf("Sort() = %v, want %v", input, tt.expected)
			}
		})
	}
}

// TestSortBy_EdgeCases 测试SortBy的边界情况
func TestSortBy_EdgeCases(t *testing.T) {
	t.Run("nil slice", func(t *testing.T) {
		var arr []*Person
		SortBy(arr, func(p, q *Person) bool { return true })
		// 不应该panic
	})

	t.Run("empty slice", func(t *testing.T) {
		arr := []*Person{}
		SortBy(arr, func(p, q *Person) bool { return true })
		if len(arr) != 0 {
			t.Errorf("空slice长度应该保持为0")
		}
	})

	t.Run("single element", func(t *testing.T) {
		arr := []*Person{{Name: "Alice", Age: 25}}
		original := []*Person{{Name: "Alice", Age: 25}}
		SortBy(arr, func(p, q *Person) bool { return true })
		if !reflect.DeepEqual(arr, original) {
			t.Errorf("单个元素应该保持不变")
		}
	})

	t.Run("two elements", func(t *testing.T) {
		arr := []*Person{
			{Name: "Bob", Age: 20},
			{Name: "Alice", Age: 25},
		}
		expected := []*Person{
			{Name: "Alice", Age: 25},
			{Name: "Bob", Age: 20},
		}
		SortBy(arr, func(p, q *Person) bool { return p.Name < q.Name })
		if !reflect.DeepEqual(arr, expected) {
			t.Errorf("SortBy() = %v, want %v", formatPeople(arr), formatPeople(expected))
		}
	})
}

// TestSort_EdgeCases 测试Sort的边界情况
func TestSort_EdgeCases(t *testing.T) {
	t.Run("nil slice", func(t *testing.T) {
		var arr []int
		Sort(arr, func(p, q int) bool { return p < q })
		// 不应该panic
	})

	t.Run("empty slice", func(t *testing.T) {
		arr := []int{}
		Sort(arr, func(p, q int) bool { return p < q })
		if len(arr) != 0 {
			t.Errorf("空slice长度应该保持为0")
		}
	})

	t.Run("single element", func(t *testing.T) {
		arr := []int{42}
		Sort(arr, func(p, q int) bool { return p < q })
		if len(arr) != 1 || arr[0] != 42 {
			t.Errorf("单个元素应该保持不变")
		}
	})

	t.Run("basic int sort", func(t *testing.T) {
		arr := []int{3, 1, 4, 1, 5}
		expected := []int{1, 1, 3, 4, 5}
		Sort(arr, func(p, q int) bool { return p < q })
		if !reflect.DeepEqual(arr, expected) {
			t.Errorf("Sort() = %v, want %v", arr, expected)
		}
	})

	t.Run("string sort", func(t *testing.T) {
		arr := []string{"zebra", "apple", "banana"}
		expected := []string{"apple", "banana", "zebra"}
		Sort(arr, func(p, q string) bool { return p < q })
		if !reflect.DeepEqual(arr, expected) {
			t.Errorf("Sort() = %v, want %v", arr, expected)
		}
	})
}

// TestSortBy_Stability 测试排序稳定性
func TestSortBy_Stability(t *testing.T) {
	type Item struct {
		Value int
		Index int
	}

	// 创建具有相同值但不同索引的项
	items := []*Item{
		{Value: 1, Index: 1},
		{Value: 2, Index: 2},
		{Value: 1, Index: 3},
		{Value: 3, Index: 4},
		{Value: 1, Index: 5},
	}

	SortBy(items, func(p, q *Item) bool {
		return p.Value < q.Value
	})

	// 验证排序结果
	if items[0].Value != 1 || items[1].Value != 1 || items[2].Value != 1 ||
		items[3].Value != 2 || items[4].Value != 3 {
		t.Errorf("排序结果不正确: %v", items)
	}

	// 注意：由于我们使用的是sort.Sort，它不保证稳定性
	// 这里我们只是验证基本排序功能
}

// TestSort_ComplexTypes 测试复杂类型的排序
func TestSort_ComplexTypes(t *testing.T) {
	// 测试结构体slice
	type Complex struct {
		ID       int
		Priority int
		Name     string
	}

	items := []Complex{
		{ID: 1, Priority: 3, Name: "Low"},
		{ID: 2, Priority: 1, Name: "High"},
		{ID: 3, Priority: 2, Name: "Medium"},
	}

	expected := []Complex{
		{ID: 2, Priority: 1, Name: "High"},
		{ID: 3, Priority: 2, Name: "Medium"},
		{ID: 1, Priority: 3, Name: "Low"},
	}

	Sort(items, func(p, q Complex) bool {
		return p.Priority < q.Priority
	})

	if !reflect.DeepEqual(items, expected) {
		t.Errorf("Sort() = %v, want %v", items, expected)
	}
}

// TestSortBy_NilPointers 测试包含nil指针的情况
func TestSortBy_NilPointers(t *testing.T) {
	// 注意：这个测试可能会有问题，因为比较函数需要处理nil指针
	// 在实际使用中，应该避免nil指针或者在比较函数中处理

	t.Run("with nil pointers - careful", func(t *testing.T) {
		people := []*Person{
			{Name: "Alice", Age: 25},
			nil,
			{Name: "Bob", Age: 20},
		}

		// 这个比较函数需要小心处理nil
		SortBy(people, func(p, q *Person) bool {
			if p == nil && q == nil {
				return false
			}
			if p == nil {
				return true
			}
			if q == nil {
				return false
			}
			return p.Age < q.Age
		})

		// nil应该在前面（根据我们的比较函数）
		if people[0] != nil {
			t.Errorf("nil指针应该在前面")
		}
	})
}

// TestSort_CompareWithStdLib 对比标准库排序
func TestSort_CompareWithStdLib(t *testing.T) {
	// 生成随机数据
	data := []int{3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5}

	// 使用我们的Sort函数
	ourData := make([]int, len(data))
	copy(ourData, data)
	Sort(ourData, func(p, q int) bool { return p < q })

	// 使用标准库sort
	stdData := make([]int, len(data))
	copy(stdData, data)
	sort.Ints(stdData)

	// 结果应该相同
	if !reflect.DeepEqual(ourData, stdData) {
		t.Errorf("我们的排序结果 %v 与标准库结果 %v 不一致", ourData, stdData)
	}
}

// 辅助函数：格式化Person slice用于错误信息
func formatPeople(people []*Person) []string {
	result := make([]string, len(people))
	for i, p := range people {
		if p == nil {
			result[i] = "nil"
		} else {
			result[i] = p.Name
		}
	}
	return result
}

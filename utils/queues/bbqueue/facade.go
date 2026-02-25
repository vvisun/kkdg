package bbqueue

// IFiFoQueue 泛型 FIFO 队列接口
type IFiFoQueue[T Sizable] interface {
	Len() int
	IsFull() bool
	IsEmpty() bool
	Push(data T) bool
	Pop() T
	PopMany(count int, recv []T, limitBytes int) int
}

// NewFIFOQueue 根据严格模式选择不同的队列实现。
// 严格模式下，使用 BBQueue，性能更高。
// 非严格模式下，使用 NNQueue，内存占用更低。
func NewFIFOQueue[T Sizable](size int, isStrict bool) IFiFoQueue[T] {
	if isStrict {
		return NewBBQueue[T](size, isStrict)
	}
	return NewNNQueue[T](size, isStrict)
}

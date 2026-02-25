package bbqueue

// Sizable 用于 PopMany(limitBytes>0) 时计算元素字节大小
type Sizable interface {
	Len() int
}

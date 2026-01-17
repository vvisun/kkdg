package buffers

type IBuffer interface {
	Write(p []byte) (int, error)
}

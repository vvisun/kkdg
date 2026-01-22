package ccgate

// Option configures the gate component.
type Option struct {
	TCPAddr         string
	WSAddr          string
	BusinessHandler IBusinessHandler // 业务处理器，可选
}

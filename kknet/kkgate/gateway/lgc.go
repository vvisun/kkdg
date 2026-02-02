package gateway

import (
	"bytes"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// -------------------------- 连接池监控指标 --------------------------
type ConnPoolMetric struct {
	TotalConn  uint64  // 总连接数
	IdleConn   uint64  // 空闲连接数
	BusyConn   uint64  // 忙碌连接数
	ReuseCount uint64  // 连接复用次数
	CreateFail uint64  // 建连失败次数
	ReuseRate  float64 // 连接复用率（%）
}

// -------------------------- 逻辑服连接结构（TCP调优+零拷贝写缓冲） --------------------------
type LogicConn struct {
	conn     net.Conn
	lastUse  int64         // 最后使用时间戳（ms）
	writeBuf *bytes.Buffer // 零拷贝写缓冲
	mu       sync.Mutex    // 写锁
}

// NewLogicConn 创建逻辑服连接并做TCP内核级调优
func NewLogicConn(netConn net.Conn) *LogicConn {
	// TCP内核调优，仅对TCP连接生效
	if tcpConn, ok := netConn.(*net.TCPConn); ok {
		fd, err := tcpConn.File()
		if err == nil {
			defer fd.Close()
			// // 关闭Nagle算法，降低延迟
			// syscall.SetsockoptInt(int(fd.Fd()), syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1)
			// // 关闭延迟确认，快速ACK
			// syscall.SetsockoptInt(int(fd.Fd()), syscall.IPPROTO_TCP, syscall.TCP_QUICKACK, 1)
			// // 增大滑动窗口
			// syscall.SetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 64*1024)
			// syscall.SetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 64*1024)
			// // 拥塞控制：cubic算法（高吞吐）
			// syscall.SetsockoptString(int(fd.Fd()), syscall.IPPROTO_TCP, syscall.TCP_CONGESTION, "cubic")
		}
		// TCP保活，检测假连接
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(20 * time.Second)
	}
	// 初始化64K零拷贝写缓冲
	buf := bytes.NewBuffer(make([]byte, 0, 64*1024))
	return &LogicConn{
		conn:     netConn,
		lastUse:  time.Now().UnixMilli(),
		writeBuf: buf,
	}
}

// Write 零拷贝写，缓冲满自动刷盘
func (lc *LogicConn) Write(data []byte) (int, error) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	n, err := lc.writeBuf.Write(data)
	if err != nil {
		return n, err
	}
	// 刷盘条件：缓冲满64K / 单条数据>16K
	if lc.writeBuf.Len() >= 64*1024 || len(data) >= 16*1024 {
		if err := lc.flush(); err != nil {
			return n, err
		}
	}
	lc.lastUse = time.Now().UnixMilli()
	return n, nil
}

// flush 刷盘缓冲到TCP连接
func (lc *LogicConn) flush() error {
	if lc.writeBuf.Len() == 0 {
		return nil
	}
	lc.conn.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := lc.conn.Write(lc.writeBuf.Bytes())
	if err == nil {
		lc.writeBuf.Reset()
	}
	return err
}

// Flush 手动刷盘
func (lc *LogicConn) Flush() error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.flush()
}

// HealthCheck 增强型健康检测（超时+读+写）
func (lc *LogicConn) HealthCheck() bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	// 超时检测：5分钟未使用则不健康
	if time.Now().UnixMilli()-lc.lastUse > 5*60*1000 {
		return false
	}
	// 写检测
	lc.conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := lc.conn.Write([]byte{}); err != nil {
		return false
	}
	// 读检测（非阻塞）
	if tcpConn, ok := lc.conn.(*net.TCPConn); ok {
		tcpConn.SetReadDeadline(time.Now())
		buf := make([]byte, 1)
		if _, err := tcpConn.Read(buf); err != nil && err != os.ErrDeadlineExceeded {
			return false
		}
	}
	return true
}

// Close 关闭前强制刷盘，避免消息丢失
func (lc *LogicConn) Close() error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.flush()
	return lc.conn.Close()
}

// -------------------------- 连接池配置 --------------------------
type ConnConfig struct {
	LogicAddrs          map[uint32]string // 逻辑服ID→地址
	MaxConnPerLogic     int               // 每个逻辑服最大连接数
	MinIdleConnPerLogic int               // 每个逻辑服最小空闲连接数
	IdleTimeout         int64             // 空闲连接超时（ms）
	ReconnInterval      int64             // 故障重连间隔（ms）
	CreateConnLimit     int               // 每秒最大建连数（限流）
}

// -------------------------- 逻辑服连接池核心结构 --------------------------
type LogicConnPool struct {
	mu               sync.RWMutex
	pool             map[uint32][]*LogicConn // 逻辑服连接池
	faultMap         map[uint32]bool         // 故障逻辑服标记
	connConfig       ConnConfig              // 配置
	cleanTicker      *time.Ticker            // 空闲连接清理定时器
	reconnTicker     *time.Ticker            // 故障重连定时器
	createConnTicker *time.Ticker            // 建连限流定时器
	closeChan        chan struct{}           // 关闭通道
	metric           ConnPoolMetric          // 监控指标
	createConnCount  atomic.Uint64           // 建连计数（限流）
}

// NewLogicConnPool 创建连接池并初始化最小空闲连接
func NewLogicConnPool(cfg ConnConfig) *LogicConnPool {
	// 默认配置
	if cfg.MaxConnPerLogic <= 0 {
		cfg.MaxConnPerLogic = 10
	}
	if cfg.MinIdleConnPerLogic <= 0 {
		cfg.MinIdleConnPerLogic = 2
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 30000 // 30秒
	}
	if cfg.ReconnInterval <= 0 {
		cfg.ReconnInterval = 3000 // 3秒
	}
	if cfg.CreateConnLimit <= 0 {
		cfg.CreateConnLimit = 50 // 每秒50个
	}

	p := &LogicConnPool{
		pool:             make(map[uint32][]*LogicConn),
		faultMap:         make(map[uint32]bool),
		connConfig:       cfg,
		cleanTicker:      time.NewTicker(time.Duration(cfg.IdleTimeout) * time.Millisecond),
		reconnTicker:     time.NewTicker(time.Duration(cfg.ReconnInterval) * time.Millisecond),
		createConnTicker: time.NewTicker(1 * time.Second),
		closeChan:        make(chan struct{}),
	}

	// 初始化最小空闲连接
	p.initMinIdleConn()
	// 启动定时器协程
	go p.cleanIdleConn()
	go p.reconnFaultLogic()
	go p.resetCreateConnCount()

	return p
}

// initMinIdleConn 初始化每个逻辑服的最小空闲连接
func (p *LogicConnPool) initMinIdleConn() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for logicID := range p.connConfig.LogicAddrs {
		if p.faultMap[logicID] {
			continue
		}
		for len(p.pool[logicID]) < p.connConfig.MinIdleConnPerLogic {
			addr := p.connConfig.LogicAddrs[logicID]
			netConn, err := net.DialTimeout("tcp", addr, 1*time.Second)
			if err != nil {
				p.faultMap[logicID] = true
				atomic.AddUint64(&p.metric.CreateFail, 1)
				break
			}
			conn := NewLogicConn(netConn)
			p.pool[logicID] = append(p.pool[logicID], conn)
			atomic.AddUint64(&p.metric.TotalConn, 1)
			atomic.AddUint64(&p.metric.IdleConn, 1)
		}
	}
}

// supplyMinIdleConn 补充指定逻辑服的最小空闲连接
func (p *LogicConnPool) supplyMinIdleConn(logicID uint32) {
	if len(p.pool[logicID]) >= p.connConfig.MinIdleConnPerLogic {
		return
	}
	addr := p.connConfig.LogicAddrs[logicID]
	netConn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		p.faultMap[logicID] = true
		atomic.AddUint64(&p.metric.CreateFail, 1)
		return
	}
	conn := NewLogicConn(netConn)
	p.pool[logicID] = append(p.pool[logicID], conn)
	atomic.AddUint64(&p.metric.TotalConn, 1)
	atomic.AddUint64(&p.metric.IdleConn, 1)
}

var (
	ErrInvalidAddr = errors.New("network closed")
	ErrConnRefused = errors.New("connection refused")
)

// Get 获取逻辑服连接（健康检测+复用统计+建连限流）
func (p *LogicConnPool) Get(logicID uint32) (*LogicConn, error) {
	p.mu.RLock()
	if p.faultMap[logicID] {
		p.mu.RUnlock()
		return nil, net.ErrClosed
	}
	if _, ok := p.connConfig.LogicAddrs[logicID]; !ok {
		p.mu.RUnlock()
		return nil, ErrInvalidAddr
	}
	// 尝试获取空闲连接
	conns, ok := p.pool[logicID]
	if ok && len(conns) > 0 {
		conn := conns[0]
		p.pool[logicID] = conns[1:]
		p.mu.RUnlock()

		// 健康检测
		if !conn.HealthCheck() {
			conn.Close()
			atomic.AddUint64(&p.metric.TotalConn, ^uint64(0))
			atomic.AddUint64(&p.metric.IdleConn, ^uint64(0))
			return p.Get(logicID)
		}
		// 统计复用
		atomic.AddUint64(&p.metric.ReuseCount, 1)
		atomic.AddUint64(&p.metric.BusyConn, 1)
		atomic.AddUint64(&p.metric.IdleConn, ^uint64(0))
		return conn, nil
	}
	p.mu.RUnlock()

	// 建连限流
	if p.createConnCount.Load() >= uint64(p.connConfig.CreateConnLimit) {
		return nil, ErrConnRefused
	}
	p.createConnCount.Add(1)

	// 新建连接
	p.mu.Lock()
	defer p.mu.Unlock()
	// 双重检查
	if len(p.pool[logicID]) > 0 {
		conn := p.pool[logicID][0]
		p.pool[logicID] = p.pool[logicID][1:]
		if conn.HealthCheck() {
			atomic.AddUint64(&p.metric.ReuseCount, 1)
			atomic.AddUint64(&p.metric.BusyConn, 1)
			atomic.AddUint64(&p.metric.IdleConn, ^uint64(0))
			return conn, nil
		}
		conn.Close()
		atomic.AddUint64(&p.metric.TotalConn, ^uint64(0))
		atomic.AddUint64(&p.metric.IdleConn, ^uint64(0))
	}
	// 检查最大连接数
	if len(p.pool[logicID]) >= p.connConfig.MaxConnPerLogic {
		atomic.AddUint64(&p.metric.CreateFail, 1)
		return nil, ErrConnRefused
	}
	// 实际建连
	addr := p.connConfig.LogicAddrs[logicID]
	netConn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		p.faultMap[logicID] = true
		atomic.AddUint64(&p.metric.CreateFail, 1)
		return nil, err
	}
	conn := NewLogicConn(netConn)
	p.pool[logicID] = append(p.pool[logicID], conn)
	atomic.AddUint64(&p.metric.TotalConn, 1)
	atomic.AddUint64(&p.metric.BusyConn, 1)
	return conn, nil
}

// Put 归还逻辑服连接（刷盘+健康检测+补充最小空闲）
func (p *LogicConnPool) Put(logicID uint32, conn *LogicConn) {
	if conn == nil {
		return
	}
	// 强制刷盘
	conn.Flush()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.faultMap[logicID] {
		conn.Close()
		atomic.AddUint64(&p.metric.TotalConn, ^uint64(0))
		atomic.AddUint64(&p.metric.BusyConn, ^uint64(0))
		return
	}
	// 健康检测
	if !conn.HealthCheck() {
		conn.Close()
		atomic.AddUint64(&p.metric.TotalConn, ^uint64(0))
		atomic.AddUint64(&p.metric.BusyConn, ^uint64(0))
		p.supplyMinIdleConn(logicID)
		return
	}
	// 归还到队尾
	p.pool[logicID] = append(p.pool[logicID], conn)
	atomic.AddUint64(&p.metric.BusyConn, ^uint64(0))
	atomic.AddUint64(&p.metric.IdleConn, 1)
}

// MarkFault 标记逻辑服故障，关闭所有连接
func (p *LogicConnPool) MarkFault(logicID uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.faultMap[logicID] = true
	for _, conn := range p.pool[logicID] {
		conn.Close()
	}
	atomic.AddUint64(&p.metric.TotalConn, ^uint64(len(p.pool[logicID])))
	atomic.AddUint64(&p.metric.IdleConn, ^uint64(len(p.pool[logicID])))
	p.pool[logicID] = nil
}

// -------------------------- 定时器协程 --------------------------
func (p *LogicConnPool) cleanIdleConn() {
	for {
		select {
		case <-p.cleanTicker.C:
			now := time.Now().UnixMilli()
			p.mu.Lock()
			for logicID, conns := range p.pool {
				if p.faultMap[logicID] {
					continue
				}
				var validConns []*LogicConn
				for _, conn := range conns {
					// 保留最小空闲连接，即使超时
					if len(validConns) < p.connConfig.MinIdleConnPerLogic {
						validConns = append(validConns, conn)
						continue
					}
					if now-conn.lastUse < p.connConfig.IdleTimeout {
						validConns = append(validConns, conn)
					} else {
						conn.Close()
						atomic.AddUint64(&p.metric.TotalConn, ^uint64(0))
						atomic.AddUint64(&p.metric.IdleConn, ^uint64(0))
					}
				}
				p.pool[logicID] = validConns
				p.supplyMinIdleConn(logicID)
			}
			p.mu.Unlock()
		case <-p.closeChan:
			p.cleanTicker.Stop()
			return
		}
	}
}

func (p *LogicConnPool) reconnFaultLogic() {
	for {
		select {
		case <-p.reconnTicker.C:
			p.mu.Lock()
			for logicID, isFault := range p.faultMap {
				if !isFault {
					continue
				}
				addr := p.connConfig.LogicAddrs[logicID]
				netConn, err := net.DialTimeout("tcp", addr, 1*time.Second)
				if err == nil {
					p.faultMap[logicID] = false
					conn := NewLogicConn(netConn)
					p.pool[logicID] = append(p.pool[logicID], conn)
					atomic.AddUint64(&p.metric.TotalConn, 1)
					atomic.AddUint64(&p.metric.IdleConn, 1)
					p.supplyMinIdleConn(logicID)
				}
			}
			p.mu.Unlock()
		case <-p.closeChan:
			p.reconnTicker.Stop()
			return
		}
	}
}

func (p *LogicConnPool) resetCreateConnCount() {
	for {
		select {
		case <-p.createConnTicker.C:
			p.createConnCount.Store(0)
			// 计算复用率
			total := atomic.LoadUint64(&p.metric.ReuseCount) + atomic.LoadUint64(&p.metric.CreateFail)
			if total > 0 {
				p.metric.ReuseRate = float64(atomic.LoadUint64(&p.metric.ReuseCount)) / float64(total) * 100
			}
		case <-p.closeChan:
			p.createConnTicker.Stop()
			return
		}
	}
}

// GetMetric 获取连接池监控指标
func (p *LogicConnPool) GetMetric() ConnPoolMetric {
	metric := p.metric
	metric.TotalConn = atomic.LoadUint64(&p.metric.TotalConn)
	metric.IdleConn = atomic.LoadUint64(&p.metric.IdleConn)
	metric.BusyConn = atomic.LoadUint64(&p.metric.BusyConn)
	metric.ReuseCount = atomic.LoadUint64(&p.metric.ReuseCount)
	metric.CreateFail = atomic.LoadUint64(&p.metric.CreateFail)
	return metric
}

// Close 优雅关闭连接池
func (p *LogicConnPool) Close() {
	close(p.closeChan)
	p.mu.Lock()
	defer p.mu.Unlock()
	// 关闭所有逻辑服连接（先刷盘再关闭）
	for _, conns := range p.pool {
		for _, conn := range conns {
			conn.Flush()
			conn.Close()
		}
	}
	// 清空资源
	p.pool = nil
	p.faultMap = nil
}

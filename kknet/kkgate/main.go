package main

import (
	"encoding/binary"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/vvisun/kkdg/kknet/kkgate/gateway"
	"github.com/vvisun/kkdg/kknet/kkgate/gateway/pbgate"
)

// 客户端连接管理：ConnID生成+连接映射
var (
	globalConnID uint64 = 0
	connMap             = make(map[uint64]net.Conn)
	connMapMu    sync.Mutex
)

// 生成全局唯一ConnID
func genConnID() uint64 {
	connMapMu.Lock()
	defer connMapMu.Unlock()
	globalConnID++
	return globalConnID
}

// 网关配置
type GatewayConfig struct {
	ListenAddr         string
	LogicAddrs         map[uint32]string
	PerShardBackMaxLen uint64
	BatchSize          int
	ConnPoolConfig     gateway.ConnConfig
}

func main() {
	// 命令行参数
	listenAddr := flag.String("listen", ":8888", "网关监听地址")
	logicAddr1 := flag.String("logic1", "127.0.0.1:9001", "逻辑服1地址")
	logicAddr2 := flag.String("logic2", "127.0.0.1:9002", "逻辑服2地址")
	flag.Parse()

	// 初始化配置
	cfg := GatewayConfig{
		ListenAddr: *listenAddr,
		LogicAddrs: map[uint32]string{
			1: *logicAddr1,
			2: *logicAddr2,
		},
		PerShardBackMaxLen: 1000,
		BatchSize:          32,
		ConnPoolConfig: gateway.ConnConfig{
			LogicAddrs: map[uint32]string{
				1: *logicAddr1,
				2: *logicAddr2,
			},
			MaxConnPerLogic:     10,
			MinIdleConnPerLogic: 2,
			IdleTimeout:         30000,
			ReconnInterval:      3000,
			CreateConnLimit:     50,
		},
	}

	// 1. 初始化逻辑服连接池
	logicPool := gateway.NewLogicConnPool(cfg.ConnPoolConfig)
	defer logicPool.Close()

	// 2. 初始化分片背压Channel
	bpChan := gateway.NewGatewayBackPressureChan(cfg.PerShardBackMaxLen, cfg.BatchSize, logicPool)
	defer bpChan.Close()

	// 3. 启动TCP监听
	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("TCP listen failed: %v", err)
	}
	defer listener.Close()
	log.Printf("Gateway started, listening on %s", cfg.ListenAddr)

	// 4. 启动监控协程（每秒打印指标）
	go startMonitor(bpChan, logicPool)

	// 5. 启动信号处理（优雅退出）
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received exit signal, shutting down...")
		bpChan.Close()
		logicPool.Close()
		listener.Close()
		os.Exit(0)
	}()

	// 6. 接收客户端连接
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept failed: %v", err)
			continue
		}
		connID := genConnID()
		connMapMu.Lock()
		connMap[connID] = conn
		connMapMu.Unlock()
		log.Printf("New client connected, ConnID: %d, RemoteAddr: %s", connID, conn.RemoteAddr())

		// 启动客户端连接处理协程
		go handleClientConn(connID, conn, bpChan)
	}
}

// 处理客户端连接：读消息→封装→发送到背压Channel
func handleClientConn(connID uint64, conn net.Conn, bpChan *gateway.GatewayBackPressureChan) {
	defer func() {
		connMapMu.Lock()
		delete(connMap, connID)
		connMapMu.Unlock()
		conn.Close()
		log.Printf("Client disconnected, ConnID: %d", connID)
	}()

	// 协议头长度
	const headerLen = pbgate.HeaderLen
	buf := make([]byte, 4096)
	readBuf := make([]byte, 0, 4096)
	for {
		// 设置读超时（30秒）
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buf)
		if err != nil {
			log.Printf("Read from client failed, ConnID: %d, err: %v", connID, err)
			return
		}
		readBuf = append(readBuf, buf[:n]...)

		// 粘包/拆包处理
		for len(readBuf) >= headerLen {
			// 解析消息总长度
			totalLen := binary.BigEndian.Uint16(readBuf[pbgate.MagicNumLen : pbgate.MagicNumLen+pbgate.TotalLenLen])
			fullLen := pbgate.HeaderLen + int(totalLen) - pbgate.MsgIDLen - pbgate.TraceIDLen
			if len(readBuf) < fullLen {
				break // 消息不完整，等待后续数据
			}

			idSlice := readBuf[pbgate.MagicNumLen+pbgate.TotalLenLen : pbgate.MagicNumLen+pbgate.TotalLenLen+pbgate.MsgIDLen]
			msgID := binary.BigEndian.Uint32(idSlice)

			// 解码消息
			pbMsg := pbgate.GetPbObjByMsgID(msgID)
			if pbMsg == nil {
				log.Printf("Unknown MsgID: %d, ConnID: %d", msgID, connID)
				readBuf = readBuf[fullLen:]
				continue
			}
			_, traceID, err := pbgate.Decode(readBuf[:fullLen], pbMsg)
			if err != nil {
				log.Printf("Decode failed, ConnID: %d, err: %v", connID, err)
				readBuf = readBuf[fullLen:]
				pbgate.PutPbObjByMsgID(msgID, pbMsg)
				continue
			}

			// 封装网关消息（简化：固定LogicID=1，可根据负载均衡调整）
			gwMsg := gateway.GatewayMsg{
				ConnID:     connID,
				LogicID:    1,
				MsgID:      msgID,
				TraceID:    traceID,
				PbMsg:      pbMsg,
				TS:         time.Now().UnixMilli(),
				RetryCount: 0,
				TimeoutTS:  time.Now().Add(5 * time.Second).UnixMilli(),
				MsgLevel:   getMsgLevel(msgID), // 按MsgID分级
			}

			// 发送到背压Channel
			if !bpChan.Send(gwMsg) {
				log.Printf("Send to bpChan failed, ConnID: %d, MsgID: %d", connID, msgID)
			}

			// 移动缓冲区指针
			readBuf = readBuf[fullLen:]
		}
	}
}

// 按MsgID获取消息优先级（业务自定义）
func getMsgLevel(msgID uint32) int {
	switch msgID {
	case 1001: // 心跳：低优先级
		return gateway.MsgLevelLow
	case 2001, 3001: // 移动/聊天：高优先级
		return gateway.MsgLevelHigh
	default:
		return gateway.MsgLevelLow
	}
}

// 监控协程：每秒打印网关+连接池指标
func startMonitor(bpChan *gateway.GatewayBackPressureChan, logicPool *gateway.LogicConnPool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		// 网关指标
		totalSend, totalSucc, totalFail, backLen, backMaxLen, highDiscard, lowDiscard, fuseCount := bpChan.GetMetric()
		log.Printf(
			"Gateway: totalSend=%d, totalSucc=%d, totalFail=%d, backLen=%d, backMaxLen=%d, highDiscard=%d, lowDiscard=%d, fuseCount=%d",
			totalSend, totalSucc, totalFail, backLen, backMaxLen, highDiscard, lowDiscard, fuseCount,
		)

		// 连接池指标
		poolMetric := logicPool.GetMetric()
		log.Printf(
			"ConnPool: totalConn=%d, idleConn=%d, busyConn=%d, reuseCount=%d, createFail=%d, reuseRate=%.2f%%",
			poolMetric.TotalConn, poolMetric.IdleConn, poolMetric.BusyConn,
			poolMetric.ReuseCount, poolMetric.CreateFail, poolMetric.ReuseRate,
		)
	}
}

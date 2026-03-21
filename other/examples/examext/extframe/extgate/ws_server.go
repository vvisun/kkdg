package extgate

import (
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/lxzan/gws"
	"github.com/vvisun/kkdg/other/examples/examext/extframe/extmsg"
	"github.com/vvisun/kkdg/utils/kklog"
)

const sessionKeyClientConn = "clientConn"

// ====================== WS客户端处理 + 心跳 + 断开清理 ======================
type WsHandler struct{}

func (h *WsHandler) OnOpen(s *gws.Conn) {
	connID := nextConnID()
	c := &ClientConn{
		ws:         s,
		connID:     connID,
		writeGroup: int(connID % WriteGroupCnt),
		lastBeat:   time.Now().Unix(),
	}
	clientMap.Store(connID, c)
	s.Session().Store(sessionKeyClientConn, c)
	atomic.AddInt32(&clientCount, 1)
	go h.startHeartbeat(c)
}

func (h *WsHandler) OnMessage(s *gws.Conn, msg *gws.Message) {
	defer msg.Close()
	cc, ok := s.Session().Load(sessionKeyClientConn)
	if !ok {
		return
	}
	c := cc.(*ClientConn)
	if atomic.LoadInt32(&c.closed) == 1 {
		return
	}

	// 心跳更新
	atomic.StoreInt64(&c.lastBeat, time.Now().Unix())

	// 解析命令
	var req struct {
		Cmd string `json:"cmd"`
		Uid uint64 `json:"uid"`
	}
	_ = json.Unmarshal(msg.Bytes(), &req)

	// 心跳包直接响应，不上发逻辑服
	if req.Cmd == extmsg.CmdHeartbeat {
		resp, _ := json.Marshal(map[string]string{"cmd": extmsg.CmdHeartbeatAck})
		sendToClient(c.connID, resp)
		return
	}

	// 设置UID
	if req.Uid != 0 {
		c.uid = req.Uid
	}

	// 转发逻辑服
	transToLogic(c.connID, msg.Bytes(), c.uid, req.Cmd)
}

func (h *WsHandler) OnPing(s *gws.Conn, payload []byte) {
	_ = s.WritePong(nil)
}

func (h *WsHandler) OnPong(s *gws.Conn, payload []byte) {

}

func (h *WsHandler) OnClose(s *gws.Conn, err error) {
	cc, ok := s.Session().Load(sessionKeyClientConn)
	if !ok {
		return
	}
	c := cc.(*ClientConn)
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		clientMap.Delete(c.connID)
		atomic.AddInt32(&clientCount, -1)
		kklog.Debugf("客户端已清理 connID=%d uid=%d err=%v", c.connID, c.uid, err)

		// 通知逻辑服：玩家断开
		go func() {
			if conn := routeLogicConn(c.connID); conn != nil {
				bs, _ := json.Marshal(extmsg.UpMsg{
					ConnID: c.connID,
					Uid:    c.uid,
					Cmd:    extmsg.CmdClientDisconnect,
				})
				_, _ = conn.Write(append(bs, '\n'))
			}
		}()
	}
}

// 心跳超时检测
func (h *WsHandler) startHeartbeat(c *ClientConn) {
	ticker := time.NewTicker(ClientHeartbeatSec * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if atomic.LoadInt32(&c.closed) == 1 {
			return
		}
		now := time.Now().Unix()
		last := atomic.LoadInt64(&c.lastBeat)
		if now-last > ClientHeartbeatSec*ClientMaxMiss {
			if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
				clientMap.Delete(c.connID)
				atomic.AddInt32(&clientCount, -1)
				kklog.Debugf("心跳超时关闭 connID=%d uid=%d", c.connID, c.uid)
			}
			_ = c.ws.WriteClose(1000, []byte("heartbeat_timeout"))
			return
		}
	}
}

package cnats

import (
	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xcall"
)

// handleResponse 处理响应（单订阅分发）
func (c *NatsCluster) handleResponse(msg *nats.Msg) {
	var resp kkcluster.ClusterResponse
	if err := c.msgCodec.Unmarshal(msg.Data, &resp); err != nil {
		kklog.Errorf("NatsCluster(%s) unmarshal response failed: %v", c.nodeID, err)
		c.stats.AddError()
		return
	}
	if resp.RequestID == "" {
		// 防御：没有 requestID 的响应无法路由
		c.stats.AddError()
		return
	}

	// 1) 优先投递同步请求（如果还在等待）
	c.requestMapMu.RLock()
	ch := c.requestMap[resp.RequestID]
	c.requestMapMu.RUnlock()
	if ch != nil {
		select {
		case ch <- &resp:
		default:
		}
		// 同步路径的 AddResponseReceived 在 RequestRemote 等待处统计
		return
	}

	// 2) 投递异步请求（如果还在等待）
	c.reqMu.Lock()
	p := c.reqMap[resp.RequestID]
	delete(c.reqMap, resp.RequestID)
	c.reqMu.Unlock()
	if p == nil || p.cb == nil {
		return
	}
	// 取消超时 timer，避免响应到达后超时回调再次触发
	if p.timer != nil {
		p.timer.Stop()
	}

	// 记录接收响应统计（异步路径只有这里能统计）
	c.stats.AddResponseReceived(len(resp.Data))

	data := make([]byte, len(resp.Data))
	copy(data, resp.Data)
	code := kkcluster.ClusterErrorCode(resp.Code)
	cb := p.cb
	p.cb, p.timer = nil, nil
	asyncReqPool.Put(p)
	xcall.AntsGo(func() {
		defer func() {
			if r := recover(); r != nil {
				c.stats.AddError()
				kklog.Errorf("NatsCluster(%s) RequestRemoteAsync callback panic: %v", c.nodeID, r)
			}
		}()
		cb(data, code)
	})
}

// handleRequest 处理请求
func (c *NatsCluster) handleRequest(msg *nats.Msg) {
	// 记录接收请求统计
	c.stats.AddRequestReceived(len(msg.Data))

	var req kkcluster.ClusterRequest
	if err := c.msgCodec.Unmarshal(msg.Data, &req); err != nil {
		kklog.Errorf("NatsCluster(%s) unmarshal request failed: %v", c.nodeID, err)
		c.stats.AddError()
		return
	}

	response := &kkcluster.ClusterResponse{
		RequestID: req.RequestID,
		Code:      int32(kkcluster.ClusterErrorCodeFail),
		Data:      nil,
	}
	// 调用用户注册的处理器来处理请求
	if c.requestHandler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.stats.AddError()
					kklog.Errorf("NatsCluster(%s) request handler panic: %v", c.nodeID, r)
				}
			}()
			resp, err := c.requestHandler(&req)
			if err != nil || resp == nil {
				kklog.Errorf("NatsCluster(%s) request handler failed: %v", c.nodeID, err)
				c.stats.AddError()
				return
			}
			response.Code = resp.Code
			response.Data = resp.Data
		}()
	}

	respBytes, err := c.msgCodec.Marshal(response)
	if err != nil {
		kklog.Errorf("NatsCluster(%s) marshal response failed: %v", c.nodeID, err)
		c.stats.AddError()
		return
	}

	// NATS Request 路径：直接 Reply 到客户端 inbox（与 conn.Request 配对）
	if msg.Reply != "" {
		if err := msg.Respond(respBytes); err != nil {
			kklog.Errorf("NatsCluster(%s) respond failed: %v", c.nodeID, err)
			c.stats.AddError()
			return
		}
		c.stats.AddResponseSent(len(respBytes))
		return
	}

	// 异步/兼容路径：仍发往 kkcluster.response.<requestID>
	responseSubject := c.getResponseSubject(req.RequestID)
	if err := c.conn.Publish(responseSubject, respBytes); err != nil {
		kklog.Errorf("NatsCluster(%s) publish response failed: %v", c.nodeID, err)
		c.stats.AddError()
	} else {
		c.stats.AddResponseSent(len(respBytes))
	}
}

// handlePublish 处理发布消息（来自节点ID主题）
func (c *NatsCluster) handlePublish(msg *nats.Msg) {
	// 记录接收发布消息统计
	c.stats.AddPublishReceived(len(msg.Data))

	c.workerQueue.Push(func() {
		var packet kkcluster.ClusterPacket
		if err := c.msgCodec.Unmarshal(msg.Data, &packet); err != nil {
			kklog.Errorf("NatsCluster(%s) unmarshal publish packet failed: %v", c.nodeID, err)
			c.stats.AddError()
			return
		}

		// 调用用户注册的处理器
		if c.publishHandler != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						c.stats.AddError()
						kklog.Errorf("NatsCluster(%s) publish handler panic: %v", c.nodeID, r)
					}
				}()
				c.publishHandler(packet.SourcePath, &packet)
			}()
		}
	})
}

// handleTypePublish 处理类型发布消息（来自类型主题）
func (c *NatsCluster) handleTypePublish(msg *nats.Msg) {
	// 记录接收发布消息统计
	c.stats.AddPublishReceived(len(msg.Data))

	c.workerQueue.Push(func() {
		var packet kkcluster.ClusterPacket
		if err := c.msgCodec.Unmarshal(msg.Data, &packet); err != nil {
			kklog.Errorf("NatsCluster(%s) unmarshal type publish packet failed: %v", c.nodeID, err)
			c.stats.AddError()
			return
		}

		// 调用用户注册的处理器
		if c.publishHandler != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						c.stats.AddError()
						kklog.Errorf("NatsCluster(%s) type publish handler panic: %v", c.nodeID, r)
					}
				}()
				c.publishHandler(packet.SourcePath, &packet)
			}()
		}
	})
}

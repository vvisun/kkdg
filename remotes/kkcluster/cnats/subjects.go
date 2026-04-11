package cnats

import (
	"strconv"

	"github.com/vvisun/kkdg/remotes/kkcluster"
)

// generateRequestID 生成请求ID
func (c *NatsCluster) generateRequestID() string {
	seq := kkcluster.GenRequestID()
	return c.nodeID + "." + strconv.FormatUint(seq, 10)
}

// getRequestSubject 获取自己的请求主题（用于接收其他节点的请求）
func (c *NatsCluster) getRequestSubject() string {
	return "kkcluster.request." + c.nodeID
}

// getRequestSubjectForNode 获取指定节点的请求主题（用于向目标节点发送请求）
func (c *NatsCluster) getRequestSubjectForNode(nodeID string) string {
	return "kkcluster.request." + nodeID
}

// getResponseSubject 获取响应主题
func (c *NatsCluster) getResponseSubject(requestID string) string {
	return "kkcluster.response." + requestID
}

// getResponseSubjectPattern 返回本节点需要订阅的响应主题（通配）
func (c *NatsCluster) getResponseSubjectPattern() string {
	// requestID = <sourceNodeID>.<seq>，响应主题为 kkcluster.response.<requestID>
	// 仅订阅本节点发起请求的响应：kkcluster.response.<nodeID>.>
	return "kkcluster.response." + c.nodeID + ".>"
}

// getPublishSubject 获取发布主题
func (c *NatsCluster) getPublishSubject(nodeID string) string {
	return "kkcluster.publish." + nodeID
}

// getPublishTypeSubject 获取类型发布主题
func (c *NatsCluster) getPublishTypeSubject(nodeType string) string {
	return "kkcluster.publish.type." + nodeType
}

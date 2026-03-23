package kkgin

import (
	"strings"

	"github.com/gin-gonic/gin"
)

func GetIPWithProxyHeaders(c *gin.Context) string {
	// 尝试从 X-Real-IP 头部获取真实 IP
	ip := c.GetHeader("X-Real-IP")

	// 如果 X-Real-IP 头部不存在，则尝试从 X-Forwarded-For 头部获取
	if ip == "" {
		ip = c.GetHeader("X-Forwarded-For")
	}

	// 如果两者都不存在，则使用默认的 ClientIP 方法获取 IP
	if ip == "" {
		ip = c.ClientIP()
	}

	return ip
}

func GetIPWithValidatedProxyHeaders(c *gin.Context) string {
	// 获取代理头部
	proxyHeaders := c.Request.Header.Get("X-Real-IP,X-Forwarded-For")

	// 分割代理头部，取第一个 IP 作为真实 IP
	ips := strings.Split(proxyHeaders, ",")
	ip := strings.TrimSpace(ips[0])

	// 如果 IP 格式合法，则使用获取到的 IP，否则使用默认的 ClientIP 方法获取
	if isValidIP(ip) {
		return ip
	} else {
		ip = c.ClientIP()
		return ip
	}
}

// isValidIP 判断 IP 格式是否合法
func isValidIP(ip string) bool {
	// 此处添加自定义的 IP 格式验证逻辑
	// 例如，使用正则表达式验证 IP 格式
	// ...
	if ip == "" {
		return false
	}

	return true
}

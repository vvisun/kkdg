// Package kkgin 提供基于 Gin 的独立 HTTP 服务封装（非 kkapp 组件、非 actor）。
//
// 典型用法：
//
//	srv := kkgin.NewServer(kkgin.ApplyOptions(
//	    kkgin.WithHttpAddr(":8080"),
//	    kkgin.WithRegisterRoutes(func(e *gin.Engine) {
//	        e.GET("/healthz", func(c *gin.Context) { c.String(200, "ok") })
//	    }),
//	))
//	if err := srv.Start(); err != nil { ... }
//	defer srv.Stop()
package kkgin

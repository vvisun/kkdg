package kkgin

import (
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// defaultCorsConfig 默认跨域；AllowOrigins 为 * 时 AllowCredentials 须为 false，否则浏览器会拒绝。
var defaultCorsConfig = cors.Config{
	AllowOrigins:     []string{"*"},
	AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"},
	AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "User-Agent", "Referrer", "Host"},
	ExposeHeaders:    []string{"Content-Length"},
	AllowCredentials: false,
	MaxAge:           12 * time.Hour,
}

func cfgCors(eng *gin.Engine, cfg *cors.Config) {
	if cfg == nil {
		eng.Use(cors.New(defaultCorsConfig))
		return
	}
	eng.Use(cors.New(*cfg))
}

type Options struct {
	HttpAddr string
	CertFile string
	KeyFile  string

	// Middlewares 按顺序 engine.Use
	Middlewares []gin.HandlerFunc

	// RegisterRoutes 在 NewServer 时调用一次，用于注册路由。
	RegisterRoutes func(engine *gin.Engine)

	// ShutdownTimeout 用于 Stop 时的优雅关闭超时。
	ShutdownTimeout time.Duration

	// CorsConfig 为 nil 时使用默认跨域配置。
	CorsConfig *cors.Config
}

func defaultOptions() Options {
	return Options{
		HttpAddr:        "0.0.0.0:8080",
		ShutdownTimeout: 5 * time.Second,
	}
}

func ApplyOptions(opts ...func(o *Options)) Options {
	cfg := defaultOptions()
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

func WithHttpAddr(httpAddr string) func(o *Options) {
	return func(o *Options) {
		o.HttpAddr = httpAddr
	}
}

func WithCertFile(certFile string) func(o *Options) {
	return func(o *Options) {
		o.CertFile = certFile
	}
}

func WithKeyFile(keyFile string) func(o *Options) {
	return func(o *Options) {
		o.KeyFile = keyFile
	}
}

func WithMiddlewares(mws ...gin.HandlerFunc) func(o *Options) {
	return func(o *Options) {
		o.Middlewares = append(o.Middlewares, mws...)
	}
}

func WithRegisterRoutes(register func(engine *gin.Engine)) func(o *Options) {
	return func(o *Options) {
		o.RegisterRoutes = register
	}
}

func WithShutdownTimeout(timeout time.Duration) func(o *Options) {
	return func(o *Options) {
		if timeout > 0 {
			o.ShutdownTimeout = timeout
		}
	}
}

func WithCorsConfig(corsConfig *cors.Config) func(o *Options) {
	return func(o *Options) {
		o.CorsConfig = corsConfig
	}
}

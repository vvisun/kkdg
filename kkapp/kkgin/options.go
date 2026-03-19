package kkgin

import (
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

var defaultCorsConfig = cors.Config{
	AllowOrigins:     []string{"*"},
	AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"},
	AllowHeaders:     []string{"Origin", "Content-Length", "Content-Type", "User-Agent", "Referrer", "Host"},
	ExposeHeaders:    []string{"Content-Length"},
	AllowCredentials: true,           //允许携带cookie
	MaxAge:           12 * time.Hour, //预检请求的缓存时间
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

	// Middlewares will be applied in order via engine.Use().
	Middlewares []gin.HandlerFunc

	// RegisterRoutes will be called during OnInit before server start.
	// Use it to define routes / handlers.
	//  ccgin.WithRegisterRoutes(func(e *gin.Engine) {
	//	    e.GET("/healthz", func(c *gin.Context) { c.String(200, "ok") })
	//  })
	RegisterRoutes func(engine *gin.Engine)

	// ShutdownTimeout is used in OnStop for graceful shutdown.
	ShutdownTimeout time.Duration

	// 跨域配置接口。如果为nil，则使用默认配置。
	CorsConfig *cors.Config
}

func defaultOptions() Options {
	return Options{
		HttpAddr: "0.0.0.0:8080",
		// Keep default small so shutdown during tests is not too slow.
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

module github.com/vvisun/kkdg

go 1.25.3

require (
	github.com/BurntSushi/toml v1.6.0
	github.com/asynkron/protoactor-go v0.0.0-20260118094027-288962e52f3f
	github.com/bytedance/sonic v1.14.2
	github.com/google/flatbuffers v25.12.19+incompatible
	github.com/gorilla/websocket v1.5.3
	github.com/lxzan/gws v1.8.9
	github.com/nats-io/nats.go v1.48.0
	github.com/shamaton/msgpack/v2 v2.4.0
	go.uber.org/zap v1.27.0
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/Workiva/go-datastructures v1.1.7 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dolthub/maphash v0.1.0 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/grafana/regexp v0.0.0-20250905093917-f7b3be9d1853 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/klauspost/compress v1.18.2 // indirect
	github.com/lithammer/shortuuid/v4 v4.2.0 // indirect
	github.com/lmittmann/tint v1.1.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nats-io/nkeys v0.4.12 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/orcaman/concurrent-map v1.0.0 // indirect
	github.com/prometheus/client_golang v1.23.2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/otlptranslator v0.0.2 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.60.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	golang.org/x/crypto v0.46.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	xorm.io/builder v0.3.6 // indirect
	xorm.io/core v0.7.2-0.20190928055935-90aeac8d08eb // indirect
)

require (
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic/loader v0.4.0 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/go-sql-driver/mysql v1.9.3
	github.com/go-xorm/xorm v0.7.9
	github.com/google/uuid v1.6.0
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/lestrrat-go/strftime v1.1.1
	github.com/panjf2000/ants/v2 v2.11.4
	github.com/panjf2000/gnet/v2 v2.9.7
	github.com/shirou/gopsutil v2.21.11+incompatible
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/sys v0.39.0 // indirect
	gorm.io/driver/mysql v1.5.7
	gorm.io/gorm v1.25.10
)

replace github.com/lxzan/gws => ../gws

replace github.com/panjf2000/gnet/v2 => ../gnet

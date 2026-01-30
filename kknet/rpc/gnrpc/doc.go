package gnrpc

/**
基于gnet实现的grpc网络库。仿grpc

gnet: 是一个高性能的网络库，支持TCP和UDP协议。
地址: https://github.com/panjf2000/gnet

grpc: 是一个高性能的rpc库。
地址: https://github.com/grpc/grpc-go

stub 生成器（生成类似 protoc-gen-go-grpc 的 Go 代码风格）：

	# 从 spec.json 生成 stub
	go run ./cmd/gnrpcstubgen -in ./kknet/rpc/gnrpc/stubgen/example_string_service.json -out ./proto/pbbase/string_service_gnrpc.pb.go

	# 批量生成（目录下所有 *.json）
	go run ./cmd/gnrpcstubgen -in ./kknet/rpc/gnrpc/stubgen -outdir ./proto/pbbase

	# 递归批量生成（目录递归）
	go run ./cmd/gnrpcstubgen -in ./kknet/rpc/gnrpc/stubgen -r -outdir ./proto/pbbase

	# bundle 文件（一个 json 里多个 service）
	go run ./cmd/gnrpcstubgen -in ./kknet/rpc/gnrpc/stubgen/example_bundle.json


*/

// Package fbspb 提供 FlatBuffers 与 Protobuf 的互相转换及代码生成。
//
// 命令行用法（单文件）：
//
//	go run ./tools/cmd/fbspb fbs2proto path/to/file.fbs
//	go run ./tools/cmd/fbspb proto2fbs path/to/file.proto
//
// 目录（递归）：将 path 换为目录即可。生成 FlatBuffers 代码需系统已安装 flatc：https://github.com/google/flatbuffers/releases
package fbspb

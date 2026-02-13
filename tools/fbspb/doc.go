package fbspb

/*
FlatBuffers 与 Protobuf 的互相转换

# 单个文件
go run ./tools/cmd/fbspb fbs2proto path/to/file.fbs
go run ./tools/cmd/fbspb proto2fbs path/to/file.proto

# 目录（递归）
go run ./tools/cmd/fbspb fbs2proto path/to/fbs_dir
go run ./tools/cmd/fbspb proto2fbs path/to/proto_dir


# 生成 Flatbuffer 代码
前提：系统已安装 flatc（FlatBuffers 编译器），可从这里获取：https://github.com/google/flatbuffers/releases

*/

./protoc --go_out=./pbcluster ./pbcluster/*.proto
./protoc --go_out=./pbrpc ./pbrpc/*.proto
# pto.proto lives under kkapp/transport/ptotrans: -I must be that dir; pass paths relative to -I
./protoc --proto_path=../kkapp/transport/ptotrans --go_out=paths=source_relative:../kkapp/transport/ptotrans pto.proto

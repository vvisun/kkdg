protoc.exe --go_out=./pbcluster ./pbcluster/*.proto
protoc.exe --go_out=./pbrpc ./pbrpc/*.proto
rem pto.proto 不在 proto\ 下：须指定 --proto_path 为 .proto 所在目录；文件名写该目录下的相对路径（如 pto.proto）
rem 避免使用 -I=../...（PowerShell 会误解析）；用 --proto_path= 更稳
protoc.exe --proto_path=../kkapp/transport/ptotrans --go_out=paths=source_relative:../kkapp/transport/ptotrans pto.proto

PAUSE
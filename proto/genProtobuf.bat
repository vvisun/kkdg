protoc.exe --go_out=./pbcluster ./pbcluster/*.proto
protoc.exe --go_out=./pbrpc ./pbrpc/*.proto
protoc.exe --go_out=./pbgate ./pbgate/*.proto

PAUSE
protoc.exe --go_out=./pbbase ./pbbase/*.proto
protoc.exe --go_out=./pbcluster ./pbcluster/*.proto
protoc.exe --go_out=./pb ./pb/*.proto

PAUSE
@echo off
cd /d %~dp0..
echo Converting .proto to .fbs in proto/...
go run ./tools/cmd/fbspb proto2fbs ./proto
echo Done.
PAUSE

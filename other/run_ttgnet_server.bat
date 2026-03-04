@echo off
cd /d %~dp0\..
echo Starting ttgnet gnet-only TCP server...
go run ./other/tests/ttgnet/ttgnetserver
pause


@echo off
cd /d %~dp0\..
echo Starting ttgnet gnet-only TCP server...
go run ../tests/ttgnet/ttgnetserver
pause


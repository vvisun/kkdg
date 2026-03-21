@echo off
cd /d %~dp0\..
echo Starting ttgnet gnet-only TCP server...
go run ../zothers/tests/ttgnet/ttgnetserver
pause


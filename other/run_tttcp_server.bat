@echo off
cd /d %~dp0\..
go run ./other/tests/tttcp/tttcpserver
PAUSE
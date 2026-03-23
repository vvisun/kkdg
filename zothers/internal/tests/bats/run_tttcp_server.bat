@echo off
cd /d %~dp0\..
go run ../tests/tttcp/tttcpserver
PAUSE
@echo off
cd /d %~dp0\..
go run ../zothers/tests/tttcp/tttcpserver
PAUSE
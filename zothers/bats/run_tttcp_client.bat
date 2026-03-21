@echo off
cd /d %~dp0\..
go run ../zothers/tests/tttcp/tttcpclient -addr=127.0.0.1:19090 -conn=20000 -size=1222 -interval=150ms
PAUSE
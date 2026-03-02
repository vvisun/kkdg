@echo off
go run ./other/tests/ttgws/ttgwsclient -addr=localhost:8080 -conn=100000 -size=256 -interval=1000ms
PAUSE

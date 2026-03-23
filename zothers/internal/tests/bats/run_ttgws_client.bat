@echo off
cd /d %~dp0\..
rem conn连接数 size每个消息包的最小字节数 interval每个连接的消息发送间隔 connDelay建连间隔
go run ../tests/ttgws/ttgwsclient -addr=localhost:8080 -conn=30000 -size=1256 -interval=225ms -connDelay=5ms
PAUSE

@echo off
rem conn连接数 size每个消息包的最小字节数 interval每个连接的消息发送间隔 connDelay建连间隔
go run ./other/tests/ttgws/ttgwsclient -addr=localhost:8080 -conn=100000 -size=1256 -interval=25ms -connDelay=5ms
PAUSE

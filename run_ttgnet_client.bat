@echo off
rem conn 连接数  size 每条消息大小（字节） interval 每连接发送间隔  connDelay 建连间隔
go run ./other/tests/ttgnet/ttgnetclient -addr=127.0.0.1:19190 -conn=80000 -size=222 -interval=3200ms -connDelay=2ms
PAUSE

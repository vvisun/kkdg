@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0"
cd ..\..\..

go run ./other/examples/examapp/aclient -conn-num=1112 -conn-delay=5ms -send-interval=200ms -gate-tcp=127.0.0.1:19090 -gate-ws=127.0.0.1:19091
endlocal

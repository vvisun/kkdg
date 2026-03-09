@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0"
cd ..\..\..

go run ./other/examples/examapp/aclient -gate-tcp=127.0.0.1:19090 -gate-ws=127.0.0.1:19091 -conn-num=2500 -conn-delay=5ms -send-interval=250ms
endlocal

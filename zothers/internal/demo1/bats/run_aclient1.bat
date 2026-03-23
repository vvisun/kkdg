@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0"
cd ..\..\..\..

go run ./zothers/internal/demo1/examapp/aclient -conn-num=1000 -conn-delay=5ms -send-interval=100ms -gate-tcp=127.0.0.1:19091 -gate-ws=127.0.0.1:19092
endlocal

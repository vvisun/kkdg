@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0"
cd ..\..\..\..

go run ./zothers/examples/examapp/aserver -node-id=game3 -trans=rpc -nats-url=nats://127.0.0.1:4222 -gate-tcp=127.0.0.1:19090 -gate-ws=127.0.0.1:19091 -trans-addr=127.0.0.1:19092
endlocal

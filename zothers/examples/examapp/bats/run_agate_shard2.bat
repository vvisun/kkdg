@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0"
cd ..\..\..\..

go run ./zothers/examples/examapp/agate -trans=shard -nats-url=nats://127.0.0.1:4222 -gate-tcp=127.0.0.1:19093 -gate-ws=127.0.0.1:19094 -trans-addr=127.0.0.1:19012
endlocal

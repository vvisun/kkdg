@echo off
cd /d %~dp0\..
go run ../zothers/tests/ttws/ttwsclient -addr=localhost:8080 -conn=6000 -size=666 -interval=50ms
PAUSE
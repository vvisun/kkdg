@echo off
cd /d %~dp0\..
go run ../tests/ttgws/ttgwsserver
PAUSE

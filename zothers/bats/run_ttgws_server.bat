@echo off
cd /d %~dp0\..
go run ../zothers/tests/ttgws/ttgwsserver
PAUSE

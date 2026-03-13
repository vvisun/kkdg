@echo off
cd /d %~dp0\..
go run ../other/tests/ttws/ttwsserver
PAUSE
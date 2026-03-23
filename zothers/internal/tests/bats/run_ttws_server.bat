@echo off
cd /d %~dp0\..
go run ../tests/ttws/ttwsserver
PAUSE
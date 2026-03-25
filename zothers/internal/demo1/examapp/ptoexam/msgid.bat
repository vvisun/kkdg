@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0..\..\..\..\.."
go run ./tools/cmd/msdid "%~dp0."
if errorlevel 1 (
  endlocal
  exit /b 1
)
endlocal
exit /b 0

@echo off
chcp 65001 >nul
setlocal

echo [examapp] 启动 agate / aserver / aclient，每个在新窗口中运行。
echo.

start "agate" cmd /k "%~dp0run_agate.bat"
timeout /t 2 /nobreak >nul

start "aserver" cmd /k "%~dp0run_aserver.bat"
timeout /t 2 /nobreak >nul

start "aclient" cmd /k "%~dp0run_aclient.bat"

echo [examapp] 已启动三个窗口。关闭各窗口或在该窗口按 Ctrl+C 可退出对应进程。
endlocal

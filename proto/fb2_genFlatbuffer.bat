@echo off
setlocal enabledelayedexpansion
for %%d in (pbbase pbcluster pbrpc) do (
    set "files="
    for /f "delims=" %%f in ('dir /b "%%d\*.fbs" 2^>nul') do set "files=!files! %%d\%%f"
    if defined files (
        echo Generating Go from %%d/*.fbs
        flatc --go -o ./%%d !files!
    )
)
endlocal
PAUSE

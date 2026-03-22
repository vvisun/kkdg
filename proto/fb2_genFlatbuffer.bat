@echo off
setlocal enabledelayedexpansion
for %%d in (pbgate pbcluster pbrpc) do (
    set "files="
    for /f "delims=" %%f in ('dir /b "ptoflats\%%d\*.fbs" 2^>nul') do set "files=!files! ptoflats\%%d\%%f"
    if defined files (
        echo Generating Go from ptoflats/%%d/*.fbs
        flatc --go -o ./ptoflats/%%d !files!
    )
)
endlocal
PAUSE

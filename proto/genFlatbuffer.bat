@echo off
for %%d in (pbbase pbcluster pbrpc) do (
    if exist "%%d\*.fbs" (
        echo Generating Go from %%d/*.fbs
        flatc --go -o ./%%d ./%%d/*.fbs
    )
)
PAUSE

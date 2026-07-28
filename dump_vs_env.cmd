@echo off
call "C:\Program Files\Microsoft Visual Studio\18\Community\Common7\Tools\VsDevCmd.bat" -arch=x64
echo PATH=%PATH% > vs_env.txt
where cl >> vs_env.txt 2>&1 || echo cl not found >> vs_env.txt
where nmake >> vs_env.txt 2>&1 || echo nmake not found >> vs_env.txt

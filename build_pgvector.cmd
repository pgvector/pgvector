@echo off
set "MSVC_BIN=C:\Program Files\Microsoft Visual Studio\18\Community\VC\Tools\MSVC\14.44.35207\bin\HostX64\x64"
set "PATH=%MSVC_BIN%;%PATH%"
set "PGROOT=C:\Program Files\PostgreSQL\18"
cd /d "%~dp0"
nmake /F Makefile.win || goto :err
nmake /F Makefile.win install || goto :err
necho Build and install finished successfully
goto :eof
:err
echo Build failed with error code %errorlevel%
exit /b %errorlevel%

@echo off
set "MSVC_ROOT=C:\Program Files\Microsoft Visual Studio\18\Community\VC\Tools\MSVC\14.44.35207"
set "MSVC_BIN=%MSVC_ROOT%\bin\HostX64\x64"
set "MSVC_INCLUDE=%MSVC_ROOT%\include"
set "MSVC_LIB=%MSVC_ROOT%\lib\x64"
set "WINSDK_VER=10.0.26100.0"
set "WINSDK_INC=C:\Program Files (x86)\Windows Kits\10\Include\%WINSDK_VER%"
set "WINSDK_LIB=C:\Program Files (x86)\Windows Kits\10\Lib\%WINSDK_VER%"
set "INCLUDE=%MSVC_INCLUDE%;%WINSDK_INC%\ucrt;%WINSDK_INC%\shared;%WINSDK_INC%\um;%INCLUDE%"
set "LIB=%MSVC_LIB%;%WINSDK_LIB%\ucrt\x64;%WINSDK_LIB%\um\x64;%LIB%"
set "PATH=%MSVC_BIN%;%PATH%"
set "PGROOT=C:\Program Files\PostgreSQL\18"
cd /d "%~dp0"
nmake /F Makefile.win || goto :err
nmake /F Makefile.win install || goto :err
echo Build and install finished successfully
goto :eof
:err
echo Build failed with error code %errorlevel%
exit /b %errorlevel%

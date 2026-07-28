@echo off
set PGROOT=C:\PROGRA~1\POSTGR~1\18
cd /d "%~dp0"
"C:\Program Files\Microsoft Visual Studio\18\Community\VC\Tools\MSVC\14.44.35207\bin\HostX64\x64\nmake.exe" /F Makefile.win install

@echo off
for %%I in ("C:\Program Files\PostgreSQL\18") do @echo %%~sI > pgroot_short.txt
echo Written short path to pgroot_short.txt

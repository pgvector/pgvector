$ErrorActionPreference = 'Stop'
$src = Split-Path -Parent $MyInvocation.MyCommand.Path
$pgroot = 'C:\Program Files\PostgreSQL\18'
Write-Host "Copying vector.dll to $pgroot\lib"
Copy-Item -Path (Join-Path $src 'vector.dll') -Destination (Join-Path $pgroot 'lib') -Force
Write-Host "Copying vector.control to $pgroot\share\extension"
Copy-Item -Path (Join-Path $src 'vector.control') -Destination (Join-Path $pgroot 'share\extension') -Force
Write-Host "Copying SQL file to $pgroot\share\extension"
Copy-Item -Path (Join-Path $src 'sql\vector--0.8.5.sql') -Destination (Join-Path $pgroot 'share\extension') -Force
# Optionally copy headers
$incdest = Join-Path $pgroot 'include\server\extension\vector'
if (-not (Test-Path $incdest)) { New-Item -ItemType Directory -Path $incdest -Force | Out-Null }
Copy-Item -Path (Join-Path $src 'src\*.h') -Destination $incdest -Force
Write-Host 'Files copied. Attempting to run CREATE EXTENSION via psql (may prompt for password).'
$psql = Join-Path $pgroot 'bin\psql.exe'
if (Test-Path $psql) {
    & $psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS vector;"
} else {
    Write-Host 'psql not found at expected location:' $psql
}
Write-Host 'Done.'

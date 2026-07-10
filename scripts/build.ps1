# PowerShell wrapper for parity with prior course repos.
$root = Split-Path -Parent $PSScriptRoot
python (Join-Path $root "scripts\build.py") @args
exit $LASTEXITCODE

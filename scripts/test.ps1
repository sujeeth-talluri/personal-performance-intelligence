$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$embeddedPython = Join-Path $repoRoot ".python311_embed\\python.exe"
$venvPython = Join-Path $repoRoot ".venv312\\Scripts\\python.exe"
$fallbackVenvPython = Join-Path $repoRoot ".venv\\Scripts\\python.exe"

if (Test-Path $embeddedPython) {
    $python = $embeddedPython
} elseif (Test-Path $venvPython) {
    $python = $venvPython
} elseif (Test-Path $fallbackVenvPython) {
    $python = $fallbackVenvPython
} else {
    throw "No usable Python found. Expected one of: $embeddedPython, $venvPython, $fallbackVenvPython"
}

$sandboxVendor = Join-Path $repoRoot ".sandbox_vendor"
$tempDir = Join-Path $repoRoot ".tmp_pytest_run"
$cacheDir = Join-Path $repoRoot ".pytest_local_cache_run"

New-Item -ItemType Directory -Force $tempDir | Out-Null
New-Item -ItemType Directory -Force $cacheDir | Out-Null

$env:TEMP = $tempDir
$env:TMP = $tempDir

if (Test-Path $sandboxVendor) {
    $env:PYTHONPATH = "$sandboxVendor;$repoRoot"
} else {
    $env:PYTHONPATH = $repoRoot
}

& $python -m pytest -q tests --basetemp=$tempDir -o cache_dir=$cacheDir

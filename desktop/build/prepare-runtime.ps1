Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$DesktopRoot = Split-Path -Parent $PSScriptRoot
$RepoRoot = Split-Path -Parent $DesktopRoot
$StageRoot = Join-Path $PSScriptRoot "runtime-staged-release"
$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$FrontendSource = Get-ChildItem -LiteralPath $RepoRoot -Recurse -Force -Directory -ErrorAction Stop |
  Where-Object {
    $_.FullName -notlike "$RepoRoot\desktop\build\*" -and
    $_.FullName -notlike "$RepoRoot\desktop\dist\*" -and
    $_.FullName -notlike "$RepoRoot\desktop\dist-installer-test\*" -and
    $_.FullName -notlike "$RepoRoot\public-repo-export*" -and
    (Test-Path -LiteralPath (Join-Path $_.FullName "app.js")) -and
    (Test-Path -LiteralPath (Join-Path $_.FullName "index.html"))
  } |
  Sort-Object FullName |
  Select-Object -First 1 -ExpandProperty FullName
if (-not $FrontendSource) {
  throw "Frontend source directory not found (expected app.js + index.html in repository source tree)."
}
$MinerSource = Get-ChildItem -LiteralPath $RepoRoot -Recurse -Force -File -Filter "heph.py" -ErrorAction Stop |
  Select-Object -First 1 -ExpandProperty FullName
$MinerSourceDir = Split-Path -Parent $MinerSource

function Reset-Dir {
  param([string]$Path)
  if (Test-Path -LiteralPath $Path) {
    Get-ChildItem -LiteralPath $Path -Recurse -Force -ErrorAction SilentlyContinue | ForEach-Object {
      try { $_.Attributes = 'Normal' } catch {}
    }
    Get-ChildItem -LiteralPath $Path -Force -ErrorAction SilentlyContinue | ForEach-Object {
      Remove-Item -LiteralPath $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
    }
    if ((Get-ChildItem -LiteralPath $Path -Force -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0) {
      Write-Warning "Directory still has locked items, will reuse and overwrite: $Path"
    }
  }
  New-Item -ItemType Directory -Path $Path -Force | Out-Null
}

function Ensure-Dir {
  param([string]$Path)
  New-Item -ItemType Directory -Path $Path -Force | Out-Null
}

function Copy-Tree {
  param(
    [string]$Source,
    [string]$Destination
  )
  Ensure-Dir $Destination
  Get-ChildItem -LiteralPath $Source -Force -ErrorAction Stop | ForEach-Object {
    Copy-Item -LiteralPath $_.FullName -Destination $Destination -Recurse -Force
  }
}

function Copy-Files {
  param(
    [string]$Source,
    [string]$Destination,
    [string[]]$FileNames
  )
  Ensure-Dir $Destination
  foreach ($fileName in $FileNames) {
    $sourcePath = Join-Path $Source $fileName
    if (-not (Test-Path -LiteralPath $sourcePath)) {
      throw "Required file missing from staged source: $sourcePath"
    }
    Copy-Item -LiteralPath $sourcePath -Destination $Destination -Force
  }
}

function Remove-TreeItems {
  param(
    [string]$Base,
    [string[]]$DirectoryNames = @(),
    [string[]]$FileNames = @(),
    [string[]]$Extensions = @()
  )
  if (-not (Test-Path -LiteralPath $Base)) { return }
  foreach ($dirName in $DirectoryNames) {
    Get-ChildItem -LiteralPath $Base -Force -ErrorAction SilentlyContinue |
      Where-Object { $_.PSIsContainer -and $_.Name -ieq $dirName } |
      ForEach-Object { Remove-Item -LiteralPath $_.FullName -Recurse -Force -ErrorAction SilentlyContinue }
    Get-ChildItem -LiteralPath $Base -Recurse -Force -Directory -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -ieq $dirName } |
      ForEach-Object { Remove-Item -LiteralPath $_.FullName -Recurse -Force -ErrorAction SilentlyContinue }
  }
  foreach ($fileName in $FileNames) {
    Get-ChildItem -LiteralPath $Base -Force -ErrorAction SilentlyContinue |
      Where-Object { -not $_.PSIsContainer -and $_.Name -ieq $fileName } |
      ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }
    Get-ChildItem -LiteralPath $Base -Recurse -Force -File -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -ieq $fileName } |
      ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }
  }
  foreach ($ext in $Extensions) {
    Get-ChildItem -LiteralPath $Base -Recurse -Force -File -Filter "*$ext" -ErrorAction SilentlyContinue |
      ForEach-Object { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue }
  }
}

function Remove-PathPatterns {
  param(
    [string]$Base,
    [string[]]$Patterns
  )
  if (-not (Test-Path -LiteralPath $Base)) { return }
  foreach ($pattern in $Patterns) {
    Get-ChildItem -LiteralPath $Base -Recurse -Force -ErrorAction SilentlyContinue |
      Where-Object { $_.FullName -like $pattern } |
      Sort-Object FullName -Descending |
      ForEach-Object {
        try {
          if ($_.PSIsContainer) {
            Remove-Item -LiteralPath $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
          } else {
            Remove-Item -LiteralPath $_.FullName -Force -ErrorAction SilentlyContinue
          }
        } catch {}
      }
  }
}

function Assert-NotPresent {
  param(
    [string]$Base,
    [string[]]$DirectoryNames = @(),
    [string[]]$FileNames = @()
  )
  if (-not (Test-Path -LiteralPath $Base)) { return }
  foreach ($dirName in $DirectoryNames) {
    $matches = Get-ChildItem -LiteralPath $Base -Recurse -Force -Directory -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -ieq $dirName }
    if ($matches) {
      $paths = ($matches | Select-Object -ExpandProperty FullName) -join "; "
      throw "Unexpected directory remained in staged runtime: $paths"
    }
  }
  foreach ($fileName in $FileNames) {
    $matches = Get-ChildItem -LiteralPath $Base -Recurse -Force -File -ErrorAction SilentlyContinue |
      Where-Object { $_.Name -ieq $fileName }
    if ($matches) {
      $paths = ($matches | Select-Object -ExpandProperty FullName) -join "; "
      throw "Unexpected file remained in staged runtime: $paths"
    }
  }
}

function Compile-Entrypoint {
  param(
    [string]$SourcePath,
    [string]$OutputPath
  )
  if (-not (Test-Path -LiteralPath $PythonExe)) {
    throw "Python runtime not found: $PythonExe"
  }
  $script = @'
import importlib._bootstrap_external as be
import pathlib
import sys
import time

source_path = pathlib.Path(sys.argv[1])
output_path = pathlib.Path(sys.argv[2])
virtual_name = sys.argv[3]
output_path.parent.mkdir(parents=True, exist_ok=True)
source_bytes = source_path.read_bytes()
code = compile(source_bytes, virtual_name, 'exec')
data = be._code_to_timestamp_pyc(code, int(time.time()), len(source_bytes))
output_path.write_bytes(data)
'@
  & $PythonExe -X utf8 -c $script $SourcePath $OutputPath ([System.IO.Path]::GetFileName($SourcePath))
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to compile $SourcePath"
  }
}

Reset-Dir $StageRoot

$OllamaStage = Join-Path $StageRoot "ollama"
Copy-Tree (Join-Path $RepoRoot "ollama") $OllamaStage
Remove-TreeItems -Base $OllamaStage -DirectoryNames @("__pycache__") -Extensions @(".pyc", ".log")

$OpsStage = Join-Path $StageRoot "ops"
Copy-Files -Source (Join-Path $RepoRoot "ops") -Destination $OpsStage -FileNames @(
  "check-health.ps1",
  "common.ps1",
  "restart-all.ps1",
  "start-all.ps1",
  "start-frontend.ps1",
  "start-gateway.ps1",
  "start-miner.ps1",
  "start-ollama.ps1",
  "status.ps1",
  "stop-all.ps1",
  "stop-ollama.ps1",
  "uninstall-app.ps1"
)
Assert-NotPresent -Base $OpsStage -DirectoryNames @("logs", "runtime") -FileNames @("GO_LIVE_CHECKLIST.md")

$VenvStage = Join-Path $StageRoot ".venv"
Copy-Tree (Join-Path $RepoRoot ".venv") $VenvStage
Remove-TreeItems -Base $VenvStage -DirectoryNames @("__pycache__", "pip-cache") -Extensions @(".pyc")
Remove-TreeItems -Base (Join-Path $VenvStage "Lib\site-packages") -DirectoryNames @("tests", "testing", "test", "__pycache__", "pip", "setuptools", "wheel") -FileNames @(".gitignore", "CACHEDIR.TAG")
Remove-PathPatterns -Base (Join-Path $VenvStage "Lib\site-packages") -Patterns @(
  "*\\pip-*.dist-info",
  "*\\setuptools-*.dist-info",
  "*\\wheel-*.dist-info",
  "*\\*.dist-info\\licenses",
  "*\\*.dist-info\\RECORD",
  "*\\*.dist-info\\INSTALLER",
  "*\\*.dist-info\\REQUESTED",
  "*\\*.dist-info\\direct_url.json",
  "*\\pip-*.virtualenv"
)
Remove-TreeItems -Base (Join-Path $VenvStage "Scripts") -FileNames @(
  "activate",
  "activate.bat",
  "activate.fish",
  "activate.nu",
  "activate.ps1",
  "activate_this.py",
  "deactivate.bat",
  "dotenv.exe",
  "fastapi.exe",
  "httpx.exe",
  "markdown-it.exe",
  "normalizer.exe",
  "pip.exe",
  "pip3.exe",
  "pip-3.14.exe",
  "pip3.14.exe",
  "pydoc.bat",
  "pygmentize.exe",
  "pyiceberg.exe",
  "uvicorn.exe",
  "websockets.exe"
)
Remove-PathPatterns -Base (Join-Path $VenvStage "Lib\site-packages") -Patterns @(
  "*\\tests\\*",
  "*\\test\\*",
  "*\\testing\\*",
  "*\\docs\\*",
  "*\\doc\\*",
  "*\\examples\\*",
  "*\\demos\\*"
)

$FrontendStage = Join-Path $StageRoot "app-frontend"
Copy-Tree $FrontendSource $FrontendStage

$ConfigStage = Join-Path $StageRoot "config-templates"
Copy-Tree (Join-Path $DesktopRoot "build\config-templates") $ConfigStage

$GatewayOut = Join-Path $StageRoot "v12_gateway\api\index.pyc"
Compile-Entrypoint (Join-Path $RepoRoot "v12_gateway\api\index.py") $GatewayOut
# Defense-in-depth: ensure no plaintext runtime env files are present in staged gateway runtime.
Remove-TreeItems -Base (Join-Path $StageRoot "v12_gateway") -FileNames @(".env", "gateway.env", "miner.env")

$MinerOut = Join-Path $StageRoot "miner\heph.pyc"
Compile-Entrypoint $MinerSource $MinerOut

Write-Host "Prepared staged runtime at $StageRoot"

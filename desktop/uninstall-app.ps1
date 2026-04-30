Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$candidateScripts = @(
  (Join-Path (Split-Path -Parent $PSScriptRoot) "ops\uninstall-app.ps1"),
  (Join-Path $PSScriptRoot "resources\runtime\ops\uninstall-app.ps1"),
  (Join-Path $PSScriptRoot "runtime\ops\uninstall-app.ps1")
)

$script = $candidateScripts | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $script) {
  throw "Uninstall script not found."
}

& $script

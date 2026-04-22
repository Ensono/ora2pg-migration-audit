# =============================================================================
# publish.ps1 — Build the Row Count Validator self-contained binary (Windows)
#
# Usage (run in PowerShell as Administrator or with Script Execution enabled):
#   .\publish.ps1                   # defaults to win-x64
#   .\publish.ps1 -Rid win-x64      # Windows 64-bit
#   .\publish.ps1 -Rid linux-x64    # Linux binary (cross-compile)
#   .\publish.ps1 -Rid osx-arm64    # macOS Apple Silicon (cross-compile)
#
# Output:
#   .\output\<rid>\row-count-validator.exe   (or row-count-validator on Linux/macOS)
#   .\output\<rid>\.env
#   .\output\<rid>\README.md
#
# Requirements:
#   - .NET 9 SDK installed (https://dot.net)
# =============================================================================

param(
    [string]$Rid = "win-x64"
)

$ErrorActionPreference = "Stop"

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Definition
$RepoRoot   = Resolve-Path "$ScriptDir\..\.."
$Project    = "$RepoRoot\src\Ora2PgRowCountValidator\Ora2PgRowCountValidator.csproj"
$OutDir     = "$ScriptDir\output\$Rid"

Write-Host ""
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Row Count Validator — Publishing for: $Rid"      -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Clean output
if (Test-Path $OutDir) { Remove-Item -Recurse -Force $OutDir }
New-Item -ItemType Directory -Path $OutDir | Out-Null

# Publish
dotnet publish $Project `
    -c Release `
    -r $Rid `
    --self-contained true `
    -p:PublishSingleFile=true `
    -p:IncludeNativeLibrariesForSelfExtract=true `
    -p:EnableCompressionInSingleFile=true `
    -o $OutDir

# Remove dev artifacts that BAU doesn't need
Get-ChildItem "$OutDir\*.pdb" -ErrorAction SilentlyContinue | Remove-Item -Force
Get-ChildItem "$OutDir\*.xml" -ErrorAction SilentlyContinue | Remove-Item -Force

# Copy BAU support files
Copy-Item "$ScriptDir\.env.template" "$OutDir\.env"
Copy-Item "$ScriptDir\README.md"     "$OutDir\README.md"

# Rename executable to a friendly name
if ($Rid -like "win-*") {
    $src = Join-Path $OutDir "Ora2PgRowCountValidator.exe"
    $dst = Join-Path $OutDir "row-count-validator.exe"
} else {
    $src = Join-Path $OutDir "Ora2PgRowCountValidator"
    $dst = Join-Path $OutDir "row-count-validator"
}
if (Test-Path $src) { Rename-Item -Path $src -NewName (Split-Path $dst -Leaf) }

Write-Host ""
Write-Host "✅ Done! Package ready at: $OutDir" -ForegroundColor Green
Write-Host ""
Write-Host "Contents:" -ForegroundColor Yellow
Get-ChildItem $OutDir | Format-Table Name, Length, LastWriteTime
Write-Host ""
Write-Host "Zip for delivery:" -ForegroundColor Yellow
Write-Host "  Compress-Archive -Path '$OutDir\*' -DestinationPath '$ScriptDir\output\row-count-validator-$Rid.zip'"
Write-Host ""

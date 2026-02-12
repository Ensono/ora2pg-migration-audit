#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Build and run all Oracle to PostgreSQL migration validators in sequence.

.DESCRIPTION
    This script builds the solution and executes all five validation tools in the
    recommended order for comprehensive migration validation:
    1. SchemaComparer - Validate schema structure
    2. RowCountValidator - Validate row counts
    3. DataTypeValidator - Validate data type mappings
    4. DataValidator - Validate data integrity
    5. PerformanceValidator - Validate query performance

.PARAMETER RollForward
    Specifies the roll-forward policy for .NET runtime version. Valid values: Major, Minor, LatestPatch.
    Use this when your system has a higher version of .NET installed than the project targets.

.EXAMPLE
    .\run-all-validators.ps1
    
.EXAMPLE
    .\run-all-validators.ps1 -RollForward Major

.EXAMPLE
    .\run-all-validators.ps1 -RollForward Minor

.NOTES
    Requires: .NET 8.0 SDK or higher
    Configuration: Ensure .env file is configured at solution root
#>

Param(
    [Parameter(Mandatory=$false)]
    [ValidateSet('Major', 'Minor', 'LatestPatch')]
    [string]$RollForward
)

# Exit on error
$ErrorActionPreference = "Stop"

# Script configuration
$SolutionDir = $PSScriptRoot
$SolutionFile = Join-Path $SolutionDir "src/ora2pg-migration-audit.sln"

# Project paths
$Projects = @(
    @{
        Name = "SchemaComparer"
        Path = "src/Ora2PgSchemaComparer/Ora2PgSchemaComparer.csproj"
        Description = "Schema structure validation"
    },
    @{
        Name = "RowCountValidator"
        Path = "src/Ora2PgRowCountValidator/Ora2PgRowCountValidator.csproj"
        Description = "Row count validation"
    },
    @{
        Name = "DataTypeValidator"
        Path = "src/Ora2PgDataTypeValidator/Ora2PgDataTypeValidator.csproj"
        Description = "Data type mapping validation"
    },
    @{
        Name = "PerformanceValidator"
        Path = "src/Ora2PgPerformanceValidator/Ora2PgPerformanceValidator.csproj"
        Description = "Query performance validation"
    },
    @{
        Name = "DataValidator"
        Path = "src/Ora2PgDataValidator/Ora2PgDataValidator.csproj"
        Description = "Data integrity validation"
    }
)

# Color output functions
function Write-Step {
    param([string]$Message)
    Write-Host "`n═══════════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  $Message" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════════════`n" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ $Message" -ForegroundColor Yellow
}

# Main execution
try {
    Write-Host "`n╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
    Write-Host "║  Oracle to PostgreSQL Migration Validation Suite             ║" -ForegroundColor Magenta
    Write-Host "╚═══════════════════════════════════════════════════════════════╝`n" -ForegroundColor Magenta

    # Check for .env file
    $EnvFile = Join-Path $SolutionDir ".env"
    if (-not (Test-Path $EnvFile)) {
        Write-Error-Custom "Missing .env configuration file at solution root"
        Write-Info "Copy .env.example to .env and configure database credentials"
        exit 1
    }
    Write-Success "Found .env configuration file"

    # Check for .NET SDK
    Write-Step "Checking Prerequisites"
    try {
        $dotnetVersion = dotnet --version
        Write-Success ".NET SDK version: $dotnetVersion"
        
        if ($RollForward) {
            Write-Info "Roll-forward policy: $RollForward"
        }
    }
    catch {
        Write-Error-Custom ".NET SDK not found. Please install .NET 8.0 SDK or higher"
        exit 1
    }

    # Build solution
    Write-Step "Building Solution"
    Write-Host "Solution: $SolutionFile`n" -ForegroundColor Gray
    
    $buildOutput = dotnet build $SolutionFile --configuration Release 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Error-Custom "Build failed"
        Write-Host $buildOutput
        exit 1
    }
    Write-Success "Solution built successfully"

    # Run each validator in sequence
    $results = @()
    $totalStartTime = Get-Date

    foreach ($project in $Projects) {
        Write-Step "Running $($project.Name)"
        Write-Host "Description: $($project.Description)" -ForegroundColor Gray
        Write-Host "Project: $($project.Path)`n" -ForegroundColor Gray

        $projectPath = Join-Path $SolutionDir $project.Path
        $startTime = Get-Date

        try {
            # Build dotnet run command with optional roll-forward parameter
            $runArgs = @('run', '--project', $projectPath, '--configuration', 'Release', '--no-build')
            if ($RollForward) {
                $runArgs += @('--roll-forward', $RollForward)
            }
            
            dotnet @runArgs
            $endTime = Get-Date
            $duration = ($endTime - $startTime).TotalSeconds

            if ($LASTEXITCODE -eq 0) {
                Write-Success "$($project.Name) completed successfully (${duration}s)"
                $results += @{
                    Name = $project.Name
                    Status = "Success"
                    Duration = $duration
                }
            }
            else {
                Write-Error-Custom "$($project.Name) failed with exit code $LASTEXITCODE"
                $results += @{
                    Name = $project.Name
                    Status = "Failed"
                    Duration = $duration
                    ExitCode = $LASTEXITCODE
                }
            }
        }
        catch {
            $endTime = Get-Date
            $duration = ($endTime - $startTime).TotalSeconds
            Write-Error-Custom "$($project.Name) failed: $($_.Exception.Message)"
            $results += @{
                Name = $project.Name
                Status = "Error"
                Duration = $duration
                Error = $_.Exception.Message
            }
        }
    }

    # Summary report
    $totalEndTime = Get-Date
    $totalDuration = ($totalEndTime - $totalStartTime).TotalSeconds

    Write-Step "Validation Summary"
    Write-Host ("Total execution time: {0:N2}s`n" -f $totalDuration) -ForegroundColor Gray

    $successCount = 0
    $failCount = 0

    foreach ($result in $results) {
        $statusIcon = if ($result.Status -eq "Success") { "✓" } else { "✗" }
        $statusColor = if ($result.Status -eq "Success") { "Green" } else { "Red" }
        $durationStr = "{0,6:N2}s" -f $result.Duration

        Write-Host "$statusIcon " -ForegroundColor $statusColor -NoNewline
        Write-Host ("{0,-25}" -f $result.Name) -NoNewline
        Write-Host " $durationStr " -ForegroundColor Gray -NoNewline
        Write-Host $result.Status -ForegroundColor $statusColor

        if ($result.Status -eq "Success") {
            $successCount++
        }
        else {
            $failCount++
            if ($result.ExitCode) {
                Write-Host "    Exit Code: $($result.ExitCode)" -ForegroundColor Red
            }
            if ($result.Error) {
                Write-Host "    Error: $($result.Error)" -ForegroundColor Red
            }
        }
    }

    Write-Host "`nResults: $successCount passed, $failCount failed" -ForegroundColor $(if ($failCount -eq 0) { "Green" } else { "Yellow" })

    # Exit with appropriate code
    if ($failCount -gt 0) {
        Write-Host "`n⚠ Some validators failed. Review logs and reports for details.`n" -ForegroundColor Yellow
        exit 1
    }
    else {
        Write-Host "`n✓ All validators completed successfully!`n" -ForegroundColor Green
        exit 0
    }
}
catch {
    Write-Error-Custom "Fatal error: $($_.Exception.Message)"
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
    exit 1
}

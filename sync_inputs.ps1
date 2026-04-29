<#
.SYNOPSIS
    Sync FTP receipt files to local data/input for the Cash Reconciliation pipeline.

.DESCRIPTION
    Uses robocopy / native PowerShell for fast Windows SMB access.
    Run this BEFORE launching the Streamlit app each morning.

    Daily mode (default): copies only files not already present locally.
    Full mode (-Full):     copies all files within the lookback window.

.EXAMPLE
    .\sync_inputs.ps1           # daily incremental — run each morning
    .\sync_inputs.ps1 -Full     # initial setup or after a gap
#>

param(
    [switch]$Full,
    [int]$LookbackDays = 90     # calendar days; covers ~60 business days
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Destination root (WSL project data/input folder via UNC)
# ---------------------------------------------------------------------------
$dest_root = "\\wsl.localhost\Ubuntu\home\gloriazuo\Cash-Reconciliation_V2\data\input"

# ---------------------------------------------------------------------------
# Source definitions
#   src        : network source folder (Windows path)
#   pattern    : filename filter
#   multi_file : $true  = keep ALL files in lookback window (citi_hi)
#                $false = keep only the single latest file
# ---------------------------------------------------------------------------
$sources = [ordered]@{
    tradar         = @{ src = "W:\Ops\Controls\Cash\Tradar Reports";         pattern = "*Cash Flow since One Month ago - all funds.csv"; multi_file = $false }
    citi           = @{ src = "W:\Ops\Citi\Rec Files\Archive";               pattern = "DOD_CASH_BALANCES_*.csv";       multi_file = $false }
    citi_hi        = @{ src = "W:\Ops\Citi\Rec Files";                       pattern = "BSFFTMOUT_Positions_All_*.CSV"; multi_file = $true  }
    bnp            = @{ src = "W:\Ops\BNP\Rec Files";                        pattern = "*GPBCash*.csv";                 multi_file = $false }
    bnp_nz         = @{ src = "W:\Ops\BNP NZ\Rec Files\Recon cleaned";       pattern = "*Bal_cash*.csv";                multi_file = $true  }
    bnp_margin_pdf = @{ src = "W:\Ops\BNP\Rec Files";                        pattern = "*DAILY_STAT.pdf";               multi_file = $false }
}

$mode = if ($Full) { "FULL (last $LookbackDays calendar days)" } else { "DAILY (incremental)" }
Write-Host ""
Write-Host "RecX Input Sync  -  $mode" -ForegroundColor Cyan
Write-Host "Destination: $dest_root"
Write-Host (Get-Date -Format "yyyy-MM-dd HH:mm")
Write-Host ""

$cutoff = (Get-Date).AddDays(-$LookbackDays)
$total_copied = 0

foreach ($name in $sources.Keys) {
    $s = $sources[$name]
    $dest = Join-Path $dest_root $name

    Write-Host "[$name]" -ForegroundColor Yellow -NoNewline
    Write-Host "  $($s.src)"

    if (-not (Test-Path $s.src)) {
        Write-Host "  SKIP: source folder not found" -ForegroundColor Red
        Write-Host ""
        continue
    }

    if (-not (Test-Path $dest)) {
        New-Item -ItemType Directory -Path $dest -Force | Out-Null
    }

    if (-not $s.multi_file) {
        # ----------------------------------------------------------------
        # Single-file source: copy only the most recently modified file.
        # LastWriteTime is returned by Get-ChildItem at no extra cost on
        # Windows (no additional stat calls needed).
        # ----------------------------------------------------------------
        $files = Get-ChildItem -Path $s.src -Filter $s.pattern -File -ErrorAction SilentlyContinue |
                 Sort-Object LastWriteTime -Descending
        if ($files.Count -eq 0) {
            Write-Host "  WARN: no files matching '$($s.pattern)'" -ForegroundColor DarkYellow
            Write-Host ""
            continue
        }
        $latest    = $files[0]
        $dest_file = Join-Path $dest $latest.Name
        if (-not (Test-Path $dest_file)) {
            Copy-Item -Path $latest.FullName -Destination $dest_file
            Write-Host "  Copied : $($latest.Name)  ($($latest.LastWriteTime.ToString('yyyy-MM-dd HH:mm')))" -ForegroundColor Green
            $total_copied++
        } else {
            Write-Host "  Current: $($latest.Name)"
        }
    } else {
        # ----------------------------------------------------------------
        # Multi-file source (citi_hi): copy all files within the window
        # that are not already present locally.
        # ----------------------------------------------------------------
        $files = Get-ChildItem -Path $s.src -Filter $s.pattern -File -ErrorAction SilentlyContinue |
                 Where-Object { $_.LastWriteTime -ge $cutoff } |
                 Sort-Object LastWriteTime

        if ($files.Count -eq 0) {
            Write-Host "  WARN: no files within $LookbackDays-day window" -ForegroundColor DarkYellow
            Write-Host ""
            continue
        }

        $copied = 0
        foreach ($f in $files) {
            $dest_file = Join-Path $dest $f.Name
            if (-not (Test-Path $dest_file)) {
                Copy-Item -Path $f.FullName -Destination $dest_file
                Write-Host "  Copied : $($f.Name)" -ForegroundColor Green
                $copied++
            }
        }
        if ($copied -eq 0) {
            Write-Host "  Up to date ($($files.Count) files in window)"
        } else {
            $total_copied += $copied
        }
    }
    Write-Host ""
}

Write-Host "Done. $total_copied file(s) copied." -ForegroundColor Cyan
Write-Host ""

<#
.SYNOPSIS
    Registers each per-folder Linx consumer instance as its own NSSM service.

.DESCRIPTION
    Folder-per-instance model: each feed already lives in its own directory (e.g.
    C:\Codebase\TDCS) containing MqiptConsumer.exe and a hand-maintained
    appsettings.json. This script registers one NSSM service per folder, pointing
    at that folder's exe and using that folder's config.

    It NEVER touches appsettings.json - your per-folder config is left exactly as is.
    Optionally (-ExeSource) it refreshes just the exe in each folder before
    registering, so you can roll out a new build without disturbing config.

    By default it discovers every immediate subfolder of -Root that contains
    MqiptConsumer.exe; pass -Folders to target a specific set.

    Requires (for the service step): Administrator, and nssm.exe on PATH or via
    -NssmPath (https://nssm.cc/download).

.EXAMPLE
    # Register a service for every instance folder under C:\Codebase:
    .\Deploy-LinxConsumers.ps1 -Root C:\Codebase

.EXAMPLE
    # Roll out a new exe into each folder, then (re)register the services:
    .\Deploy-LinxConsumers.ps1 -Root C:\Codebase -ExeSource .\MqiptConsumer.exe

.EXAMPLE
    # Only specific folders:
    .\Deploy-LinxConsumers.ps1 -Root C:\Codebase -Folders TDCS,VSCS

.EXAMPLE
    # Remove the services (folders/configs untouched):
    .\Deploy-LinxConsumers.ps1 -Root C:\Codebase -Remove
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $Root,
    [string[]] $Folders,
    [string]   $ServicePrefix = 'LinxConsumer-',
    [string]   $NssmPath      = 'nssm',
    [string]   $ExeSource,
    [switch]   $Remove
)

$ErrorActionPreference = 'Stop'

function Test-Admin {
    $id = [Security.Principal.WindowsIdentity]::GetCurrent()
    return (New-Object Security.Principal.WindowsPrincipal($id)).IsInRole(
        [Security.Principal.WindowsBuiltInRole]::Administrator)
}
function Resolve-Nssm {
    $cmd = Get-Command $NssmPath -ErrorAction SilentlyContinue
    if (-not $cmd) { throw "nssm not found ('$NssmPath'). Install it (https://nssm.cc/download) or pass -NssmPath." }
    return $cmd.Source
}

if (-not (Test-Path $Root)) { throw "Root not found: $Root" }
if (-not (Test-Admin))      { throw 'This script must be run from an elevated (Administrator) PowerShell.' }
$nssm = Resolve-Nssm

# Resolve the target instance folders.
if ($Folders) {
    $dirs = $Folders | ForEach-Object { Join-Path $Root $_ }
} else {
    # Auto-discover: immediate subfolders that contain MqiptConsumer.exe.
    $dirs = Get-ChildItem -Path $Root -Directory |
        Where-Object { Test-Path (Join-Path $_.FullName 'MqiptConsumer.exe') } |
        Select-Object -ExpandProperty FullName
}
if (-not $dirs) { throw "No instance folders found under $Root (looked for MqiptConsumer.exe)." }

# ---- Remove mode -----------------------------------------------------------
if ($Remove) {
    foreach ($dir in $dirs) {
        $svc = "$ServicePrefix$(Split-Path $dir -Leaf)"
        if (Get-Service -Name $svc -ErrorAction SilentlyContinue) {
            Write-Host "Removing $svc ..."
            & $nssm stop   $svc confirm | Out-Null
            & $nssm remove $svc confirm | Out-Null
        } else {
            Write-Host "Skipping $svc (not installed)."
        }
    }
    Write-Host 'Done. Folders and configs were left untouched.'
    return
}

if ($ExeSource) {
    if (-not (Test-Path $ExeSource)) { throw "ExeSource not found: $ExeSource" }
    $ExeSource = (Resolve-Path $ExeSource).Path
    $pdbSource = [IO.Path]::ChangeExtension($ExeSource, '.pdb')
}

# ---- Register / update services --------------------------------------------
foreach ($dir in $dirs) {
    $name = Split-Path $dir -Leaf
    $svc  = "$ServicePrefix$name"
    $exe  = Join-Path $dir 'MqiptConsumer.exe'
    $logs = Join-Path $dir 'logs'

    if (-not (Test-Path (Join-Path $dir 'appsettings.json'))) {
        Write-Warning "$name has no appsettings.json - the consumer will fail to start until one is present."
    }

    # Optional: refresh the binary only (config untouched).
    if ($ExeSource) {
        Copy-Item $ExeSource $exe -Force
        if (Test-Path $pdbSource) { Copy-Item $pdbSource ([IO.Path]::ChangeExtension($exe, '.pdb')) -Force }
        Write-Host "Refreshed exe in $name"
    }
    if (-not (Test-Path $exe)) { throw "MqiptConsumer.exe not found in $dir (pass -ExeSource to stage it)." }
    New-Item -ItemType Directory -Force -Path $logs | Out-Null

    if (Get-Service -Name $svc -ErrorAction SilentlyContinue) {
        & $nssm stop $svc confirm | Out-Null
    } else {
        & $nssm install $svc $exe | Out-Null
    }

    & $nssm set $svc Application     $exe  | Out-Null
    & $nssm set $svc AppDirectory    $dir  | Out-Null
    & $nssm set $svc DisplayName     "Linx Consumer - $name" | Out-Null
    & $nssm set $svc Description     "MQ->Kinesis consumer instance ($dir)" | Out-Null
    & $nssm set $svc Start           SERVICE_AUTO_START | Out-Null
    & $nssm set $svc AppExit Default Restart | Out-Null
    & $nssm set $svc AppThrottle     10000   | Out-Null
    & $nssm set $svc AppRestartDelay 5000    | Out-Null
    & $nssm set $svc AppStdout       (Join-Path $logs 'out.log') | Out-Null
    & $nssm set $svc AppStderr       (Join-Path $logs 'err.log') | Out-Null
    & $nssm set $svc AppRotateFiles  1        | Out-Null
    & $nssm set $svc AppRotateOnline 1        | Out-Null
    & $nssm set $svc AppRotateBytes  10485760 | Out-Null
    & $nssm start $svc | Out-Null
    Write-Host "  -> $svc started (AppDirectory=$dir)"
}

Write-Host ''
Write-Host 'Done.'
Get-Service -Name "$ServicePrefix*" | Select-Object Name, Status, StartType | Format-Table -AutoSize

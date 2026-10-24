[CmdletBinding()]
param(
    [ValidateSet('intl', 'cn')]
    [string]$Edition = 'intl',
    [string]$TaskName = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not $TaskName) {
    $TaskName = if ($Edition -eq 'cn') { 'Qoder CN Chat Export Watcher' } else { 'Qoder Chat Export Watcher' }
}

$toolDir = Split-Path -Parent $PSCommandPath
$exporter = Join-Path $toolDir 'export_qoder_chats_live.py'
if (-not (Test-Path -LiteralPath $exporter -PathType Leaf)) {
    throw "Qoder chat exporter not found: $exporter"
}

$homeDir = [Environment]::GetFolderPath('UserProfile')
$pythonw = 'C:\Program Files\PyManager\pythonw.exe'
if (-not (Test-Path -LiteralPath $pythonw -PathType Leaf)) {
    throw "No-console Python launcher not found: $pythonw"
}

$qoderOutput = if ($Edition -eq 'cn') {
    Join-Path $homeDir 'qoder_cn_chat_live_exports'
} else {
    Join-Path $homeDir 'qoder_chat_live_exports'
}
$logPath = Join-Path $qoderOutput 'watcher.log'
$statusPath = Join-Path $qoderOutput 'watcher.status.json'

[void](New-Item -ItemType Directory -Path $qoderOutput -Force)

$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
$userName = $identity.Name
$actionArguments = '"{0}" --edition "{1}" --output-dir "{2}" --interval 30 --quiet --log-file "{3}" --status-file "{4}"' -f `
    $exporter, $Edition, $qoderOutput, $logPath, $statusPath
$action = New-ScheduledTaskAction -Execute $pythonw -Argument $actionArguments

$triggers = @(
    (New-ScheduledTaskTrigger -AtLogOn -User $userName),
    (New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
        -RepetitionInterval (New-TimeSpan -Minutes 5) `
        -RepetitionDuration (New-TimeSpan -Days 3650))
)

$principal = New-ScheduledTaskPrincipal -UserId $userName -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -Compatibility Win8

$editionLabel = if ($Edition -eq 'cn') { 'Qoder CN (com.qodercn.app.stable)' } else { 'Qoder international (com.qoder.app.stable)' }
$task = New-ScheduledTask -Action $action -Trigger $triggers -Principal $principal -Settings $settings `
    -Description "Continuously exports local $editionLabel chat sessions from %APPDATA%/main.sqlite to $qoderOutput every 30s; the five-minute trigger revives the watcher if it stops."

[void](Register-ScheduledTask -TaskName $TaskName -InputObject $task -Force)
Start-ScheduledTask -TaskName $TaskName

Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State, Author, Description
Write-Host "Output directory: $qoderOutput"
Write-Host "Log: $logPath"
Write-Host "Status: $statusPath"

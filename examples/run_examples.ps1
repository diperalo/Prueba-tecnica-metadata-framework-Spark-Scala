# Runs the statement example plus the 3 additional examples and verifies
# every output against its expected_*.json file.
# Usage: .\examples\run_examples.ps1
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Read-YesNo {
    param(
        [string]$Question,
        [bool]$DefaultYes = $true
    )

    # Do not block automated/non-interactive executions waiting for an answer.
    if ([Console]::IsInputRedirected) { return $false }

    $suffix = if ($DefaultYes) { "[S/n]" } else { "[s/N]" }
    try {
        $answer = Read-Host "$Question $suffix"
    } catch {
        return $false
    }

    if ([string]::IsNullOrWhiteSpace($answer)) { return $DefaultYes }
    return ($answer.Trim() -match '^(s|si|y|yes)$')
}

$script:SbtCommand = $null
$script:HadoopNativeVersions = @("3.3.6", "3.3.5")
function Find-Sbt {
    $fromPath = Get-Command sbt -ErrorAction SilentlyContinue
    if ($fromPath) { return $fromPath.Source }

    $candidates = @()
    if ($env:SBT_HOME) { $candidates += (Join-Path $env:SBT_HOME "bin\sbt.bat") }
    $candidates += "C:\Program Files\sbt\bin\sbt.bat"
    $candidates += "C:\Program Files (x86)\sbt\bin\sbt.bat"
    $candidates += "C:\ProgramData\chocolatey\bin\sbt.bat"
    if ($env:LOCALAPPDATA) { $candidates += (Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Links\sbt.bat") }
    $candidates += (Join-Path $HOME "scoop\apps\sbt\current\bin\sbt.bat")
    $candidates += (Join-Path $HOME "scoop\shims\sbt.cmd")

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) { return $candidate }
    }
    return $null
}

function Resolve-Sbt {
    $found = Find-Sbt
    if ($found) { $script:SbtCommand = $found; return }

    Write-Host ""
    Write-Host "No se ha encontrado sbt en el PATH ni en las ubicaciones habituales de instalacion."
    Write-Host "El sbt integrado de IntelliJ no instala el comando 'sbt' para PowerShell."

    $install = Read-YesNo "Quieres instalarlo ahora con 'winget install --id sbt.sbt -e'?"
    if (-not $install) {
        throw "Instalacion de sbt cancelada. Instalalo con 'winget install --id sbt.sbt -e', reabre la terminal, o ejecuta el proyecto desde IntelliJ (Run configurations / sbt tool window)."
    }

    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if (-not $winget) {
        throw "No se ha encontrado winget, por lo que no se puede instalar sbt automaticamente. Instala sbt desde https://www.scala-sbt.org/download.html y vuelve a ejecutar este script."
    }

    Write-Host "Instalando sbt con winget..."
    & $winget.Source install --id sbt.sbt -e --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -ne 0) { throw "winget no ha podido instalar sbt (exit code $LASTEXITCODE)." }

    # winget updates the persisted PATH, but this PowerShell process still has the old one.
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path = "$machinePath;$userPath"

    $found = Find-Sbt
    if ($found) {
        $script:SbtCommand = $found
        Write-Host "sbt instalado y localizado en: $found"
        return
    }

    throw "sbt se ha instalado, pero no se ha localizado sbt.bat en esta sesion. Cierra y reabre PowerShell y vuelve a ejecutar .\examples\run_examples.ps1."
}
Resolve-Sbt

function Ensure-HadoopNative {
    $hadoopHome = $env:HADOOP_HOME
    if ([string]::IsNullOrWhiteSpace($hadoopHome)) { $hadoopHome = "C:\hadoop" }
    $hadoopBin = Join-Path $hadoopHome "bin"
    $winutilsPath = Join-Path $hadoopBin "winutils.exe"
    $hadoopDllPath = Join-Path $hadoopBin "hadoop.dll"

    $hasWinutils = (Test-Path $winutilsPath) -and ((Get-Item $winutilsPath).Length -gt 0)
    $hasDll = (Test-Path $hadoopDllPath) -and ((Get-Item $hadoopDllPath).Length -gt 0)
    if ($hasWinutils -and $hasDll) {
        $env:HADOOP_HOME = $hadoopHome
        if ($env:Path -notlike "*$hadoopBin*") { $env:Path = "$hadoopBin;$env:Path" }
        return
    }

    Write-Host ""
    Write-Host "No se han encontrado los binarios nativos de Hadoop (winutils.exe y hadoop.dll, linea 3.3.x) en $hadoopBin."
    Write-Host "Sin ellos, Spark arranca en Windows pero falla con UnsatisfiedLinkError al acceder al sistema de ficheros local."

    $install = Read-YesNo "Quieres descargarlos y configurar HADOOP_HOME=$hadoopHome ahora?"
    if (-not $install) {
        throw "Configuracion de Hadoop cancelada. Descarga winutils.exe y hadoop.dll de un paquete winutils 3.3.x en $hadoopBin, define HADOOP_HOME=$hadoopHome y anade $hadoopBin al PATH antes de ejecutar Spark."
    }

    try {
        New-Item -ItemType Directory -Force $hadoopBin | Out-Null

        # Windows PowerShell 5.1 may need TLS 1.2 explicitly for GitHub downloads.
        try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 } catch { }

        # cdarlint/winutils does not publish a hadoop-3.3.4 folder (it jumps
        # from 3.2.2 to 3.3.5/3.3.6). Any 3.3.x patch build provides the same
        # NativeIO$Windows.access0 entry point used by Spark 3.5.1's Hadoop
        # 3.3.4 client, so try the newest 3.3.x first and fall back if needed.
        $downloadedVersion = $null
        foreach ($version in $script:HadoopNativeVersions) {
            try {
                Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-$version/bin/winutils.exe" -OutFile $winutilsPath -UseBasicParsing
                Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-$version/bin/hadoop.dll" -OutFile $hadoopDllPath -UseBasicParsing
                if (((Get-Item $winutilsPath).Length -gt 0) -and ((Get-Item $hadoopDllPath).Length -gt 0)) {
                    $downloadedVersion = $version
                    break
                }
            } catch {
                Write-Host "No se ha podido descargar el paquete winutils $version. Probando otra version..."
                Remove-Item $winutilsPath, $hadoopDllPath -ErrorAction SilentlyContinue
            }
        }
        if (-not $downloadedVersion) { throw "No se ha podido descargar ningun paquete winutils 3.3.x desde GitHub." }
        Write-Host "Paquete winutils descargado: Hadoop $downloadedVersion (misma linea 3.3.x que el cliente Hadoop de Spark 3.5.1)."

        [Environment]::SetEnvironmentVariable("HADOOP_HOME", $hadoopHome, "User")

        $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
        if ($userPath -notlike "*%HADOOP_HOME%\bin*") {
            [Environment]::SetEnvironmentVariable("Path", "$userPath;%HADOOP_HOME%\bin", "User")
        }

        $env:HADOOP_HOME = $hadoopHome
        $env:Path = "$env:HADOOP_HOME\bin;$env:Path"
    } catch {
        throw "No se ha podido instalar/configurar Hadoop nativo en $hadoopBin. Comprueba la conexion y los permisos sobre esa carpeta (si no puedes escribir en C:\, ejecuta PowerShell como administrador) o configura HADOOP_HOME manualmente. Error original: $($_.Exception.Message)"
    }

    $hasWinutils = (Test-Path $winutilsPath) -and ((Get-Item $winutilsPath).Length -gt 0)
    $hasDll = (Test-Path $hadoopDllPath) -and ((Get-Item $hadoopDllPath).Length -gt 0)
    Write-Host "winutils.exe existe: $hasWinutils"
    Write-Host "hadoop.dll existe: $hasDll"
    if (-not ($hasWinutils -and $hasDll)) {
        throw "La descarga de Hadoop nativo no ha dejado winutils.exe y hadoop.dll en $hadoopBin."
    }

    Write-Host "Comprobando winutils.exe..."
    & $winutilsPath | Out-Host
    # winutils without arguments prints its usage; the file checks above are the real validation.
}
Ensure-HadoopNative

$metadataFiles = @(
    "config/metadata.json",
    "examples/01-validaciones-multiples/metadata.json",
    "examples/02-ordenes-csv/metadata.json",
    "examples/03-transformaciones-genericas/metadata.json"
)

foreach ($metadata in $metadataFiles) {
    Write-Host "=== Running $metadata"
    & $script:SbtCommand -batch "run $metadata"
    if ($LASTEXITCODE -ne 0) { throw "sbt failed for $metadata (exit code $LASTEXITCODE)" }
}

Write-Host ""
Write-Host "=== Verifying outputs against the expected files (ExamplesSpec)"
& $script:SbtCommand -batch "testOnly com.sdg.framework.ExamplesSpec"
if ($LASTEXITCODE -ne 0) { throw "Output verification failed (exit code $LASTEXITCODE)" }

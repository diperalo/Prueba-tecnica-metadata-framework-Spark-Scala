# Guided commit & push assistant.
# Usage: .\git-commit.ps1
#
# What it does:
#   1. Shows the pending changes (new / modified / deleted / renamed).
#   2. Proposes a commit message built from those changes; you can
#      accept it (Enter), edit it, or cancel.
#   3. Asks whether to push. If you say yes, it first fetches the
#      remote, integrates remote changes (rebase) when the local
#      branch has fallen behind, and only then pushes.
# It NEVER force-pushes: if integrating remote changes produces a
# conflict, it stops, names the conflicted files and pushes nothing.
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"

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

function Read-Text {
    param([string]$Prompt)
    if ([Console]::IsInputRedirected) { return "" }
    try {
        return (Read-Host $Prompt).Trim()
    } catch {
        return ""
    }
}

# ---------- Git available? ----------

$script:GitExe = $null
function Resolve-Git {
    $cmd = Get-Command git -ErrorAction SilentlyContinue
    if ($cmd) { $script:GitExe = $cmd.Source; return $true }

    Write-Host "No se ha encontrado el comando 'git' en el PATH: Git no esta instalado (o no esta en el PATH)."
    $install = Read-YesNo "Quieres instalar Git para Windows ahora con 'winget install --id Git.Git -e'?"
    if (-not $install) {
        Write-Host "Instalacion cancelada. Instala Git para Windows desde https://git-scm.com/download/win y vuelve a ejecutar este script."
        return $false
    }
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if (-not $winget) {
        Write-Host "No se ha encontrado winget para instalar Git automaticamente. Instalalo desde https://git-scm.com/download/win y vuelve a ejecutar este script."
        return $false
    }
    Write-Host "Instalando Git para Windows con winget..."
    & $winget.Source install --id Git.Git -e --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -ne 0) {
        Write-Host "winget no ha podido instalar Git (exit code $LASTEXITCODE). Instalalo desde https://git-scm.com/download/win."
        return $false
    }
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path = "$machinePath;$userPath"
    $cmd = Get-Command git -ErrorAction SilentlyContinue
    if ($cmd) { $script:GitExe = $cmd.Source; return $true }
    Write-Host "Git se ha instalado, pero no se ha localizado 'git' en esta sesion. Reabre PowerShell y vuelve a ejecutar este script."
    return $false
}

if (-not (Resolve-Git)) { return }
$git = $script:GitExe

# Runs git discarding stderr without letting PowerShell 5.1 turn a
# non-zero exit into a terminating error; the caller checks $LASTEXITCODE.
function Invoke-GitSilent {
    param([string[]]$ArgsList)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try { & $git @ArgsList 2>$null } finally { $ErrorActionPreference = $prev }
}

# ---------- Inside a repository? ----------

$repoTop = Invoke-GitSilent @("-C", $PSScriptRoot, "rev-parse", "--show-toplevel")
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($repoTop)) {
    Write-Host "Este directorio no esta dentro de un repositorio Git."
    Write-Host "Si el proyecto ya existe en GitHub/GitLab, clona el repositorio y trabaja dentro del clon."
    Write-Host "Si es un proyecto local nuevo, inicializalo con 'git init' (el instalador .\doc\hooks\install-hooks.ps1 te guia en ambos casos)."
    return
}
Set-Location $repoTop

$branch = & $git branch --show-current
if ([string]::IsNullOrWhiteSpace($branch)) {
    Write-Host "Estas en modo 'detached HEAD' (sin rama). Cambia a una rama (git switch -c <nombre>) antes de usar este asistente."
    return
}
Write-Host "Repositorio: $repoTop"
Write-Host "Rama actual: $branch"

# ---------- Collect changes ----------

function Get-ChangeCategory {
    param([string]$Path)
    $p = $Path.Replace("\", "/")
    if ($p -match '(^|/)(src/test|tests)/' -or $p -match 'Spec\.scala$' -or $p -match '(^|/)test_.*\.py$') { return "los tests" }
    if ($p -match '^src/') { return "el codigo" }
    if ($p -match '^examples/') { return "los ejemplos" }
    if ($p -match '^doc/') { return "la documentacion" }
    if ($p -match '^config/') { return "la configuracion" }
    if ($p -match '^data/') { return "los datos de ejemplo" }
    if ($p -match '^(project/|build\.sbt$|pyproject\.toml$|requirements.*\.txt$)') { return "la configuracion de build" }
    if ($p -match '\.md$' -or $p -match '^(\.run/|\.gitignore$|\.scalafmt\.conf$|\.editorconfig$)' -or $p -match '\.(ps1|cmd)$') { return "la documentacion y herramientas" }
    return "el proyecto"
}

$statusLines = & $git status --porcelain
$changes = @()
foreach ($line in $statusLines) {
    if ($line.Length -lt 4) { continue }
    $code = $line.Substring(0, 2)
    $path = $line.Substring(3).Trim().Trim('"')
    if ($path -match ' -> ') { $path = ($path -split ' -> ')[-1].Trim().Trim('"') }
    $type = "Modificado"
    if ($code -eq "??") { $type = "Nuevo" }
    elseif ($code -match 'D') { $type = "Eliminado" }
    elseif ($code -match 'A') { $type = "Nuevo" }
    elseif ($code -match 'R') { $type = "Renombrado" }
    $changes += [PSCustomObject]@{ Type = $type; Path = $path }
}

$committedNow = $false
if ($changes.Count -eq 0) {
    Write-Host ""
    Write-Host "No hay cambios pendientes de commit (el arbol de trabajo esta limpio)."
} else {
    Write-Host ""
    Write-Host "Cambios detectados ($($changes.Count)):"
    foreach ($group in @("Nuevo", "Modificado", "Eliminado", "Renombrado")) {
        $items = $changes | Where-Object { $_.Type -eq $group }
        if ($items) {
            Write-Host "  $group`:"
            $items | ForEach-Object { Write-Host "    $($_.Path)" }
        }
    }

    # ----- Propose a commit message -----
    $countOf = { param($t) @($changes | Where-Object { $_.Type -eq $t }).Count }
    $nNew = & $countOf "Nuevo"; $nMod = & $countOf "Modificado"
    $nDel = & $countOf "Eliminado"; $nRen = & $countOf "Renombrado"

    if ($changes.Count -eq 1) {
        $only = $changes[0]
        $verb = switch ($only.Type) { "Nuevo" { "Anade" } "Eliminado" { "Elimina" } "Renombrado" { "Renombra" } default { "Actualiza" } }
        $subject = "$verb $($only.Path)"
    } else {
        $categories = @()
        foreach ($c in $changes) {
            $cat = Get-ChangeCategory $c.Path
            if ($categories -notcontains $cat) { $categories += $cat }
        }
        $shown = $categories | Select-Object -First 3
        $catText = $shown -join ", "
        if ($categories.Count -gt $shown.Count) { $catText += " y otros" }
        elseif ($shown.Count -gt 1) {
            $catText = ($shown[0..($shown.Count - 2)] -join ", ") + " y " + $shown[-1]
        }
        $details = @()
        if ($nNew -gt 0) { $details += $(if ($nNew -eq 1) { "1 nuevo" } else { "$nNew nuevos" }) }
        if ($nMod -gt 0) { $details += $(if ($nMod -eq 1) { "1 modificado" } else { "$nMod modificados" }) }
        if ($nDel -gt 0) { $details += $(if ($nDel -eq 1) { "1 eliminado" } else { "$nDel eliminados" }) }
        if ($nRen -gt 0) { $details += $(if ($nRen -eq 1) { "1 renombrado" } else { "$nRen renombrados" }) }
        $subject = "Actualiza $catText ($($changes.Count) ficheros: $($details -join ', '))"
    }

    $bodyLines = $changes | Select-Object -First 15 | ForEach-Object { "- ($($_.Type.ToLower())) $($_.Path)" }
    if ($changes.Count -gt 15) { $bodyLines += "- ... y $($changes.Count - 15) mas" }
    $body = $bodyLines -join "`n"

    Write-Host ""
    Write-Host "Mensaje de commit propuesto:"
    Write-Host "  $subject"
    $choice = Read-Text "Pulsa Enter para aceptarlo, escribe E para editarlo o N para cancelar"
    if ($choice -match '^(n|no)$') {
        Write-Host "Commit cancelado. No se ha hecho ningun cambio."
        return
    }
    if ($choice -match '^(e|editar)$') {
        $edited = Read-Text "Escribe el mensaje de commit"
        if ([string]::IsNullOrWhiteSpace($edited)) {
            Write-Host "Mensaje vacio: se mantiene el propuesto."
        } else {
            $subject = $edited
            $body = ""
        }
    }

    # ----- Git identity (needed to commit) -----
    $userName = (& $git config user.name) -join ""
    if ([string]::IsNullOrWhiteSpace($userName)) {
        Write-Host "Git necesita una identidad para firmar commits (user.name y user.email)."
        $name = Read-Text "Tu nombre (Enter para cancelar)"
        $email = Read-Text "Tu email (Enter para cancelar)"
        if (-not ($name -and $email)) {
            Write-Host "Commit cancelado: falta la identidad de Git."
            return
        }
        & $git config user.name $name
        & $git config user.email $email
    }

    # ----- Stage and confirm -----
    & $git add -A
    Write-Host ""
    Write-Host "Resumen de lo que entrara en el commit:"
    & $git diff --cached --stat
    if (-not (Read-YesNo "Confirmas el commit con el mensaje '$subject'?")) {
        Write-Host "Commit cancelado. Los cambios quedan preparados en el area de staging (git status para verlos)."
        return
    }

    if ($body) { & $git commit -m $subject -m $body } else { & $git commit -m $subject }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "git commit ha fallado (exit code $LASTEXITCODE). Revisa la salida anterior (el hook pre-commit o la identidad pueden ser la causa)."
        return
    }
    $committedNow = $true
    $hash = & $git rev-parse --short HEAD
    Write-Host "Commit creado: $hash - $subject"
}

# ---------- Push? ----------

$remotes = (& $git remote) -join "`n"
$hasOrigin = $remotes -match '(?m)^origin$'
if (-not $hasOrigin) {
    Write-Host ""
    Write-Host "Este repositorio no tiene remoto 'origin': no hay nube a la que subir."
    $url = Read-Text "Si ya tienes un repositorio en GitHub/GitLab, pega su URL (Enter para terminar sin remoto)"
    if ([string]::IsNullOrWhiteSpace($url)) {
        Write-Host "Sin remoto. Cuando lo tengas: git remote add origin <URL> y vuelve a ejecutar este asistente."
        return
    }
    & $git remote add origin $url
    if ($LASTEXITCODE -ne 0) { Write-Host "No se ha podido anadir el remoto."; return }
    $hasOrigin = $true
    Write-Host "Remoto origin anadido: $url"
}

# How far ahead/behind are we, before asking?
Invoke-GitSilent @("fetch", "--prune", "origin")
$upstreamRef = $null
$up = Invoke-GitSilent @("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
if ($LASTEXITCODE -eq 0 -and $up) {
    $upstreamRef = ($up | Select-Object -First 1)
} else {
    Invoke-GitSilent @("show-ref", "--verify", "--quiet", "refs/remotes/origin/$branch")
    if ($LASTEXITCODE -eq 0) { $upstreamRef = "origin/$branch" }
}

$ahead = 0; $behind = 0
if ($upstreamRef) {
    $counts = (& $git rev-list --left-right --count "HEAD...$upstreamRef") -split '\s+'
    $ahead = [int]$counts[0]; $behind = [int]$counts[1]
}

if (-not $committedNow -and $ahead -eq 0) {
    Write-Host ""
    if ($behind -gt 0) {
        Write-Host "No hay nada que subir, pero el remoto tiene $behind commit(s) nuevos."
        if (Read-YesNo "Quieres traerlos ahora (git pull --rebase)?") {
            & $git pull --rebase origin $branch
            if ($LASTEXITCODE -ne 0) {
                Write-Host "El pull ha tenido un problema (posible conflicto). Resuelvelo con git status o deshazlo con: git rebase --abort"
            } else {
                Write-Host "Rama actualizada con lo remoto."
            }
        }
    } else {
        Write-Host "Todo sincronizado: no hay commits que subir ni que traer."
    }
    return
}

Write-Host ""
Write-Host "Estado frente al remoto: $ahead commit(s) por subir, $behind commit(s) remotos por integrar."
if (-not (Read-YesNo "Quieres subirlo ahora (push)?")) {
    Write-Host "Sin push. Tu commit queda en local; subelo cuando quieras con: git push"
    return
}

# ---------- Integrate remote changes BEFORE pushing (never force) ----------

& $git fetch --prune origin
if ($LASTEXITCODE -ne 0) {
    Write-Host "git fetch ha fallado: sin acceso al remoto no se sube nada. Comprueba la red y tus credenciales."
    return
}

$upstreamRef = $null
$up = Invoke-GitSilent @("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
if ($LASTEXITCODE -eq 0 -and $up) {
    $upstreamRef = ($up | Select-Object -First 1)
} else {
    Invoke-GitSilent @("show-ref", "--verify", "--quiet", "refs/remotes/origin/$branch")
    if ($LASTEXITCODE -eq 0) { $upstreamRef = "origin/$branch" }
}

$ahead = 0; $behind = 0
if ($upstreamRef) {
    $counts = (& $git rev-list --left-right --count "HEAD...$upstreamRef") -split '\s+'
    $ahead = [int]$counts[0]; $behind = [int]$counts[1]
}

if ($behind -gt 0) {
    $dirty = & $git status --porcelain
    if ($dirty) {
        Write-Host "Hay cambios sin confirmar y el remoto tiene novedades: no se integra nada automaticamente."
        Write-Host "Confirma o aparta (git stash) esos cambios y vuelve a ejecutar este asistente."
        return
    }
    Write-Host "El remoto tiene $behind commit(s) nuevos: integrando antes de subir (git pull --rebase)..."
    & $git pull --rebase origin $branch
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "La integracion ha encontrado CONFLICTOS. No se ha subido nada."
        $conflicted = & $git diff --name-only --diff-filter=U
        if ($conflicted) {
            Write-Host "Ficheros en conflicto:"
            $conflicted | ForEach-Object { Write-Host "  $_" }
        }
        if (Read-YesNo "Quieres deshacer la integracion y dejar tu rama como estaba (git rebase --abort)?") {
            & $git rebase --abort
            Write-Host "Integracion deshecha: tu rama queda como antes. Tus commits estan a salvo en local."
            Write-Host "Para resolverlo a mano: git pull --rebase, edita los ficheros, git add <ficheros> y git rebase --continue; despues, git push."
        } else {
            Write-Host "Rebase en curso: resuelve los ficheros, haz git add <ficheros> y git rebase --continue; despues ejecuta git push (o este asistente otra vez)."
        }
        return
    }
    Write-Host "Integracion limpia: tu rama ya incluye lo remoto."
    $counts = (& $git rev-list --left-right --count "HEAD...$upstreamRef") -split '\s+'
    $ahead = [int]$counts[0]; $behind = [int]$counts[1]
}

if ($ahead -eq 0 -and $upstreamRef) {
    Write-Host "Nada que subir: tu rama ya coincide con el remoto."
    return
}

Write-Host ""
Write-Host "Subiendo a origin/$branch (si hay hook pre-push, se ejecutara ahora y puede tardar)..."
if ($upstreamRef) {
    & $git push
} else {
    & $git push -u origin $branch
}
if ($LASTEXITCODE -ne 0) {
    Write-Host "El push no se ha completado. Causas tipicas: credenciales, que el remoto haya cambiado mientras tanto (repite el asistente) o el hook pre-push."
    Write-Host "Este asistente nunca fuerza la subida: no se ha sobrescrito nada remoto."
    return
}

Write-Host ""
Write-Host "Subida completada."
& $git status -sb
& $git log -1 --oneline

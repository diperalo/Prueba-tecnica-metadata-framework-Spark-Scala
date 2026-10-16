# Installs the pre-push git hook (doc/hooks/pre-push) into .git/hooks.
# Usage: .\doc\hooks\install-hooks.ps1
# On Windows, git executes hooks with the Git Bash shell bundled with
# Git for Windows, so the hook itself stays as the POSIX sh version.
#
# Guided scenarios covered by this installer:
#   A. Git is not installed            -> offers to install it with winget.
#   B. No .git because the project already lives in GitHub/GitLab
#                                      -> offers to clone it and install
#                                         the hook in the cloned copy.
#   C. No .git because the project is local and not uploaded yet
#                                      -> offers 'git init', an optional
#                                         first commit and optional
#                                         remote link + first push.
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$hooksDir = Join-Path $repoRoot ".git\hooks"
$sourceHook = Join-Path $repoRoot "doc\hooks\pre-push"

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

# ---------- Scenario A: is Git installed? ----------

$script:GitExe = $null
function Resolve-Git {
    $cmd = Get-Command git -ErrorAction SilentlyContinue
    if ($cmd) { $script:GitExe = $cmd.Source; return $true }

    Write-Host ""
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
        Write-Host "winget no ha podido instalar Git (exit code $LASTEXITCODE). Instalalo desde https://git-scm.com/download/win y vuelve a ejecutar este script."
        return $false
    }

    # winget updates the persisted PATH, but this PowerShell process still has the old one.
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path = "$machinePath;$userPath"

    $cmd = Get-Command git -ErrorAction SilentlyContinue
    if ($cmd) {
        $script:GitExe = $cmd.Source
        Write-Host "Git instalado y localizado en: $($cmd.Source)"
        return $true
    }

    Write-Host "Git se ha instalado, pero no se ha localizado 'git' en esta sesion. Cierra y reabre PowerShell y vuelve a ejecutar este script."
    return $false
}

function Install-Hook {
    param(
        [string]$TargetHooksDir,
        [string]$PreferredSource
    )

    $src = if ($PreferredSource -and (Test-Path $PreferredSource)) { $PreferredSource } else { $sourceHook }
    $targetHook = Join-Path $TargetHooksDir "pre-push"
    if ((Test-Path $targetHook) -and ((Get-FileHash $src).Hash -eq (Get-FileHash $targetHook).Hash)) {
        Write-Host "El hook pre-push ya estaba instalado y actualizado en $targetHook."
        return
    }
    Copy-Item $src $targetHook -Force
    Write-Host "pre-push hook instalado en $targetHook (ejecuta: sbt prep)."
}

# ---------- Scenarios B and C: there is no .git folder ----------

if (-not (Test-Path $hooksDir)) {
    Write-Host ""
    Write-Host "Este directorio no es un repositorio Git (no existe la carpeta .git)."
    Write-Host "Puede pasar por dos motivos:"
    Write-Host "  [1] El proyecto ya existe en GitHub/GitLab y esta copia viene de un ZIP -> lo suyo es CLONARLO."
    Write-Host "  [2] El proyecto es local y aun no se ha subido a ningun sitio -> lo suyo es inicializar Git aqui (git init)."
    Write-Host "  [3] Salir y hacerlo manualmente."

    if (-not (Resolve-Git)) {
        Write-Host ""
        Write-Host "Sin Git no se puede instalar el hook. Cuando lo tengas, vuelve a ejecutar: .\doc\hooks\install-hooks.ps1"
        return
    }
    $git = $script:GitExe

    $option = Read-Text "Elige una opcion [1/2/3]"
    switch ($option) {
        "1" {
            # ----- Scenario B: clone the remote repository -----
            $url = Read-Text "URL del repositorio en GitHub/GitLab (HTTPS o SSH)"
            if ([string]::IsNullOrWhiteSpace($url)) {
                Write-Host "No se ha indicado URL. Saliendo sin clonar nada."
                return
            }
            $repoName = [System.IO.Path]::GetFileNameWithoutExtension($url.TrimEnd('/').Replace(':', '/'))
            $target = Join-Path (Split-Path $repoRoot -Parent) $repoName
            if (Test-Path $target) {
                Write-Host "Ya existe la carpeta $target. Clona manualmente en otra ubicacion y ejecuta el instalador dentro del clon:"
                Write-Host "  git clone $url"
                Write-Host "  cd $repoName"
                Write-Host "  .\doc\hooks\install-hooks.ps1"
                return
            }
            Write-Host "Clonando $url en $target ..."
            & $git clone $url $target
            if ($LASTEXITCODE -ne 0) {
                Write-Host "git clone ha fallado (exit code $LASTEXITCODE). Comprueba la URL, tus credenciales y que tienes acceso al repositorio."
                return
            }
            Install-Hook (Join-Path $target ".git\hooks") (Join-Path $target "doc\hooks\pre-push")
            Write-Host ""
            Write-Host "Listo: tu copia de trabajo es ahora $target (con el hook instalado)."
            Write-Host "Esta carpeta descomprimida del ZIP queda intacta; si tenias cambios en ella, copialos a la carpeta clonada."
            return
        }
        "2" {
            # ----- Scenario C: initialize a new repository here -----
            & $git -C $repoRoot init
            if ($LASTEXITCODE -ne 0) { throw "'git init' ha fallado (exit code $LASTEXITCODE)." }
            if (-not (Test-Path $hooksDir)) { throw "Tras 'git init' sigue sin existir $hooksDir; no se puede instalar el hook." }
            Write-Host "Repositorio Git inicializado en $repoRoot."
            Install-Hook $hooksDir $sourceHook

            $doCommit = Read-YesNo "Quieres hacer ahora el primer commit (git add . + git commit)?" $false
            if ($doCommit) {
                $userName = (& $git -C $repoRoot config user.name) -join ""
                if ([string]::IsNullOrWhiteSpace($userName)) {
                    Write-Host "Git necesita una identidad para firmar commits (user.name y user.email)."
                    $name = Read-Text "Tu nombre (Enter para omitir el commit)"
                    $email = Read-Text "Tu email (Enter para omitir el commit)"
                    if ($name -and $email) {
                        & $git -C $repoRoot config user.name $name
                        & $git -C $repoRoot config user.email $email
                        Write-Host "Identidad guardada en la configuracion local de este repositorio."
                    }
                }
                $userName = (& $git -C $repoRoot config user.name) -join ""
                if ([string]::IsNullOrWhiteSpace($userName)) {
                    Write-Host "Sin identidad configurada no se hace el commit. Puedes hacerlo cuando quieras con:"
                    Write-Host "  git config user.name `"Tu Nombre`""
                    Write-Host "  git config user.email `"tu[at]email.com`""
                    Write-Host "  git add . ; git commit -m `"Version inicial`""
                } else {
                    & $git -C $repoRoot add .
                    & $git -C $repoRoot commit -m "Framework Spark dirigido por metadatos: version inicial"
                    if ($LASTEXITCODE -ne 0) {
                        Write-Host "El commit no se ha completado (puede que no hubiera cambios). Revisa 'git status'."
                    } else {
                        Write-Host "Primer commit creado."
                        # Optional: link an (empty) GitHub/GitLab repository and push
                        Write-Host ""
                        Write-Host "Si ya has creado un repositorio VACIO en GitHub/GitLab (sin README ni licencia),"
                        Write-Host "puedes vincularlo y hacer el primer push ahora. El push disparara el hook (sbt prep),"
                        Write-Host "asi que puede tardar; si el hook fallara, podrias repetirlo con 'git push --no-verify'."
                        $remoteUrl = Read-Text "URL del repositorio remoto (Enter para hacerlo mas tarde)"
                        if ($remoteUrl) {
                            $remotes = (& $git -C $repoRoot remote) -join "`n"
                            if ($remotes -match '(?m)^origin$') {
                                & $git -C $repoRoot remote set-url origin $remoteUrl
                            } else {
                                & $git -C $repoRoot remote add origin $remoteUrl
                            }
                            & $git -C $repoRoot branch -M main
                            & $git -C $repoRoot push -u origin main
                            if ($LASTEXITCODE -ne 0) {
                                Write-Host "El push no se ha completado. Causas tipicas: credenciales, repositorio remoto no vacio o el propio hook (sbt prep)."
                                Write-Host "Si el error es '.git/hooks/pre-push: sbt: not found', Git Bash no localiza sbt: actualiza el hook reejecutando este instalador o empuja una vez con 'git push --no-verify'."
                                Write-Host "Tu repositorio local queda creado y vinculado; intentalo con 'git push -u origin main' cuando lo resuelvas."
                            } else {
                                Write-Host "Repositorio subido y vinculado: los proximos 'git push' iran a origin/main ejecutando antes el hook."
                            }
                        } else {
                            Write-Host "Sin remoto por ahora. Cuando crees el repositorio vacio en GitHub/GitLab:"
                            Write-Host "  git remote add origin <URL>"
                            Write-Host "  git branch -M main"
                            Write-Host "  git push -u origin main"
                        }
                    }
                }
            } else {
                Write-Host "Sin commit por ahora. Cuando quieras: git add .  y  git commit -m `"Version inicial`"."
            }
            return
        }
        default {
            Write-Host "Saliendo sin hacer cambios. Resumen manual:"
            Write-Host "  - Si el proyecto ya esta en GitHub/GitLab:  git clone <URL>  y ejecuta este instalador dentro del clon."
            Write-Host "  - Si es un proyecto local nuevo:  git init  y vuelve a ejecutar este instalador."
            return
        }
    }
}

# ---------- Already a repository: install/update the hook ----------

if (-not (Resolve-Git)) {
    Write-Host "Aviso: sin Git instalado, el hook queda copiado pero no se ejecutara hasta que instales Git."
}
Install-Hook $hooksDir $sourceHook
Write-Host "A partir de ahora, cada 'git push' ejecutara antes 'sbt prep' (compilar + formato + tests compilados)."
Write-Host "Si el hook fallara por el entorno (por ejemplo, sbt no localizado), un push puntual puede saltarlo con: git push --no-verify"

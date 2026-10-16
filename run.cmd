@echo off
rem Launcher for run.ps1 that does not depend on the machine execution policy.
rem Use it when opening PowerShell outside IntelliJ if .\run.ps1 is blocked
rem with "scripts are disabled on this system" (PSSecurityException).
setlocal
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run.ps1" %*
exit /b %errorlevel%

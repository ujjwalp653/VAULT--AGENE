@echo off
setlocal
cd /d %~dp0
set "NODE_ID=%~1"
set "NODE_PORT=%~2"
if "%NODE_ID%"=="" exit /b 1
if "%NODE_PORT%"=="" set "NODE_PORT=81%NODE_ID%"
start "VAULT Node %NODE_ID%" cmd /k "cd /d %~dp0backend && python -m vault.node_agent --id %NODE_ID% --port %NODE_PORT% --host 0.0.0.0 --storage .\data"

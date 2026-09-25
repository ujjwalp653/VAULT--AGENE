@echo off
setlocal
cd /d %~dp0
powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $h=Invoke-RestMethod http://127.0.0.1:8000/api/health -TimeoutSec 2; $c=Invoke-RestMethod http://127.0.0.1:8000/api/cluster -TimeoutSec 2; Write-Host ('Coordinator: OK'); Write-Host ('Nodes: ' + $c.summary.nodes + ' | Healthy: ' + $c.summary.healthy_nodes); Write-Host ('Dashboard: http://127.0.0.1:8000/'); exit 0 } catch { Write-Host ('VAULT NOT READY: ' + $_.Exception.Message); exit 1 }"
pause

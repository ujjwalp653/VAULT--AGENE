@echo off
setlocal EnableExtensions
cd /d %~dp0

echo ===============================================
echo            VAULT LOCAL CLUSTER
echo ===============================================
echo.
echo Starting one-click local mode...
echo The coordinator will automatically start all storage nodes.
echo.

set NODE_COUNT=8
set NODE_START_PORT=8101
set NODE_BASE_URL=http://127.0.0.1
set DATA_ROOT=%~dp0data
set DB_PATH=%~dp0data\vault.db
set AUTO_START_NODES=1
set NODE_BIND_HOST=127.0.0.1

if not exist "%DATA_ROOT%" mkdir "%DATA_ROOT%"

start "VAULT Coordinator" cmd /k "cd /d %~dp0backend && set NODE_COUNT=%NODE_COUNT%&& set NODE_START_PORT=%NODE_START_PORT%&& set NODE_BASE_URL=%NODE_BASE_URL%&& set DATA_ROOT=%DATA_ROOT%&& set DB_PATH=%DB_PATH%&& set AUTO_START_NODES=1&& set NODE_BIND_HOST=127.0.0.1&& python -m uvicorn vault.main:app --host 127.0.0.1 --port 8000"

echo Waiting for coordinator health...
set /a tries=0
:waitloop
set /a tries+=1
powershell -NoProfile -Command "try { $r=Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8000/api/health -TimeoutSec 1; if ($r.StatusCode -eq 200) { exit 0 } } catch {} ; exit 1" >nul 2>&1
if %errorlevel%==0 goto ready
if %tries% GEQ 30 goto failed
ping 127.0.0.1 -n 2 >nul
goto waitloop

:ready
start "VAULT Browser" cmd /c "start http://127.0.0.1:8000/"
echo.
echo VAULT is running.
echo Dashboard: http://127.0.0.1:8000/
echo API Docs:  http://127.0.0.1:8000/docs
echo.
echo Close the Coordinator window or press Ctrl+C there to stop everything.
pause
exit /b 0

:failed
echo.
echo ERROR: Coordinator did not start on port 8000.
echo Check the Coordinator window for the Python error.
pause
exit /b 1

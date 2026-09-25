@echo off
for /f "tokens=5" %%P in ('netstat -ano ^| findstr ":8000 :8101 :8102 :8103 :8104 :8105 :8106 :8107 :8108"') do taskkill /PID %%P /F >nul 2>&1
echo VAULT local processes stopped.
pause

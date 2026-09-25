@echo off
setlocal
cd /d %~dp0
python -m pip install -r backend\requirements-dev.txt
cd /d %~dp0backend
python -m pytest -q tests
pause

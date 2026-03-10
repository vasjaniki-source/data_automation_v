@echo off
REM Запуск анализа данных в фоновом режиме через CLI скрипт
python %~dp0run_analysis.py --mode auto --config %~dp0..\config\config.json
pause
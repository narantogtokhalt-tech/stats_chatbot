@echo off

REM venv идэвхжүүлэх
cd /d D:\DataAnalystAgent
call venv\Scripts\activate.bat

REM FastAPI / backend-ээ 8010 порт дээр асаах
uvicorn app_configured:app --host 0.0.0.0 --port 8010
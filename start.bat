@echo off
title VPI Jobs Tracker Dashboard
echo ============================================================
echo           VPI Jobs Tracker Dashboard
echo ============================================================
echo.

REM Check environment variables
if "%BIGCHANGE_USERNAME%"=="" (
    echo [WARNING] BigChange credentials not set!
    echo.
    echo Please set these environment variables:
    echo   set BIGCHANGE_USERNAME=your_username
    echo   set BIGCHANGE_PASSWORD=your_password
    echo   set BIGCHANGE_KEY=your_company_key
    echo.
    echo Starting anyway ^(sync will be disabled^)...
    echo.
)

echo Installing dependencies...
pip install flask flask-cors requests -q 2>nul

echo.
echo Starting server...
echo    Open http://localhost:5000 in your browser
echo.
echo    Press Ctrl+C to stop
echo.

python app.py

pause

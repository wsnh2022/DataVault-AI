@echo off
REM setup.bat - One-time project setup for DataVault AI V1
REM Run from the project root: AI_CHAT_BOT\
REM Requires Python 3.11 on PATH

echo [1/4] Creating virtual environment...
python -m venv .venv
if errorlevel 1 (
    echo ERROR: python not found or venv creation failed.
    exit /b 1
)

echo [2/4] Activating venv...
call .venv\Scripts\activate.bat

echo [3/4] Installing dependencies...
.venv\Scripts\python.exe -m pip install --upgrade pip --quiet
.venv\Scripts\pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo ERROR: pip install failed. Check requirements.txt and your internet connection.
    exit /b 1
)

echo [4/4] Verifying smoke tests...
python tests\smoke_test.py
if errorlevel 1 (
    echo WARNING: Some smoke tests failed. Check output above before running the app.
) else (
    echo.
    echo Setup complete. Run the app with:
    echo     start_DataVault_AI.bat
    echo     or: .venv\Scripts\activate ^&^& python app.py
)

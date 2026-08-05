@echo off
:: ============================================================================
:: run_extract.bat — Power BI data extraction launcher
::
:: Usage:
::   run_extract.bat                        rolling last 7 days (default)
::   run_extract.bat 14                     rolling last N days
::   run_extract.bat 2026-07-01 2026-07-21  specific date range
::
:: Credentials must be set as Windows environment variables (once, system-wide):
::   POWERBI_TENANT_ID, POWERBI_CLIENT_ID, POWERBI_CLIENT_SECRET
:: ============================================================================

:: --- Paths (edit these to match your machine) -------------------------------
set PYTHON=python
set SCRIPT_DIR=%~dp0
:: ---------------------------------------------------------------------------

:: Detect which mode was called based on argument count
if "%~1"=="" goto DEFAULT
if "%~2"=="" goto LOOKBACK
goto DATERANGE

:DEFAULT
echo [run_extract] Mode: rolling last 7 days (default)
%PYTHON% "%SCRIPT_DIR%extract_to_csv.py" --lookback-days 7
goto END

:LOOKBACK
echo [run_extract] Mode: rolling last %~1 days
%PYTHON% "%SCRIPT_DIR%extract_to_csv.py" --lookback-days %~1
goto END

:DATERANGE
echo [run_extract] Mode: date range %~1 to %~2
%PYTHON% "%SCRIPT_DIR%extract_to_csv.py" --start-date %~1 --end-date %~2
goto END

:END
if %ERRORLEVEL% neq 0 (
    echo [run_extract] ERROR: script exited with code %ERRORLEVEL%
    exit /b %ERRORLEVEL%
)
echo [run_extract] Done.

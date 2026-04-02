@echo off
setlocal
call "%~dp0set_runtime_env.bat" || exit /b 1

if not exist "%REPO_ROOT%\.venv\Scripts\python.exe" (
  py -3 -m venv "%REPO_ROOT%\.venv" || exit /b 1
)

"%REPO_ROOT%\.venv\Scripts\python.exe" -m pip install --upgrade pip || exit /b 1
"%REPO_ROOT%\.venv\Scripts\python.exe" -m pip install -r "%REPO_ROOT%\requirements.txt" || exit /b 1
"%REPO_ROOT%\.venv\Scripts\python.exe" "%REPO_ROOT%\setup_runtime.py" --runtime-dir "%SAGA_MEDIA_DATA_DIR%" --copy-transfers --copy-policy-sources %* || exit /b 1

echo.
echo Ready.
echo Runtime data dir: %SAGA_MEDIA_DATA_DIR%
echo Launch with windows\run_app.bat
endlocal
exit /b 0

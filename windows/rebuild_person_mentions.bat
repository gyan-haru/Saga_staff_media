@echo off
call "%~dp0set_runtime_env.bat" || exit /b 1
if not exist "%REPO_ROOT%\.venv\Scripts\python.exe" (
  echo [.venv not found] Run windows\setup_windows.bat first.
  exit /b 1
)
"%REPO_ROOT%\.venv\Scripts\python.exe" "%REPO_ROOT%\rebuild_person_mentions.py" %*

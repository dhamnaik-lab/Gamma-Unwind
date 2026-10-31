@echo off
rem GAMMA-UNWIND: Check Setup. The setup check (.env, the Telegram ids, the port, the broker token and what your
rem broker's data supports), then the setup report logs\setup-report-*.txt: what is wrong and how to fix it.
rem It never places an order. It changes files only after you answer Y (create .env, remove the download
rem mark, delete left-over stop files), and only inside this folder.
cd /d "%~dp0"
if not exist "%~dp0python\python.exe" (
  echo python\python.exe is missing, so Check Setup cannot run.
  echo  - If you opened this from inside the ZIP: close it, right-click the ZIP, Extract All to C:\GAMMA.
  echo  - Otherwise your antivirus may have removed it: Windows Security, Protection history, Restore.
  echo  - Or extract the whole ZIP again, keeping its folders.
  pause
  exit /b 2
)
if not exist "%~dp0scripts\smoke_broker.py" (
  echo scripts\smoke_broker.py is missing. Extract the whole ZIP again, keeping its folders.
  pause
  exit /b 2
)
"%~dp0python\python.exe" "%~dp0scripts\smoke_broker.py" --check-setup
set RC=%ERRORLEVEL%
echo.
echo Check Setup finished with code %RC% (0 = all good).
pause
exit /b %RC%

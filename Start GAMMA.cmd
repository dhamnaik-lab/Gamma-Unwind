@echo off
rem GAMMA-UNWIND: start the tray icon and the service (no console window stays open).
rem A second Start while the tray is running asks that tray to start the service, then exits.
rem First start without a .env: it is created from .env.example; fill it in (README.txt) or paste
rem your broker token on the dashboard Broker login page.
rem Nothing happens / no tray icon: run_gamma.pyw writes logs\launch.log (every start) and, when the start
rem fails, logs\startup-error.log plus a message box pointing to Check Setup (D104).
if not exist "%~dp0python\pythonw.exe" (
  echo python\pythonw.exe is missing. Your antivirus may have removed it, or the ZIP was not fully extracted.
  echo Double-click Check Setup: it says what to do and writes logs\setup-report-*.txt for support.
  pause
  exit /b 2
)
if not exist "%~dp0.env" copy /y "%~dp0.env.example" "%~dp0.env" >nul
start "" "%~dp0python\pythonw.exe" "%~dp0run_gamma.pyw"

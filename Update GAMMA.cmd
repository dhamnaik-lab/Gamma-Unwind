@echo off
rem GAMMA-UNWIND: update this folder in place to the newest published version.
rem It downloads the new package, checks every file against its checksum, stops GAMMA (data\STOP + data\QUIT),
rem swaps the program files, starts it again and prints the version it reports. Your .env, data\ and logs\ are
rem never touched; the previous version is kept in data\update\backup-OLDVERSION\. config\strategy.yaml is
rem replaced by the new copy and your old one is kept next to it as config\strategy.yaml.bak-OLDVERSION.
rem Codes: 0 updated / 1 nothing to do / 2 download did not verify / 3 no network / 4 GAMMA did not stop /
rem 5 download the full ZIP instead / 6 the update failed and the previous version was restored.
rem Everything after this line sits in ONE block: cmd reads the whole block before running it, so replacing this
rem file during the update cannot derail the rest of it.
setlocal EnableDelayedExpansion
cd /d "%~dp0"
(
  if not exist "%~dp0python\python.exe" (
    echo python\python.exe is missing, so the update cannot run. Double-click Check Setup: it says what to do.
    pause
    exit /b 2
  )
  if not exist "%~dp0scripts\update_gamma.py" (
    echo scripts\update_gamma.py is missing. Download the full ZIP instead.
    pause
    exit /b 2
  )
  "%~dp0python\python.exe" "%~dp0scripts\update_gamma.py" %*
  set RC=!ERRORLEVEL!
  echo.
  echo Update finished with code !RC! - 0 = updated, 1 = nothing to do; README.txt lists the others.
  pause
  exit /b !RC!
)

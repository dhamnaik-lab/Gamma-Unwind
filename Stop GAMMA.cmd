@echo off
rem GAMMA-UNWIND: full stop, service AND tray icon.
rem data\STOP: the service checks it every 2 s, deletes it and shuts down cleanly.
rem data\QUIT: the tray checks it every 1 s, stops the service (10 s, then ends it) and exits.
if not exist "%~dp0data" mkdir "%~dp0data"
type nul > "%~dp0data\STOP"
type nul > "%~dp0data\QUIT"
echo GAMMA-UNWIND stop requested. The tray icon disappears within a few seconds.

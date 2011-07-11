@echo off
rem /*##############################################################################
rem 
rem     Copyright (C) 2011 HPCC Systems.
rem 
rem     All rights reserved. This program is free software: you can redistribute it and/or modify
rem     it under the terms of the GNU Affero General Public License as
rem     published by the Free Software Foundation, either version 3 of the
rem     License, or (at your option) any later version.
rem 
rem     This program is distributed in the hope that it will be useful,
rem     but WITHOUT ANY WARRANTY; without even the implied warranty of
rem     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
rem     GNU Affero General Public License for more details.
rem 
rem     You should have received a copy of the GNU Affero General Public License
rem     along with this program.  If not, see <http://www.gnu.org/licenses/>.
rem ############################################################################## */


cd /d %1
if not exist dafscontrol.exe (
echo "USAGE check_one_dafilesrv <dafs-directory>"
goto 999
)
dafscontrol checkver . quiet
if not errorlevel 1 goto 999
for /f %%i in ('%CD%\dafscontrol.exe myver') do set DAFSDIR=c:\dafilesrv\ver_%%i
if /i "%CD%" equ "%DAFSDIR%" goto 1
mkdir "%DAFSDIR%" 2>nul 1>nul
copy *.dll "%DAFSDIR%" 2>nul 1>nul
copy *.exe "%DAFSDIR%" 2>nul 1>nul
copy *.bat "%DAFSDIR%" 2>nul 1>nul
copy start_one_dafilesrv.bat c:\dafilesrv 2>nul 1>nul
copy stop_one_dafilesrv.bat c:\dafilesrv 2>nul 1>nul
cd /d "%DAFSDIR%"
echo %DAFSDIR% > c:\dafilesrv\CURRENT_VERSION
:1
cd /d c:\dafilesrv 2>nul 1>nul
cmd /c start_one_dafilesrv 2>nul 1>nul
:999